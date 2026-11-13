"""Settings, read once from the environment (Railway variables or a local .env).

Every value has a default that places nothing real: LIVE=off runs the whole pipeline against a paper exchange.
Prices are in cents (PREOPEN_PRICE_CENTS=49 = $0.49 a share), sizes in shares, times in seconds unless named minutes.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from decimal import Decimal as D, InvalidOperation
from pathlib import Path

# The 5-minute up/down series quoted by default: the markets btc/eth/sol/xrp/doge/hype/bnb-updown-5m-<start>.
COINS = ("btc", "eth", "sol", "xrp", "doge", "hype", "bnb")
MAX_LEVELS = 8           # the first level and at most seven deeper ones (a window's batch stays within two POST /orders of 15)
MIN_SHARES = D("5")      # Polymarket's orderMinSize on these markets; a market asking more is checked again when placing
GTD_MIN_AHEAD_S = 180    # the CLOB rejects a GTD expiration less than 3 minutes away
GTD_THRESHOLD_S = 60     # ...and expires an order one minute before its stated expiration
ENTRY_MARGIN_S = 30      # extra room for latency and clock skew on the stated expiration of a late placement


class ConfigError(ValueError):
    """A setting that cannot be used; the message names the variable."""


@dataclass(frozen=True)
class Level:
    price: D   # dollars a share (0.49)
    shares: D

    @property
    def usd(self) -> D:
        return self.price * self.shares

    def label(self) -> str:
        return f"{cents(self.price)}¢×{fmt_num(self.shares)}"


@dataclass(frozen=True)
class Settings:
    live: bool = False
    coins: tuple[str, ...] = COINS
    levels: tuple[Level, ...] = (Level(D("0.49"), D("10")),)
    ahead_s: int = 1440 * 60           # quote the windows starting within this (24 h: every published window)
    deep_ahead_s: int = 0              # the deeper levels only within this of the start (0 = like the first)
    cancel_s: int = 60                 # cancel this long before the start
    expiry_s: int = 30                 # the orders expire (GTD) this long before the start: the backstop of the cancel
    entry_s: int = 300                 # no new order this close to the start
    max_open_usd: D = D("0")           # the bot's own reservation cap (0 = only the balance limits it)
    weekend: bool = True               # off = no window starting Sat 00:00 - Mon 08:00 Beijing time
    max_attempts: int = 3              # a window level quoted at most this often after failures of its own
    poll_s: float = 5.0                # discovery poll
    fast_poll_s: float = 0.5           # ...around an expected listing (each coin's next window by the direct path)
    snapshot_s: float = 10.0           # the account snapshot (open orders) at least this often
    order_rate: float = 30.0           # local order-token bucket (Standard tier: 40/s, burst 60)
    order_burst: int = 50
    cancel_rate: float = 60.0          # local cancel-token bucket (Standard tier: 80/s, burst 120)
    cancel_burst: int = 100
    workers: int = 8                   # placement workers (the seven coins list together: all placed at once)
    auto_redeem: bool = True
    cancel_on_exit: bool = False       # a stop leaves the orders resting (they expire on their own); on = withdraw them
    paper_balance: D = D("1000")
    db_path: str = "data/preopen.sqlite3"
    publish_lead_s: int = 86400 - 450  # a window is listed about this long before its start (learned from listings)
    private_key: str = ""
    wallet: str = ""
    relayer_key: str = ""
    relayer_address: str = ""
    clob_key: str = ""
    clob_secret: str = ""
    clob_passphrase: str = ""
    telegram_token: str = ""
    telegram_chat: str = ""
    web_key: str = ""                  # the web console's password (WEB_KEY); Telegram's Mini App signs in without it
    web_port: int = 0                  # the console listens here (Railway's PORT); 0 = no console
    web_url: str = ""                  # the console's public https address (WEB_URL, else Railway's RAILWAY_PUBLIC_DOMAIN)
    log_level: str = "INFO"
    notes: tuple[str, ...] = field(default=())  # warnings found while reading the settings

    @property
    def first(self) -> Level:
        return self.levels[0]

    @property
    def window_usd(self) -> D:
        """A window's full reservation: every level, both sides."""
        return 2 * sum((lv.usd for lv in self.levels), D(0))

    def mode(self) -> str:
        return "实盘" if self.live else "模拟（不下真实单）"

    @property
    def ledger_path(self) -> str:
        """The paper mode keeps its records apart (…/preopen.paper.sqlite3): a live start never takes paper orders over."""
        if self.live or self.db_path == ":memory:":
            return self.db_path
        p = Path(self.db_path)
        return str(p.with_name(p.stem + ".paper" + (p.suffix or ".sqlite3")))

    def levels_text(self) -> str:
        return "、".join(lv.label() for lv in self.levels)


def cents(price: D) -> str:
    return fmt_num(price * 100)


def fmt_num(value: D | float | int) -> str:
    d = D(str(value)).normalize()
    text = format(d, "f")
    return text


def parse_bool(name: str, raw: str | None, default: bool) -> bool:
    if raw is None or raw.strip() == "":
        return default
    value = raw.strip().lower()
    if value in {"1", "on", "true", "yes", "y"}:
        return True
    if value in {"0", "off", "false", "no", "n"}:
        return False
    raise ConfigError(f"{name}={raw!r}：只能是 on / off")


def parse_decimal(name: str, raw: str | None, default: D, lo: D | None = None, hi: D | None = None) -> D:
    if raw is None or raw.strip() == "":
        return default
    try:
        value = D(raw.strip())
    except InvalidOperation as error:
        raise ConfigError(f"{name}={raw!r}：不是数字") from error
    if not value.is_finite() or (lo is not None and value < lo) or (hi is not None and value > hi):
        raise ConfigError(f"{name}={raw!r}：范围 {lo}～{hi}")
    return value


def parse_int(name: str, raw: str | None, default: int, lo: int, hi: int) -> int:
    value = parse_decimal(name, raw, D(default), D(lo), D(hi))
    if value != value.to_integral_value():
        raise ConfigError(f"{name}={raw!r}：要整数")
    return int(value)


def parse_coins(raw: str | None) -> tuple[str, ...]:
    """PREOPEN_MARKETS: all (the seven), none, or a comma list such as btc,eth,sol. A coin outside the seven is allowed
    (its <coin>-updown-5m-<start> markets are looked for the same way) but noted."""
    if raw is None or raw.strip() == "" or raw.strip().lower() == "all":
        return COINS
    if raw.strip().lower() == "none":
        return ()
    coins: list[str] = []
    for part in re.split(r"[,\s]+", raw.strip().lower()):
        if not part:
            continue
        part = part.removesuffix("-5m").removesuffix("5m").removesuffix("-updown")
        if not re.fullmatch(r"[a-z0-9]{2,12}", part):
            raise ConfigError(f"PREOPEN_MARKETS 里的 {part!r} 不是币种代号（如 btc、eth）")
        if part not in coins:
            coins.append(part)
    return tuple(coins)


def parse_levels(price_raw: str | None, shares_raw: str | None, levels_raw: str | None) -> tuple[Level, ...]:
    """The first level (PREOPEN_PRICE_CENTS × PREOPEN_SHARES) and the deeper ones (PREOPEN_LEVELS=48x20,47x40), the
    prices strictly falling. A price above 50¢ is refused: both sides filled would cost more than the $1 they pay."""
    price = parse_decimal("PREOPEN_PRICE_CENTS", price_raw, D("49"), D("0.1"), D("50")) / 100
    shares = parse_decimal("PREOPEN_SHARES", shares_raw, D("10"), MIN_SHARES, D("100000"))
    levels = [Level(price, shares)]
    if levels_raw and levels_raw.strip():
        for part in re.split(r"[,\s]+", levels_raw.strip()):
            if not part:
                continue
            m = re.fullmatch(r"(\d+(?:\.\d+)?)[x×*](\d+(?:\.\d+)?)", part.lower())
            if not m:
                raise ConfigError(f"PREOPEN_LEVELS 里的 {part!r}：格式是 价格x份数，如 48x20")
            lv = Level(parse_decimal("PREOPEN_LEVELS", m.group(1), D(0), D("0.1"), D("50")) / 100,
                       parse_decimal("PREOPEN_LEVELS", m.group(2), D(0), MIN_SHARES, D("100000")))
            if lv.price >= levels[-1].price:
                raise ConfigError(f"PREOPEN_LEVELS：{part} 的价格要低于上一档（{cents(levels[-1].price)}¢）")
            levels.append(lv)
    if len(levels) > MAX_LEVELS:
        raise ConfigError(f"PREOPEN_LEVELS：最多再 {MAX_LEVELS - 1} 档")
    for lv in levels:
        if lv.shares != lv.shares.quantize(D("0.01")):
            raise ConfigError(f"份数 {lv.shares} 最多两位小数")
        if lv.price != lv.price.quantize(D("0.0001")):
            raise ConfigError(f"价格 {cents(lv.price)}¢ 太细")
    return tuple(levels)


def load_dotenv(path: str = ".env") -> None:
    """A local .env (KEY=VALUE lines) for running outside Railway; real environment variables win."""
    p = Path(path)
    if not p.is_file():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        os.environ.setdefault(key, value)


def from_env(env: dict[str, str] | None = None) -> Settings:
    e = dict(os.environ if env is None else env)
    g = e.get
    notes: list[str] = []
    live = parse_bool("LIVE", g("LIVE"), False)
    coins = parse_coins(g("PREOPEN_MARKETS"))
    unknown = [c for c in coins if c not in COINS]
    if unknown:
        notes.append(f"PREOPEN_MARKETS 里的 {','.join(unknown)} 不在默认七个币种里：照样按 <币>-updown-5m-<开始秒> 去找")
    levels = parse_levels(g("PREOPEN_PRICE_CENTS"), g("PREOPEN_SHARES"), g("PREOPEN_LEVELS"))
    ahead_min = parse_int("PREOPEN_AHEAD_MINUTES", g("PREOPEN_AHEAD_MINUTES"), 1440, 10, 1500)
    deep_min = parse_int("PREOPEN_DEEP_AHEAD_MINUTES", g("PREOPEN_DEEP_AHEAD_MINUTES"), 0, 0, 1500)
    expiry_s = parse_int("PREOPEN_EXPIRY_SECONDS", g("PREOPEN_EXPIRY_SECONDS"), 30, 0, 3600)
    cancel_s = parse_int("PREOPEN_CANCEL_SECONDS", g("PREOPEN_CANCEL_SECONDS"), 60, 1, 3600)
    entry_s = parse_int("PREOPEN_ENTRY_SECONDS", g("PREOPEN_ENTRY_SECONDS"), 300, 60, 6 * 3600)
    if cancel_s < expiry_s + 5:
        raise ConfigError(f"PREOPEN_CANCEL_SECONDS={cancel_s} 要比 PREOPEN_EXPIRY_SECONDS={expiry_s} 至少多 5 秒（先撤单，到期兜底）")
    need = expiry_s + GTD_MIN_AHEAD_S - GTD_THRESHOLD_S + ENTRY_MARGIN_S
    if entry_s < need:
        raise ConfigError(f"PREOPEN_ENTRY_SECONDS={entry_s} 至少要 {need}：GTD 到期时间必须在下单 3 分钟以后（到期 {expiry_s} 秒 + 120 + 余量 30）")
    if entry_s < cancel_s + 60:
        raise ConfigError(f"PREOPEN_ENTRY_SECONDS={entry_s} 要比 PREOPEN_CANCEL_SECONDS={cancel_s} 至少多 60 秒")
    if ahead_min * 60 <= entry_s + 300:
        raise ConfigError(f"PREOPEN_AHEAD_MINUTES={ahead_min} 太小：要大于新单截止（{entry_s} 秒）再加 5 分钟")
    private_key = (g("POLY_PRIVATE_KEY") or "").strip()
    wallet = (g("POLY_WALLET") or "").strip()
    if live and not private_key:
        raise ConfigError("LIVE=on 需要 POLY_PRIVATE_KEY（签名私钥）")
    if wallet and not re.fullmatch(r"0x[0-9a-fA-F]{40}", wallet):
        raise ConfigError("POLY_WALLET 要是 0x 开头的 40 位地址（Polymarket 个人资料里的钱包地址）")
    if live and not wallet:
        notes.append("没设 POLY_WALLET：用这把私钥的默认 Deposit Wallet。网页账户的钱包地址请填进 POLY_WALLET")
    clob = [(g(k) or "").strip() for k in ("POLY_API_KEY", "POLY_API_SECRET", "POLY_API_PASSPHRASE")]
    if any(clob) and not all(clob):
        raise ConfigError("POLY_API_KEY / POLY_API_SECRET / POLY_API_PASSPHRASE 要么都填、要么都不填（不填就用私钥派生）")
    relayer = [(g(k) or "").strip() for k in ("POLY_RELAYER_API_KEY", "POLY_RELAYER_API_KEY_ADDRESS")]
    if any(relayer) and not all(relayer):
        raise ConfigError("POLY_RELAYER_API_KEY 和 POLY_RELAYER_API_KEY_ADDRESS 要一起填")
    max_open = parse_decimal("PREOPEN_MAX_OPEN_USD", g("PREOPEN_MAX_OPEN_USD"), D("0"), D("0"), D("10000000"))
    web_key = (g("WEB_KEY") or g("WEB_CONTROL_KEY") or "").strip()
    if web_key and len(web_key) < 12:
        raise ConfigError("WEB_KEY 太短：至少 12 个字符（网页能暂停机器人、能看到余额）")
    web_port = parse_int("PORT", g("PORT"), 8080 if web_key else 0, 0, 65535)
    domain = (g("RAILWAY_PUBLIC_DOMAIN") or "").strip().strip("/")
    web_url = (g("WEB_URL") or "").strip().rstrip("/") or (f"https://{domain}" if domain else "")
    if web_url and not web_url.startswith("https://"):
        notes.append(f"WEB_URL={web_url} 不是 https：Telegram 的菜单按钮只能打开 https 网页")
    settings = Settings(
        live=live,
        coins=coins,
        levels=levels,
        ahead_s=ahead_min * 60,
        deep_ahead_s=deep_min * 60,
        cancel_s=cancel_s,
        expiry_s=expiry_s,
        entry_s=entry_s,
        max_open_usd=max_open,
        weekend=parse_bool("PREOPEN_WEEKEND", g("PREOPEN_WEEKEND"), True),
        max_attempts=parse_int("PREOPEN_MAX_ATTEMPTS", g("PREOPEN_MAX_ATTEMPTS"), 3, 1, 20),
        poll_s=float(parse_decimal("PREOPEN_POLL_SECONDS", g("PREOPEN_POLL_SECONDS"), D("5"), D("1"), D("120"))),
        fast_poll_s=float(parse_decimal("PREOPEN_FAST_POLL_SECONDS", g("PREOPEN_FAST_POLL_SECONDS"), D("0.5"), D("0.2"), D("10"))),
        snapshot_s=float(parse_decimal("PREOPEN_SNAPSHOT_SECONDS", g("PREOPEN_SNAPSHOT_SECONDS"), D("10"), D("2"), D("300"))),
        order_rate=float(parse_decimal("PREOPEN_ORDER_RATE", g("PREOPEN_ORDER_RATE"), D("30"), D("1"), D("1000"))),
        order_burst=parse_int("PREOPEN_ORDER_BURST", g("PREOPEN_ORDER_BURST"), 50, 15, 2000),
        cancel_rate=float(parse_decimal("PREOPEN_CANCEL_RATE", g("PREOPEN_CANCEL_RATE"), D("60"), D("1"), D("2000"))),
        cancel_burst=parse_int("PREOPEN_CANCEL_BURST", g("PREOPEN_CANCEL_BURST"), 100, 10, 4000),
        workers=parse_int("PREOPEN_WORKERS", g("PREOPEN_WORKERS"), 8, 1, 16),
        auto_redeem=parse_bool("PREOPEN_AUTO_REDEEM", g("PREOPEN_AUTO_REDEEM"), True),
        cancel_on_exit=parse_bool("PREOPEN_CANCEL_ON_EXIT", g("PREOPEN_CANCEL_ON_EXIT"), False),
        paper_balance=parse_decimal("PAPER_BALANCE_USD", g("PAPER_BALANCE_USD"), D("1000"), D("0"), D("100000000")),
        db_path=(g("PREOPEN_DB") or "data/preopen.sqlite3").strip(),
        private_key=private_key,
        wallet=wallet,
        relayer_key=relayer[0],
        relayer_address=relayer[1],
        clob_key=clob[0],
        clob_secret=clob[1],
        clob_passphrase=clob[2],
        telegram_token=(g("TELEGRAM_BOT_TOKEN") or "").strip(),
        telegram_chat=(g("TELEGRAM_CHAT_ID") or "").strip(),
        web_key=web_key,
        web_port=web_port,
        web_url=web_url,
        log_level=(g("LOG_LEVEL") or "INFO").strip().upper(),
        notes=tuple(notes),
    )
    if settings.order_burst < 2 * len(levels):
        raise ConfigError("PREOPEN_ORDER_BURST 要至少能装下一场的全部挂单")
    return settings
