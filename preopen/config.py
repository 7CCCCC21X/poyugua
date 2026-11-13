"""Settings, read once from the environment (Railway variables or a local .env).

Every value has a default that places nothing real: LIVE=off runs the whole pipeline against a paper exchange.
Prices are in cents (PREOPEN_PRICE_CENTS=49 = $0.49 a share), sizes in shares, times in seconds unless named minutes.
"""
from __future__ import annotations

import json
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
    max_open_usd: D = D("0")           # the bot's own resting orders in all at most this (0 = no cap; markets don't share)
    max_exposure_usd: D = D("0")       # the worst case of the unsettled positions (one-sided fills) at most this (0 = off)
    max_daily_loss_usd: D = D("0")     # today's settled loss at most this, then no new quote until Beijing midnight (0 = off)
    key_exclusive: bool = False        # on: the API key is the bot's alone (market sweeps and cancel-all may be used)
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
    telegram_admins: tuple[str, ...] = ()   # the Telegram user ids whose commands and buttons are obeyed
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


def parse_admins(raw: str | None, chat: str) -> tuple[str, ...]:
    """TELEGRAM_ADMIN_IDS: the user ids allowed to command the bot (comma or space separated). Unset, the private chat's
    id (TELEGRAM_CHAT_ID of a private chat is its user's id); a group's id (negative) names no user."""
    ids: list[str] = []
    for part in re.split(r"[,\s]+", (raw or "").strip()):
        if not part:
            continue
        if not re.fullmatch(r"[1-9]\d{0,15}", part):
            raise ConfigError(f"TELEGRAM_ADMIN_IDS 里的 {part!r} 不是 Telegram 用户 ID（正整数，可以问 @userinfobot）")
        if part not in ids:
            ids.append(part)
    if not ids and re.fullmatch(r"[1-9]\d{0,15}", chat or ""):
        ids.append(chat)
    return tuple(ids)


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
    max_exposure = parse_decimal("PREOPEN_MAX_EXPOSURE_USD", g("PREOPEN_MAX_EXPOSURE_USD"), D("0"), D("0"), D("10000000"))
    max_daily_loss = parse_decimal("PREOPEN_MAX_DAILY_LOSS_USD", g("PREOPEN_MAX_DAILY_LOSS_USD"), D("0"), D("0"), D("10000000"))
    telegram_chat = (g("TELEGRAM_CHAT_ID") or "").strip()
    telegram_admins = parse_admins(g("TELEGRAM_ADMIN_IDS"), telegram_chat)
    if (g("TELEGRAM_BOT_TOKEN") or "").strip() and telegram_chat and not telegram_admins:
        notes.append("TELEGRAM_CHAT_ID 是群组：要在 TELEGRAM_ADMIN_IDS 里写上管理员的用户 ID，Telegram 的命令、按钮和网页免密登录才会生效")
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
        max_exposure_usd=max_exposure,
        max_daily_loss_usd=max_daily_loss,
        key_exclusive=parse_bool("PREOPEN_KEY_EXCLUSIVE", g("PREOPEN_KEY_EXCLUSIVE"), False),
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
        telegram_chat=telegram_chat,
        telegram_admins=telegram_admins,
        web_key=web_key,
        web_port=web_port,
        web_url=web_url,
        log_level=(g("LOG_LEVEL") or "INFO").strip().upper(),
        notes=tuple(notes),
    )
    if settings.order_burst < 2 * len(levels):
        raise ConfigError("PREOPEN_ORDER_BURST 要至少能装下一场的全部挂单")
    return settings


# ---------------------------------------------------------------------------------------------------------------------
# The web console's settings (控制台在线改参数): each is an environment variable the page may set. A value saved on the
# page beats the environment, survives restarts and is kept in <PREOPEN_DB stem>.control.json beside the ledger. Keys,
# tokens, wallets and paths are not here: those stay in the environment.

CONTROL_KEYS: tuple[tuple[str, str, str], ...] = (
    ("LIVE", "真实交易", "on = 真实下单（需要 POLY_PRIVATE_KEY），off = 模拟盘。切换后程序自动重启；从实盘切走前先撤掉账户里本程序 API Key 的全部挂单"),
    ("PREOPEN_MARKETS", "挂哪些币", "点亮要挂的币，可多选，全选 = all；一键全关 = none（已挂的撤掉，程序照常运行，再点亮就恢复）"),
    ("PREOPEN_PRICE_CENTS", "每边价格（¢）", "0.1～50：涨跌两边都按这个价挂买单（默认 49）。超过 50 两边都成交就亏"),
    ("PREOPEN_SHARES", "每边份数", "5～100000：每场每边挂这么多份（Polymarket 最少 5 份；≥50 份才可能拿流动性奖励）"),
    ("PREOPEN_LEVELS", "更深的价格档位", "第 2 档起，价格x份数、逗号分隔、价格逐档递减、最多再 7 档，如 48x20,47x40；空 = 只挂第一档"),
    ("PREOPEN_DEEP_AHEAD_MINUTES", "第 2 档起只挂几分钟内开始的场次", "0 = 和第一档一样；更深的档位离开场这么近才挂，少占资金"),
    ("PREOPEN_AHEAD_MINUTES", "只挂几分钟内开始的场次", "10～1500：1440 = 发布即挂（约开场前 24 小时）；资金不够就调小，但那样就不是发布那一刻挂上的"),
    ("PREOPEN_CANCEL_SECONDS", "开场前几秒撤单", "至少比到期多 5 秒；撤单发出后到开场每 2 秒催一次（默认 60）"),
    ("PREOPEN_EXPIRY_SECONDS", "开场前几秒订单自动失效", "0～3600：撤单发不出去时的兜底（默认 30）"),
    ("PREOPEN_ENTRY_SECONDS", "距开场不足几秒不再新挂", "至少比撤单多 60 秒、比到期多 150 秒（GTD 订单要求到期在 3 分钟后；默认 300）"),
    ("PREOPEN_WEEKEND", "周末也挂", "off = 北京时间周六 00:00 到周一 08:00 开始的场次不挂"),
    ("PREOPEN_MAX_OPEN_USD", "挂单合计上限（$）", "0 = 不限。Polymarket 按市场分别核对余额：每一场的挂单合计不超过余额就能挂，"
     "各场不共用，所以余额够一场就能挂全部场次，挂单合计可以是余额的很多倍。设了这个数，本程序全部挂单合计最多这么多，开场最近的先挂"),
    ("PREOPEN_MAX_EXPOSURE_USD", "持仓最坏亏损上限（$）", "0 = 不限：还没结算的场次里，单边成交（没配对的份数）最坏会亏的钱到了这个数，就撤掉没成交的挂单、不再新挂，结算后自动恢复"),
    ("PREOPEN_MAX_DAILY_LOSS_USD", "日亏损上限（$）", "0 = 不限：今天（北京时间）已结算的亏损到了这个数，就撤掉没成交的挂单、今天不再新挂，零点后自动恢复"),
    ("PREOPEN_MAX_ATTEMPTS", "一档最多重报几次", "1～20：因这一场自身原因失败（单边被拒、被别人撤掉）后最多重新报价几次"),
    ("PREOPEN_AUTO_REDEEM", "自动领取", "on = 结算后自动领取赢的一边（智能钱包需要 Relayer Key，EOA 需要 POL 付 gas）；off = 到网页上手动领取"),
    ("PREOPEN_CANCEL_ON_EXIT", "退出时撤单", "on = 程序停止时撤掉全部挂单；off = 留在簿上，到期自动失效，重启后接管（保住排队位置）"),
    ("PAPER_BALANCE_USD", "模拟盘余额（$）", "只在模拟盘用：假的 pUSD 余额"),
    ("PREOPEN_POLL_SECONDS", "平时扫描间隔（秒）", "1～120：两次发布之间多久看一次下一场"),
    ("PREOPEN_FAST_POLL_SECONDS", "发布前后扫描间隔（秒）", "0.2～10：预计发布时刻前后多久看一次"),
    ("PREOPEN_SNAPSHOT_SECONDS", "读账户挂单间隔（秒）", "2～300"),
    ("PREOPEN_ORDER_RATE", "本地下单限速（单/秒）", "Standard 档位 40/秒、突发 60；升档后可调高"),
    ("PREOPEN_CANCEL_RATE", "本地撤单限速（单/秒）", "Standard 档位 80/秒、突发 120"),
    ("PREOPEN_KEY_EXCLUSIVE", "API Key 只给本程序用", "off（默认）= 只撤本程序自己签的单（按订单号/哈希），同一个 Key 下别的策略、网页上的挂单都不碰；"
     "on = 确认这个 Key 只有本程序在用：到撤单时间再按市场整场扫一遍、紧急停止和切到模拟盘时一次撤光这个 Key 的全部挂单"),
)
CONTROL_KEY_SET = frozenset(k for k, _, _ in CONTROL_KEYS)
CONTROL_FORMS: dict[str, dict] = {
    "LIVE": {"kind": "choice", "group": "运行", "options": [["off", "off 模拟盘"], ["on", "on 真实下单"]], "restart": True},
    "PREOPEN_MARKETS": {"kind": "multi", "group": "预挂", "options": [[c, c.upper()] for c in COINS], "none": "none"},
    "PREOPEN_PRICE_CENTS": {"kind": "number", "group": "预挂", "presets": ["45", "47", "48", "49", "50"], "unit": "¢"},
    "PREOPEN_SHARES": {"kind": "number", "group": "预挂", "presets": ["5", "10", "20", "50", "100"], "unit": "份"},
    "PREOPEN_LEVELS": {"kind": "text", "group": "预挂", "presets": ["", "48x20", "48x20,47x40", "48x10,47x20,46x40"], "empty": "只挂第一档"},
    "PREOPEN_DEEP_AHEAD_MINUTES": {"kind": "number", "group": "预挂", "presets": ["0", "60", "120", "240", "720"], "unit": "分钟", "zero": "和第一档一样"},
    "PREOPEN_AHEAD_MINUTES": {"kind": "number", "group": "预挂", "presets": ["60", "120", "240", "720", "1440"], "unit": "分钟"},
    "PREOPEN_CANCEL_SECONDS": {"kind": "number", "group": "预挂", "presets": ["45", "60", "90", "120", "240"], "unit": "秒"},
    "PREOPEN_EXPIRY_SECONDS": {"kind": "number", "group": "预挂", "presets": ["10", "20", "30", "45"], "unit": "秒"},
    "PREOPEN_ENTRY_SECONDS": {"kind": "number", "group": "预挂", "presets": ["180", "240", "300", "600"], "unit": "秒"},
    "PREOPEN_WEEKEND": {"kind": "choice", "group": "预挂", "options": [["on", "on 周末照挂"], ["off", "off 周末不挂"]]},
    "PREOPEN_MAX_OPEN_USD": {"kind": "number", "group": "资金", "presets": ["0", "50", "100", "500", "1000", "5000", "20000"], "unit": "$", "zero": "不限"},
    "PREOPEN_MAX_EXPOSURE_USD": {"kind": "number", "group": "资金", "presets": ["0", "20", "50", "100", "500"], "unit": "$", "zero": "不限"},
    "PREOPEN_MAX_DAILY_LOSS_USD": {"kind": "number", "group": "资金", "presets": ["0", "20", "50", "100", "500"], "unit": "$", "zero": "不限"},
    "PREOPEN_MAX_ATTEMPTS": {"kind": "number", "group": "资金", "presets": ["1", "2", "3", "5"], "unit": "次"},
    "PREOPEN_AUTO_REDEEM": {"kind": "choice", "group": "资金", "options": [["on", "on 自动领取"], ["off", "off 手动领取"]]},
    "PREOPEN_CANCEL_ON_EXIT": {"kind": "choice", "group": "资金", "options": [["off", "off 留在簿上"], ["on", "on 退出时撤单"]]},
    "PAPER_BALANCE_USD": {"kind": "number", "group": "高级", "presets": ["100", "1000", "10000", "100000"], "unit": "$"},
    "PREOPEN_POLL_SECONDS": {"kind": "number", "group": "高级", "presets": ["2", "5", "10"], "unit": "秒"},
    "PREOPEN_FAST_POLL_SECONDS": {"kind": "number", "group": "高级", "presets": ["0.3", "0.5", "1"], "unit": "秒"},
    "PREOPEN_SNAPSHOT_SECONDS": {"kind": "number", "group": "高级", "presets": ["5", "10", "20"], "unit": "秒"},
    "PREOPEN_ORDER_RATE": {"kind": "number", "group": "高级", "presets": ["20", "30", "60", "150"], "unit": "单/秒"},
    "PREOPEN_CANCEL_RATE": {"kind": "number", "group": "高级", "presets": ["40", "60", "120", "300"], "unit": "单/秒"},
    "PREOPEN_KEY_EXCLUSIVE": {"kind": "choice", "group": "高级", "options": [["off", "off 只撤本程序的单"], ["on", "on Key 只给本程序用"]]},
}


def control_path(db_path: str) -> Path:
    p = Path(db_path or "data/preopen.sqlite3")
    return p.with_name(p.stem + ".control.json")


def load_overrides(path: Path) -> dict[str, str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError):
        return {}
    return {str(k): str(v) for k, v in (data.get("env") or {}).items() if str(k) in CONTROL_KEY_SET}


def save_overrides(path: Path, overrides: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps({"env": overrides}, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def effective(env: dict[str, str], overrides: dict[str, str]) -> tuple[Settings, dict[str, str], list[str]]:
    """The settings from the environment and the page's saved values (the page wins): all of them when they hold
    together; otherwise one by one in the page's order, an override that cannot be used set aside with a note (a
    saved LIVE=on without a key cannot stop the bot from starting). The environment alone must hold."""
    try:
        return from_env({**env, **overrides}), dict(overrides), []
    except ConfigError:
        pass
    kept: dict[str, str] = {}
    notes: list[str] = []
    order = [k for k, _, _ in CONTROL_KEYS]
    for key in sorted(overrides, key=lambda k: order.index(k) if k in order else len(order)):
        trial = {**kept, key: overrides[key]}
        try:
            from_env({**env, **trial})
        except ConfigError as error:
            notes.append(f"网页保存的 {key}={overrides[key]} 现在用不了（{error}），已忽略")
            continue
        kept = trial
    return from_env({**env, **kept}), kept, notes


def current_values(s: Settings) -> dict[str, str]:
    """The settings as the variables would be written (what the page shows as the value in force)."""
    lv = s.levels
    return {
        "LIVE": "on" if s.live else "off",
        "PREOPEN_MARKETS": "all" if s.coins == COINS else ("none" if not s.coins else ",".join(s.coins)),
        "PREOPEN_PRICE_CENTS": fmt_num(lv[0].price * 100),
        "PREOPEN_SHARES": fmt_num(lv[0].shares),
        "PREOPEN_LEVELS": ",".join(f"{fmt_num(x.price * 100)}x{fmt_num(x.shares)}" for x in lv[1:]),
        "PREOPEN_DEEP_AHEAD_MINUTES": str(s.deep_ahead_s // 60),
        "PREOPEN_AHEAD_MINUTES": str(s.ahead_s // 60),
        "PREOPEN_CANCEL_SECONDS": str(s.cancel_s),
        "PREOPEN_EXPIRY_SECONDS": str(s.expiry_s),
        "PREOPEN_ENTRY_SECONDS": str(s.entry_s),
        "PREOPEN_WEEKEND": "on" if s.weekend else "off",
        "PREOPEN_MAX_OPEN_USD": fmt_num(s.max_open_usd),
        "PREOPEN_MAX_EXPOSURE_USD": fmt_num(s.max_exposure_usd),
        "PREOPEN_MAX_DAILY_LOSS_USD": fmt_num(s.max_daily_loss_usd),
        "PREOPEN_MAX_ATTEMPTS": str(s.max_attempts),
        "PREOPEN_AUTO_REDEEM": "on" if s.auto_redeem else "off",
        "PREOPEN_CANCEL_ON_EXIT": "on" if s.cancel_on_exit else "off",
        "PAPER_BALANCE_USD": fmt_num(s.paper_balance),
        "PREOPEN_POLL_SECONDS": fmt_num(D(str(s.poll_s))),
        "PREOPEN_FAST_POLL_SECONDS": fmt_num(D(str(s.fast_poll_s))),
        "PREOPEN_SNAPSHOT_SECONDS": fmt_num(D(str(s.snapshot_s))),
        "PREOPEN_ORDER_RATE": fmt_num(D(str(s.order_rate))),
        "PREOPEN_CANCEL_RATE": fmt_num(D(str(s.cancel_rate))),
        "PREOPEN_KEY_EXCLUSIVE": "on" if s.key_exclusive else "off",
    }
