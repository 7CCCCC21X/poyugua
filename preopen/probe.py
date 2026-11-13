"""Read-only probe (no key, no order): what the bot would find and do right now.

    python main.py probe [hours]

For every coin: whether the windows of the next hours are listed and pass the checks, the newest listing (about a day
ahead) and when the next one is expected, and the nearest window's book at the quoted price on both sides.
"""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal as D

from . import markets as mk
from .config import Settings
from .gamma import PublicApi, best, depth_at


async def probe(settings: Settings, pub: PublicApi, hours: float = 1.0, now: float | None = None) -> str:
    now = time.time() if now is None else now
    starts = mk.window_starts(now, settings)
    near = [t for t in starts if t <= now + hours * 3600][:24]
    base = int(now) - int(now) % mk.PERIOD_S
    far = [base + 86400 + mk.PERIOD_S * k for k in range(-8, 3)]
    slugs = [mk.slug_of(c, t) for c in settings.coins for t in sorted(set(near + far))]
    found = await pub.listings(slugs)
    lines = [f"🔎 只读探测 {mk.hms(now)}（北京时间）· {settings.mode()} · 每边 {settings.levels_text()}",
             f"范围：开场前 {settings.ahead_s // 60} 分钟内；开场前 {settings.cancel_s} 秒撤单，{settings.expiry_s} 秒到期，{settings.entry_s} 秒内不新挂"]
    leads = []
    for coin in settings.coins:
        ok = problems = 0
        notes: list[str] = []
        latest = 0
        for t in sorted(set(near + far)):
            m = found.get(mk.slug_of(coin, t))
            if m is None:
                continue
            listing, why = mk.parse_listing(m, coin, t, now)
            if listing is None:
                if t in near:
                    problems += 1
                    notes.append(f"{mk.hm(t)} 不挂：{why}")
                continue
            latest = max(latest, t)
            if listing.created:
                leads.append(t - listing.created)
            if t in near:
                level_issues = [mk.level_problem(listing, lv.price, lv.shares) for lv in settings.levels]
                level_issues = [x for x in level_issues if x]
                if level_issues or listing.maker_fee:
                    problems += 1
                    notes.append(f"{mk.hm(t)} 不挂：{'；'.join(level_issues) or '挂单方也收手续费'}")
                else:
                    ok += 1
        missing = [t for t in near if mk.slug_of(coin, t) not in found]
        head = f"{mk.name_of(coin)}：接下来 {len(near)} 场里 {ok} 场可挂" + (f"，{problems} 场不挂" if problems else "") \
            + (f"，{len(missing)} 场没发布" if missing else "")
        head += f"；已发布到 {mk.hm(latest)}" if latest else "；一场都没找到（链接对不上？）"
        lines.append(head)
        lines += [f"    {n}" for n in notes[:3]]
    if leads:
        lead = sorted(leads)[len(leads) // 2]
        nxt = max((t for t in far if any(mk.slug_of(c, t) in found for c in settings.coins)), default=0) + mk.PERIOD_S
        lines.append(f"发布规律：开场前 {lead / 3600:.2f} 小时发布（中位）；下一场 {mk.hm(nxt)} 预计 {mk.hms(nxt - lead)} 发布")
    if near:
        t = near[0]
        lines.append(f"最近一场（{mk.hm(t)}）的盘口：")
        for coin in settings.coins:
            m = found.get(mk.slug_of(coin, t))
            listing = mk.parse_listing(m, coin, t, now)[0] if m else None
            if listing is None:
                continue
            books = await asyncio.gather(pub.book(listing.up), pub.book(listing.down), return_exceptions=True)
            parts = []
            for side, book in zip(mk.SIDES, books):
                if isinstance(book, BaseException) or book is None:
                    parts.append(f"{mk.SIDES_CN[side]} 读不到")
                    continue
                bid, ask = best(book, "bids"), best(book, "asks")
                ahead = depth_at(book, "bids", float(settings.first.price))
                parts.append(f"{mk.SIDES_CN[side]} 买{bid[0] if bid else '-'}/卖{ask[0] if ask else '-'}，"
                             f"{mk.fmt(settings.first.price * 100)}¢ 已有 {ahead:.0f} 份排队")
            lines.append(f"    {mk.name_of(coin)}：" + "；".join(parts))
    windows = len(starts) * len(settings.coins)
    full = settings.window_usd * windows
    lines.append(f"资金：范围内 {windows} 场全挂上要预留 ${full:,.2f}（每场 ${settings.window_usd:.2f}；"
                 f"Polymarket 的挂买单占用 pUSD 余额）")
    if settings.max_open_usd:
        lines.append(f"PREOPEN_MAX_OPEN_USD=${settings.max_open_usd}：约能挂 {int(settings.max_open_usd / settings.window_usd)} 场")
    return "\n".join(lines)


def capital_note(settings: Settings, windows: int) -> D:
    return settings.window_usd * windows
