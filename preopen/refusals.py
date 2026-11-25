"""Why the exchange refused orders (/rejects, the 🧾 拒单 button): the engine's refusal log grouped by cause, each with
what it means, the evidence in the log, what the bot does about it and what the operator can do; and a live check of
the refused windows against the exchange (🔬 现场检测).

A log entry (Engine.log_refusal): when, the window, the code, the exchange's own words, the HTTP status, how many orders
the request held and whether it was refused as a whole, how long after the listing and after the opening it came.
"""
from __future__ import annotations

import statistics
from collections import Counter, defaultdict
from typing import Any, Iterable

from . import markets as mk

# code -> (what it means, why it happens, what the bot does / what to do)
EXPLAIN: dict[str, tuple[str, str, str]] = {
    "token_not_ready": ("交易所撮合引擎还不认这个代币",
                        "市场刚开放时常见：市场显示能下单了，撮合引擎要再过一会儿才登记这一场的代币",
                        "只把这一场过一会儿再挂（旧市场半秒；V2 市场 2 秒，而且开放后先等 2.5 秒才发第一张单："
                        "追得太紧交易所会一直不认），其他场次照常；开放那一刻只挂第一档，被拒也不浪费多少额度；"
                        "连续 20 秒还不认就改成每 10 秒试一次，并重新读一遍这一场的市场信息"),
    "market_not_ready": ("市场还没开放下单", "新发布的场次要过一会儿才开放（V2 市场约在创建后 10 秒开放，旧市场 70～90 秒）", "每 0.2 秒问一次，开放了马上挂"),
    "not_enough_balance": ("余额或授权不够",
                           "交易所看到这一场已有的挂单加上新单超过了余额；或者账户没做交易授权；成交多了余额变少也会这样",
                           "全部新挂暂停 1 分钟，重读余额后再挂。一直出现就看余额，或到网页上手动交易一次完成授权"),
    "post_only_would_cross": ("挂单会立刻成交", "这个价位已经有人在卖，post-only 单不能吃单",
                              "这一场算失败一次，60 秒后再试，最多 3 次"),
    "invalid_tick": ("价格不在最小价位上", "这个市场的最小价位和你设的价格对不上", "这一场算失败一次；检查价格设置"),
    "invalid_size": ("份数低于市场最小值", "这个市场要求的最小份数比你设的大", "这一场算失败一次；把份数调大"),
    "invalid_expiration": ("到期时间无效", "到期时间要在下单 3 分钟以后（交易所规则）",
                           "全部新挂暂停并告警；检查撤单、到期、不再新挂这几个秒数"),
    "auth": ("签名私钥和 API Key 不是同一个账户", "Railway 里 POLY_API_KEY 三项不是 POLY_PRIVATE_KEY 生成的",
             "全部新挂暂停并告警；删掉 POLY_API_ 三项（程序会自己生成）或换成同一个账户的"),
    "banned": ("这个地址被禁止交易", "Polymarket 封了这个地址", "全部新挂暂停；联系 Polymarket"),
    "closed_only": ("这个地址只能平仓", "账户被限制为只能平仓、不能新挂单", "全部新挂暂停；联系 Polymarket"),
    "rate_limited": ("下单太快被限流（429）",
                     "交易所给每个签名地址（私钥）的下单速度有上限（Standard 档每秒 40 张、一次最多 60 张），用同一个私钥的程序共用这个额度",
                     "按交易所给的秒数暂停后继续；是本程序自己发多了，才把自己的速度减半、之后慢慢恢复；设置的速度超过档位上限时"
                     "自动压到上限以下；新场次开放那一刻只挂第一档，深档 30 秒后再挂，减少开放时的瞬间下单量。"
                     "本程序自己发得不多还被限流，看「证据」里交易所报的剩余额度：只剩很少，是同一个私钥还有别的程序在下单；"
                     "交易所没报额度，就不是按私钥限的"),
    "restarting": ("交易所撮合引擎在重启（425）", "交易所维护或重启", "几秒后自动重试"),
    "trading_disabled": ("交易所暂停了交易", "交易所维护", "等交易所恢复，自动重试"),
    "cancel_only": ("交易所只许撤单", "交易所维护", "等交易所恢复，自动重试"),
    "post_only_mode": ("交易所只许挂单", "交易所维护时只收 post-only 单", "自动重试（本程序的单本来就是 post-only）"),
    "timed_out": ("交易所处理超时", "同一账户同时发太多单时交易所会这样", "5 秒后重发，连续超时加倍等待"),
    "insert_failed": ("交易所插单失败", "交易所自己的临时错误", "5 秒后重发"),
    "duplicated": ("重复的单", "这张单其实已经在交易所了", "按订单哈希找回，接着管"),
    "transport": ("网络出错", "请求没发出去，或发出去了没收到回复", "按订单哈希核对：挂上了就接着管，没挂上就重挂"),
    "malformed": ("交易所的回复看不懂", "回复格式不对", "按订单哈希核对：挂上了就接着管，没挂上就重挂"),
    "missing": ("交易所没回这张单", "回复里少了这张单", "按订单哈希核对：挂上了就接着管，没挂上就重挂"),
    "exception": ("发送时程序出错", "本程序的问题", "按订单哈希核对；把「最近的问题」发给开发者"),
    "sign_failed": ("签名失败", "读不到这一场的市场信息（价位、代币）", "稍后重试"),
}
DEFAULT = ("交易所的原话没见过", "看下面交易所的原话",
           "先当成账户层面的问题：全部新挂暂停 30 秒再试；如果别的场次都能挂、只有这一场一直被拒，就只算这一场的问题。把原话发给开发者")
NUMBERS = "❶❷❸❹❺❻❼❽❾❿"
ACCOUNT_WIDE = {"rate_limited", "restarting", "timed_out", "insert_failed", "auth", "banned", "closed_only", "trading_disabled",
                "cancel_only", "post_only_mode", "not_enough_balance"}


def explain(code: str) -> tuple[str, str, str]:
    return EXPLAIN.get(code, DEFAULT)


STATUS = {"rejected": "被拒", "not_sent": "没发出", "unknown": "结果不明"}


def table(log: Iterable[dict], span: tuple[float, float] | None = None) -> list[list[Any]]:
    """Every refusal kept (the latest few hundred), the latest first, those that came within the span: for the review's
    拒单 sheet (the exchange's words and the circumstances, to tell a rate limit from a market just opened)."""
    lo, hi = span or (float("-inf"), float("inf"))
    rows: list[list[Any]] = [["时间（北京时间）", "市场", "档", "原因", "交易所原话", "HTTP", "结果", "整个请求被拒", "同一请求几张单",
                              "同一次拒单涉及几场", "上架后几秒", "开放下单后几秒"]]
    for e in sorted((e for e in log if lo <= float(e.get("at") or 0) < hi), key=lambda e: -float(e.get("at") or 0)):
        at = float(e["at"])
        rows.append([f"{mk.hm(at)[:5]} {mk.hms(at)}",
                     mk.window_name(e["coin"], int(e["start"])) if e.get("coin") and e.get("start") else e.get("slug", ""),
                     int(e["level"]) + 1 if e.get("level") is not None else None, explain(str(e.get("code") or ""))[0],
                     e.get("words") or "", e.get("http"), STATUS.get(str(e.get("status") or ""), e.get("status") or ""),
                     "是" if e.get("whole") else "", e.get("batch"), int(e.get("windows") or 1), e.get("listed_ago"),
                     e.get("opened_ago")])
    return rows


def rate_note(rate: dict | None) -> str:
    """Whose doing the last hour's 429s were ("" without one), from the adapter's rate_status: for each kind (see
    LiveExchange.note_limited) how many, and the latest one's numbers: the orders asked for, what the bot's own orders
    had left of the signer's bucket, what the exchange said was left."""
    if not rate or not rate.get("limited_hour"):
        return ""
    kinds, last_of = rate.get("kinds") or {}, rate.get("last_of") or {}
    parts = []

    def numbers(x: dict) -> str:
        return (f"最近一次请求 {x.get('orders')} 张单，按本程序自己发的量额度应该还有 {float(x.get('own_left') or 0):.0f} 张"
                f"（满 {x.get('burst')} 张、每秒补 {x.get('per_s')} 张，10 秒内自己共发 {x.get('own10')} 张）")
    if kinds.get("elsewhere"):
        x = last_of["elsewhere"]
        parts.append(f"{kinds['elsewhere']} 次不是按私钥的额度限的：最近一次请求 {x.get('orders')} 张单时，交易所报这个签名地址的额度还剩 "
                     f"{float(x['remaining']):.0f} 张（满 {x.get('burst')} 张），够用；是交易所另一道限流，多在新市场开放的那几秒。"
                     "按它给的秒数等一下再发，本程序不用减速；也说明没有别的程序在用这把私钥的额度")
    if kinds.get("own"):
        parts.append(f"{kinds['own']} 次是本程序自己发得太密，已自动减速、之后慢慢恢复")
    if kinds.get("signer"):
        x = last_of["signer"]
        parts.append(f"{kinds['signer']} 次是这个签名地址的额度被别的程序用掉了：{numbers(x)}，交易所却说只剩 {float(x['remaining']):.0f} 张。"
                     "有别的程序在用同一把私钥下单（另一个机器人、脚本、别处还在跑的旧版本）：停掉它，或给本程序单独配一把 Session Key，各用各的额度")
    if kinds.get("unreported"):
        parts.append(f"{kinds['unreported']} 次交易所没有报这个签名地址的额度（{numbers(last_of['unreported'])}）：不像是按私钥限的")
    return "；".join(parts)


def recent(log: Iterable[dict], now: float, hours: float) -> list[dict]:
    return [e for e in log if now - float(e.get("at") or 0) <= hours * 3600]


def summary(log: Iterable[dict], now: float, hours: float = 24, rate: dict | None = None) -> list[dict[str, Any]]:
    """The refusals of the last `hours` by cause, the most frequent first: what it means, the evidence, the coins.
    `rate` (the exchange's rate_status) adds the bot's own pace to a 429's evidence."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for e in recent(log, now, hours):
        groups[str(e.get("code") or "")].append(e)
    out = []
    for code, es in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        title, why, action = explain(code)
        opened = [float(e["opened_ago"]) for e in es if e.get("opened_ago") is not None]
        listed = [float(e["listed_ago"]) for e in es if e.get("listed_ago") is not None]
        evidence = []
        if code in ACCOUNT_WIDE:  # the window's timing tells something only when it clusters at the openings
            near = [x for x in opened if x < 5]
            if opened and 2 * len(near) >= len(es):
                evidence.append(f"{len(near)}/{len(es)} 次在新场次开放后 5 秒内（中位 {statistics.median(near):.1f} 秒）"
                                + ("" if code == "rate_limited" else "：开放那一刻同时下单太多"))
            opened = listed = []
        if opened:
            soon = sum(1 for x in opened if x < 5)
            evidence.append(f"{soon}/{len(es)} 次在市场开放后 5 秒内（中位 {statistics.median(opened):.1f} 秒）")
        elif listed:
            evidence.append(f"都在发布后中位 {mk.span(statistics.median(listed))}")
        whole = [e for e in es if e.get("whole")]
        if whole:
            many = sum(1 for e in whole if int(e.get("batch") or 0) > 2)
            evidence.append(f"{len(whole)} 次是整个请求被拒（HTTP {Counter(e.get('http') for e in whole).most_common(1)[0][0]}）"
                            + (f"，其中 {many} 次同一请求里还有别的场次" if many else ""))
        caught = sum(int(e.get("windows") or 1) for e in es)
        if caught > len(es):
            evidence.append(f"一共挡住了 {caught} 场（一次拒单挡住同一请求里的好几场）")
        if code == "rate_limited" and rate and rate.get("limited_hour"):
            evidence.append(f"最近 1 小时 {rate['limited_hour']} 次，" + rate_note(rate))
            if rate.get("configured", 0) > rate.get("cap", 0):
                evidence.append(f"「参数」里的下单速度 {rate['configured']:g} 超过 {rate.get('tier')} 档上限，已按 {rate['cap']:g} 跑")
        last = max(es, key=lambda e: float(e["at"]))
        out.append({"code": code, "title": title, "why": why, "action": action, "count": len(es),
                    "last": float(last["at"]), "last_text": mk.hms(float(last["at"])), "words": str(last.get("words") or ""),
                    "coins": Counter(mk.name_of(e["coin"]) for e in es).most_common(), "evidence": "；".join(evidence)})
    return out


def text(log: Iterable[dict], now: float, hours: float = 24, rate: dict | None = None) -> str:
    """/rejects: by cause (meaning, evidence, cause, what the bot does), then the latest ones one by one."""
    entries = recent(log, now, hours)
    span = f"最近 {hours:g} 小时"
    if not entries:
        return f"🧾 拒单 · {span}：没有被拒的单 ✅"
    groups = summary(entries, now, hours, rate)
    lines = [f"🧾 拒单 · {span} · 北京时间 {mk.hms(now)}", f"共 {len(entries)} 次，{len(groups)} 种原因；被拒的场次都没作废，会自动重挂", ""]
    for i, g in enumerate(groups):
        coins = "、".join(f"{c} {n}" for c, n in g["coins"])
        lines += [f"{NUMBERS[i] if i < len(NUMBERS) else '•'} {g['title']} × {g['count']}（最近 {g['last_text']}；{coins}）",
                  f"  交易所原话：{g['words'][:160]}"]
        if g["evidence"]:
            lines.append(f"  证据：{g['evidence']}")
        lines += [f"  为什么：{g['why']}", f"  机器人：{g['action']}"]
    lines += ["", "【最近 8 次】"]
    for e in sorted(entries, key=lambda e: -float(e["at"]))[:8]:
        when = []
        if e.get("opened_ago") is not None:
            when.append(f"开放后 {float(e['opened_ago']):.1f} 秒")
        elif e.get("listed_ago") is not None:
            when.append(f"发布后 {mk.span(float(e['listed_ago']))}")
        if e.get("batch"):
            when.append(f"同一请求 {e['batch']} 张单")
        if int(e.get("windows") or 1) > 1:
            when.append(f"一次挡住 {e['windows']} 场")
        lines.append(f"  {mk.hms(float(e['at']))} {mk.window_name(e['coin'], int(e['start']))} {explain(str(e.get('code') or ''))[0]}"
                     + (f"（{'，'.join(when)}）" if when else ""))
    lines += ["", "点「🔬 现场检测」，现在就去交易所查这些场次认不认"]
    return "\n".join(lines)


async def live_check(engine: Any, limit: int = 6) -> str:
    """🔬 现场检测: the latest refused windows still ahead, asked of the exchange now: is it on the book now, does the
    CLOB accept orders on its market, does its token have an order book, are Gamma's tokens the CLOB's."""
    now = engine.now()
    seen, picks = set(), []
    for e in sorted(engine.refusal_log, key=lambda e: -float(e["at"])):
        if e["slug"] in seen or int(e["start"]) - 30 <= now:
            continue
        seen.add(e["slug"])
        picks.append(e)
        if len(picks) >= limit:
            break
    if not picks:
        return "🔬 现场检测：没有还没开场的被拒场次可查"
    pub = engine.pub
    lines = [f"🔬 现场检测 · 北京时间 {mk.hms(now)}"]
    verdicts = Counter()
    for e in picks:
        name = mk.window_name(e["coin"], int(e["start"]))
        resting = any(q["phase"] == "active" and any(o["state"] == "open" for o in q["orders"].values())
                      for q in engine.quotes_of(e["slug"]))
        listing = engine.listings.get(e["slug"])
        up = listing.up if listing else e.get("up")
        cid = listing.condition_id if listing else e.get("cid")
        parts = []
        try:
            clob = await pub.clob_market(cid) if cid else None
            book = await pub.book(up) if up else None
            gamma = await pub.market(e["slug"])
        except Exception as error:
            lines.append(f"  {name}：查不了（{error}）")
            verdicts["error"] += 1
            continue
        ao = clob.get("ao") if isinstance(clob, dict) else None
        clob_tokens = {str(t.get("t")) for t in (clob or {}).get("t", []) if isinstance(t, dict)}
        gamma_tokens = {str(t) for t in (mk.outcome_ids(gamma) or [])} if isinstance(gamma, dict) else set()
        parts.append("现在挂着 ✅" if resting else "现在没挂着")
        parts.append("交易所接受下单 ✅" if ao else f"交易所接受下单 ❌（{'没有这个市场' if clob is None else ao}）")
        parts.append("代币有订单簿 ✅" if book is not None else "代币还没有订单簿 ❌")
        same = bool(clob_tokens) and up in clob_tokens and (not gamma_tokens or up in gamma_tokens)
        parts.append("Gamma 和交易所的代币一致 ✅" if same else "代币对不上 ❌")
        lines.append(f"  {name}：" + "，".join(parts))
        verdicts["resting" if resting else "ready" if ao and book is not None and same else
                 "mismatch" if clob_tokens and not same else "not_ready"] += 1
    if verdicts["mismatch"]:
        lines.append("结论：⚠️ 有场次的代币对不上——Gamma 给的代币不是交易所的，机器人会重新读市场信息；一直这样就把这条消息发给开发者")
    elif verdicts["not_ready"]:
        lines.append("结论：交易所还没给这些场次开好订单簿（还没开放完），开好后机器人会自己挂上")
    elif verdicts["ready"] and not verdicts["resting"]:
        lines.append("结论：交易所现在都认这些场次了，当时的拒单是暂时的；机器人马上会重挂")
    else:
        lines.append("结论：这些场次交易所现在都认了，大多已经重新挂上：当时的拒单是暂时的")
    return "\n".join(lines)
