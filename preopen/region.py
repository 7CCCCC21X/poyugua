"""Where the bot runs, as Polymarket sees it: the geoblock check before live trading.

Polymarket refuses new orders through the API from the OFAC-sanctioned jurisdictions and from a list of
"close-only" ones (the US, Singapore, the UK, France, Germany, Australia, ...); a few more (the Netherlands, Ireland,
Japan, South Korea) are close-only on the website only, the API is open there. Its servers are in AWS eu-west-2
(London). On Railway that leaves EU West (Amsterdam). The endpoint answers for the website, so its `blocked` flag is
reported, not obeyed: what decides is whether the country is closed to the API.
"""
from __future__ import annotations

import logging

import httpx

LOG = logging.getLogger(__name__)
GEOBLOCK = "https://polymarket.com/api/geoblock"

API_CLOSED = {  # no new orders through the API (OFAC + close-only on the frontend and the API), 2026-11
    "IR", "SY", "CU", "KP", "AR", "AU", "BY", "BE", "BI", "BR", "CF", "CD", "ET", "FR", "DE", "IQ", "IT", "LB", "LY",
    "MM", "NZ", "NI", "PL", "RU", "SG", "SO", "SK", "SS", "SD", "TW", "TH", "GB", "US", "UM", "VE", "YE", "ZW",
}
API_CLOSED_REGIONS = {"CA-BC", "CA-ON", "CA-AB", "CA-QC", "UA-43", "UA-14", "UA-09"}


def verdict(answer: dict) -> tuple[bool, str]:
    """(may place orders through the API, a line for the operator) from the geoblock answer."""
    country = str(answer.get("country") or "").upper()
    region = str(answer.get("region") or "").upper()
    where = f"{country}{'-' + region if region else ''}（IP {answer.get('ip', '?')}）"
    sub = f"{country}-{region}" if region and "-" not in region else region
    if country in API_CLOSED or sub in API_CLOSED_REGIONS:
        return False, f"⚠️ 本机所在地区 {where} 在 Polymarket 的 API 上不能开新单（只许平仓）：请把部署地区换到 EU West（Amsterdam）"
    note = "；网页端限制，API 不受限" if answer.get("blocked") else ""
    return True, f"本机所在地区 {where} 可以通过 API 下单{note}"


async def check(timeout: float = 10.0) -> tuple[bool | None, str]:
    """Ask the geoblock endpoint; (None, why) when it cannot be read (the CLOB itself refuses orders from a closed
    region, so a failed check does not stop the bot)."""
    try:
        async with httpx.AsyncClient(timeout=timeout, headers={"User-Agent": "poyugua-preopen/1.0"}) as http:
            r = await http.get(GEOBLOCK)
            r.raise_for_status()
            answer = r.json()
    except Exception as error:
        return None, f"查不到所在地区（{type(error).__name__}）：若下单一直被拒（403），请确认部署在 EU West（Amsterdam）"
    if not isinstance(answer, dict):
        return None, "所在地区的应答格式不对"
    return verdict(answer)
