#!/usr/bin/env python3
"""Predict.fun 天气市场监控：NOAA 观测 × 订单簿。Python 3.12+，仅标准库。

监控「<城市> <日期> 最高气温」市场（例如 highest-temperature-in-shanghai-on-october-13-2026）：
- 观测：NOAA/NWS 时间序列页（weather.gov/wrh/timeseries?site=zspd）同源的 Synoptic 接口；aviationweather.gov 的
  METAR 作为同一份数据的备用通道；Weather Underground 作为市场规则指定的备用结算源。
- 盘口：Predict.fun 该市场各档位（22°C 或以下 … 28°C 或以上）的订单簿。
- 推送：当日最高气温刷新、档位变化、次日首条观测出现（市场锁定）、观测被修正、数据异常、模型优势、市场结算。
只读监控，不下单，不需要钱包或交易所 API Key。
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import contextvars
import copy
import datetime as dt
import decimal
import functools
import html
import http.client
import inspect
import json
import logging
import math
import os
import re
import signal
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

D = decimal.Decimal
UTC = dt.timezone.utc
BEIJING = dt.timezone(dt.timedelta(hours=8), "Asia/Shanghai")
DAY_MS = 86_400_000
VERSION = "1.0.0"
LOG = logging.getLogger("tianqi")

# --- data sources -----------------------------------------------------------------------------------------------------
# The NWS "timeseries" page (the market's resolution source) draws its table from Synoptic Data's station API; the page
# itself is a browser application and cannot be read as data. ZSPD's rows are the airport's METAR / SPECI reports.
SYNOPTIC_URL = "https://api.synopticdata.com/v2/stations/timeseries"
SYNOPTIC_DEMO_TOKEN = "d8c6aee36a994f90857925cea26934be"  # Synoptic 文档公开的演示 token；请到 synopticdata.com 申请自己的免费 token
METAR_URL = "https://aviationweather.gov/api/data/metar"       # NOAA 航空气象中心：同一批 METAR，不需要 token
WU_URL = "https://api.weather.com/v1/location/{stid}:9:{country}/observations/historical.json"  # Weather Underground 历史页的数据接口
WU_DEFAULT_KEY = "e1f10a1e78da46f5b10a1e78da96f525"            # wunderground.com 历史页内嵌的公开 key（可用 WU_API_KEY 覆盖）
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"      # 逐小时气温预报（免费，无 key）：只用于模型概率
NOAA_PAGE = "https://www.weather.gov/wrh/timeseries?site={site}"
WU_PAGE = "https://www.wunderground.com/history/daily/{cc}/{city}/{stid}/date/{date}"
PREDICT_GRAPHQL = "https://graphql.predict.fun/graphql"
PREDICT_REST = "https://api.predict.fun/v1"
PREDICT_SITE = "https://predict.fun/zh-cn/market/"
MONTHS = ("january", "february", "march", "april", "may", "june", "july", "august", "september", "october",
          "november", "december")
SOURCE_NAMES = {"noaa": "NOAA/Synoptic", "metar": "METAR(AWC)", "wu": "Weather Underground"}
SOURCE_TIMEOUT = 12         # seconds per observation / forecast request
PREDICT_TIMEOUT = 10
PREDICT_STALE_MS = 120_000  # a book older than this is shown as stale and never recommended
PREDICT_PARALLEL = 6
PREDICT_BACKOFF_SECONDS = 20
PREDICT_MISS_SECONDS = 120  # an unlisted slug is looked up again after this long
PREDICT_META_SECONDS = 300  # outcome names / status / rules of a market are re-read this often
PREDICT_DEPTH = 10
NOTICE_GRACE_SECONDS = 180  # a data fault must last this long before subscribers hear of it (one failed request is not an outage)
NOTICE_REPEAT_SECONDS = 1800
WATCHDOG_SECONDS = 900      # the sampling loop not starting a cycle for this long means it is stuck: exit, Railway restarts
SHUTDOWN_GRACE_SECONDS = 8
OBS_KEEP_DAYS = 14          # observation records older than this are pruned from the store


# --- small helpers ------------------------------------------------------------------------------------------------------
def clean_error(error: BaseException | str) -> str:
    """Tokens and keys must never appear in logs, chat messages or diagnostics."""
    text = str(error)
    text = re.sub(r"\b\d{5,}:[A-Za-z0-9_-]{15,}\b", "[TOKEN REDACTED]", text)
    text = re.sub(r"(?i)(token|apikey|api_key|x-api-key)=[^&\s]+", r"\1=[REDACTED]", text)
    text = re.sub(r"https?://\S+", "[URL]", text)
    return text[:280]


def brief_error(text: str, limit: int = 60) -> str:
    return text if len(text) <= limit else text[:limit - 1] + "…"


def bounded_int(env: dict[str, str], key: str, default: int, low: int, high: int) -> int:
    try:
        value = int(str(env.get(key, str(default))).strip() or default)
    except ValueError:
        raise ValueError(f"{key} 必须是整数") from None
    if not low <= value <= high:
        raise ValueError(f"{key} 必须在 {low}～{high} 之间")
    return value


def parse_bounded(env: dict[str, str], key: str, default: str, low: float, high: float) -> float:
    try:
        value = float(str(env.get(key, default)).strip() or default)
    except ValueError:
        raise ValueError(f"{key} 必须是数字") from None
    if not (low <= value <= high and math.isfinite(value)):
        raise ValueError(f"{key} 必须在 {low:g}～{high:g} 之间")
    return value


def flag(env: dict[str, str], key: str, default: str = "on") -> bool:
    return str(env.get(key, default)).strip().lower() not in {"off", "0", "false", "no"}


FIXED_ZONES = {  # used only when the system has no tzdata (python:3.12-slim without the tzdata package)
    "Asia/Shanghai": 8, "Asia/Hong_Kong": 8, "Asia/Macau": 8, "Asia/Taipei": 8, "Asia/Singapore": 8,
    "Asia/Kuala_Lumpur": 8, "Asia/Manila": 8, "Asia/Tokyo": 9, "Asia/Seoul": 9, "Asia/Bangkok": 7,
    "Asia/Jakarta": 7, "Asia/Ho_Chi_Minh": 7, "Asia/Kolkata": 5.5, "Asia/Dubai": 4, "UTC": 0, "Etc/UTC": 0,
}


@functools.lru_cache(maxsize=64)
def zone(name: str) -> dt.tzinfo:
    """'Asia/Shanghai' (tzdata, else a built-in fixed offset for zones without daylight saving) or '+08:00'."""
    text = (name or "").strip()
    match = re.fullmatch(r"(?:UTC|GMT)?([+-])(\d{1,2})(?::?(\d{2}))?", text)
    if match:
        sign = 1 if match.group(1) == "+" else -1
        hours, minutes = int(match.group(2)), int(match.group(3) or 0)
        if hours > 14 or minutes > 59:
            raise ValueError(f"时区偏移无效：{text}")
        return dt.timezone(sign * dt.timedelta(hours=hours, minutes=minutes), text)
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_+-]*(?:/[A-Za-z0-9_+-]+){0,2}", text):
        raise ValueError(f"时区无效：{text or '(空)'}")
    try:
        import zoneinfo
        return zoneinfo.ZoneInfo(text)
    except Exception:  # no tzdata on this system, or an unknown name
        if text in FIXED_ZONES:
            return dt.timezone(dt.timedelta(hours=FIXED_ZONES[text]), text)
        raise ValueError(f"无法识别时区 {text}：系统缺少 tzdata，请改用 +08:00 这类固定偏移") from None


def zone_name(tz: dt.tzinfo) -> str:
    return getattr(tz, "key", None) or str(tz)


def local_date(ts_ms: int | float, tz: dt.tzinfo) -> dt.date:
    return dt.datetime.fromtimestamp(ts_ms / 1000, tz).date()


def day_bounds(day: dt.date, tz: dt.tzinfo) -> tuple[int, int]:
    """[start, end) of the local calendar day in epoch ms."""
    start = dt.datetime.combine(day, dt.time(), tz)
    end = dt.datetime.combine(day + dt.timedelta(days=1), dt.time(), tz)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def stamp(ms: int | float, tz: dt.tzinfo = BEIJING, seconds: bool = False, date: bool = True) -> str:
    text = dt.datetime.fromtimestamp(ms / 1000, tz).strftime(("%m-%d " if date else "") + ("%H:%M:%S" if seconds else "%H:%M"))
    return text


def whole_degrees(value: float) -> int:
    """The page shows whole degrees: round half away from zero (24.5 -> 25, -0.5 -> -1), the way a reading is printed."""
    return int(D(repr(float(value))).quantize(D(1), rounding=decimal.ROUND_HALF_UP))


def c_to_f(value: float) -> float:
    return value * 9 / 5 + 32


def deg(value: int | None, unit: str = "") -> str:
    return "—" if value is None else f"{value}°{unit}"


def norm_cdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2))


def cents(value: float, sign: bool = False) -> str:
    text = f"{value * 100:+.0f}¢" if sign else f"{value * 100:.0f}¢"
    return text


def pct(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:.0f}%"


def parse_day(text: str, now_day: dt.date | None = None) -> dt.date:
    """'2026-10-13' / '10-13' / '1013' / 'today' / 'tomorrow' -> date."""
    value = text.strip().lower()
    today = now_day or dt.datetime.now(BEIJING).date()
    if value in {"today", "今天", "今日"}:
        return today
    if value in {"tomorrow", "明天", "明日"}:
        return today + dt.timedelta(days=1)
    if value in {"yesterday", "昨天", "昨日"}:
        return today - dt.timedelta(days=1)
    match = re.fullmatch(r"(?:(\d{4})[-/.])?(\d{1,2})[-/.](\d{1,2})", value) or re.fullmatch(r"(\d{4})?(\d{2})(\d{2})", value)
    if not match:
        raise ValueError("日期格式应为 YYYY-MM-DD 或 MM-DD")
    year = int(match.group(1) or today.year)
    try:
        return dt.date(year, int(match.group(2)), int(match.group(3)))
    except ValueError:
        raise ValueError("日期无效") from None


# --- stations and markets -----------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class StationSpec:
    city: str            # the city part of the market slug, e.g. shanghai / hong-kong
    stid: str            # ICAO / Synoptic station id named by the market rules (site=zspd)
    tz: str              # the station's local time zone: the market counts "this day" in it
    country: str = ""    # country code of the Weather Underground location id (ZSPD:9:CN)
    lat: float | None = None
    lon: float | None = None
    name: str = ""       # how the station is called in messages

    @property
    def label(self) -> str:
        return self.name or self.city.replace("-", " ").title()


# Stations the known markets name in their rules; the rules of each market are read and a different site= there is
# flagged (and used when the city is not configured at all).
KNOWN_STATIONS = {
    "shanghai": StationSpec("shanghai", "ZSPD", "Asia/Shanghai", "CN", 31.1434, 121.8052, "上海浦东机场"),
    "hong-kong": StationSpec("hong-kong", "VHHH", "Asia/Hong_Kong", "HK", 22.3080, 113.9185, "香港机场"),
    "beijing": StationSpec("beijing", "ZBAA", "Asia/Shanghai", "CN", 40.0801, 116.5846, "北京首都机场"),
    "tokyo": StationSpec("tokyo", "RJTT", "Asia/Tokyo", "JP", 35.5494, 139.7798, "东京羽田机场"),
    "singapore": StationSpec("singapore", "WSSS", "Asia/Singapore", "SG", 1.3644, 103.9915, "新加坡樟宜机场"),
    "taipei": StationSpec("taipei", "RCTP", "Asia/Taipei", "TW", 25.0797, 121.2342, "台北桃园机场"),
}
CITY_NAMES = {"shanghai": "上海", "hong-kong": "香港", "beijing": "北京", "tokyo": "东京", "singapore": "新加坡", "taipei": "台北"}
DEFAULT_SLUG_PATTERN = "highest-temperature-in-{city}-on-{month}-{day}-{year}"


def parse_stations(spec: str) -> dict[str, StationSpec]:
    """STATIONS=shanghai=ZSPD:Asia/Shanghai:CN:31.1434,121.8052;hong-kong=VHHH:Asia/Hong_Kong:HK -> city -> station.
    Fields after the station id are optional and taken from KNOWN_STATIONS when the city is known."""
    out: dict[str, StationSpec] = {}
    for item in re.split(r"[;,]\s*(?=[a-z0-9-]+=)", spec.strip()):
        item = item.strip()
        if not item:
            continue
        city, sep, rest = item.partition("=")
        city = city.strip().lower()
        if not sep or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", city):
            raise ValueError(f"STATIONS 格式应为 城市=站号:时区:国家码:纬度,经度，无法解析：{item}")
        parts = [p.strip() for p in rest.split(":")]
        if len(parts) > 2 and re.fullmatch(r"(?:UTC|GMT)?[+-]\d{1,2}", parts[1]) and re.fullmatch(r"\d{2}", parts[2]):
            parts[1:3] = [parts[1] + ":" + parts[2]]  # a +08:00 zone was split on its own colon
        known = KNOWN_STATIONS.get(city)
        stid = parts[0].upper()
        if not re.fullmatch(r"[A-Z0-9]{3,8}", stid):
            raise ValueError(f"STATIONS 里 {city} 的站号无效：{parts[0] or '(空)'}")
        tz = parts[1] if len(parts) > 1 and parts[1] else (known.tz if known and known.stid == stid else "")
        if not tz:
            raise ValueError(f"STATIONS 里 {city} 缺少时区（如 Asia/Shanghai 或 +08:00）")
        zone(tz)  # validates
        country = (parts[2] if len(parts) > 2 and parts[2] else (known.country if known else "")).upper()
        lat = lon = None
        if len(parts) > 3 and parts[3]:
            try:
                lat_text, lon_text = parts[3].split(",")
                lat, lon = float(lat_text), float(lon_text)
            except ValueError:
                raise ValueError(f"STATIONS 里 {city} 的经纬度应为 纬度,经度：{parts[3]}") from None
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                raise ValueError(f"STATIONS 里 {city} 的经纬度超出范围")
        elif known and known.stid == stid:
            lat, lon = known.lat, known.lon
        name = known.name if known and known.stid == stid else ""
        out[city] = StationSpec(city, stid, tz, country, lat, lon, name)
    return out


def parse_slug(slug: str, pattern: str = DEFAULT_SLUG_PATTERN) -> tuple[str, dt.date] | None:
    """highest-temperature-in-shanghai-on-october-13-2026 -> ('shanghai', 2026-10-13); None when the slug is not one."""
    regex = re.escape(pattern).replace(r"\{city\}", r"(?P<city>[a-z0-9]+(?:-[a-z0-9]+)*)").replace(
        r"\{month\}", r"(?P<month>[a-z]+)").replace(r"\{day\}", r"(?P<day>\d{1,2})").replace(r"\{year\}", r"(?P<year>\d{4})")
    match = re.fullmatch(regex, slug.strip().lower())
    if not match:
        return None
    month = match.group("month")
    if month not in MONTHS:
        return None
    try:
        day = dt.date(int(match.group("year")), MONTHS.index(month) + 1, int(match.group("day")))
    except ValueError:
        return None
    return match.group("city"), day


def make_slug(city: str, day: dt.date, pattern: str = DEFAULT_SLUG_PATTERN) -> str:
    return pattern.format(city=city, month=MONTHS[day.month - 1], day=day.day, year=day.year)


def predict_url(slug: str, ref: str = "") -> str:
    return PREDICT_SITE + slug + (f"?ref={urllib.parse.quote(ref)}" if ref else "")


# --- configuration --------------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    token: str
    admin_id: int
    db_path: str
    stations: dict[str, StationSpec]      # city -> station (CITIES picks which are followed automatically)
    cities: tuple[str, ...]               # cities followed day by day (today + FOLLOW_DAYS, station local date)
    slugs: tuple[str, ...]                # extra market slugs followed as given (MARKET_SLUGS)
    follow_days: int = 1
    keep_hours: int = 30                  # a finished day's market stays followed this long after its local midnight
    slug_pattern: str = DEFAULT_SLUG_PATTERN
    poll: int = 60                        # seconds between monitor cycles (the METAR channel is read every cycle)
    noaa_poll: int = 600                  # seconds between Synoptic (NOAA page source) reads: its free tier is 5,000 requests a month
    wu_poll: int = 900                    # seconds between Weather Underground reads
    predict_poll: int = 20                # seconds between orderbook refreshes
    forecast_minutes: int = 30            # minutes between forecast refreshes
    sources: tuple[str, ...] = ("noaa", "metar", "wu")
    synoptic_token: str = SYNOPTIC_DEMO_TOKEN
    wu_key: str = WU_DEFAULT_KEY
    obs_stale_minutes: int = 150          # no new observation for this long while the day runs: a notice
    forecast: bool = True
    forecast_sigma: float = 1.2           # °C: model error of the forecast peak at the start of the day
    forecast_bias: float = 0.0            # °C added to the forecast peak (METAR readings tend to sit a little under model peaks)
    predict: bool = True
    predict_api_key: str = ""
    predict_ref: str = "B00EA"
    predict_fee_bps: int = 200
    edge_alert: bool = True
    edge_cents: float = 0.10              # net edge (per $1 share) a suggestion needs before it is announced
    edge_confirm: int = 60                # seconds an edge has to hold before it is announced
    edge_cooldown: int = 900              # seconds before the same market and side is announced again
    max_alert: bool = True                # announce each new running maximum of the day
    new_market_alert: bool = True         # announce a followed market the moment Predict lists it

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        e = dict(os.environ if env is None else env)
        stations = dict(KNOWN_STATIONS)
        stations.update(parse_stations(e.get("STATIONS", "")))
        cities = tuple(dict.fromkeys(c.strip().lower() for c in e.get("CITIES", "shanghai").split(",") if c.strip()))
        for city in cities:
            if city not in stations:
                raise ValueError(f"CITIES 里的 {city} 没有站点配置：请在 STATIONS 里写 {city}=站号:时区:国家码")
        pattern = e.get("SLUG_PATTERN", DEFAULT_SLUG_PATTERN).strip() or DEFAULT_SLUG_PATTERN
        if any(f"{{{k}}}" not in pattern for k in ("city", "month", "day", "year")) or not re.fullmatch(r"[a-z0-9{}-]+", pattern):
            raise ValueError("SLUG_PATTERN 必须含 {city} {month} {day} {year}，只用小写字母、数字和 -")
        slugs = tuple(dict.fromkeys(s.strip().lower().rstrip("/").rsplit("/", 1)[-1].split("?")[0]
                                    for s in e.get("MARKET_SLUGS", "").split(",") if s.strip()))
        for slug in slugs:
            if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", slug):
                raise ValueError(f"MARKET_SLUGS 里的 slug 无效：{slug}")
        sources = tuple(dict.fromkeys(s.strip().lower() for s in e.get("SOURCES", "noaa,metar,wu").split(",") if s.strip()))
        if not sources or any(s not in SOURCE_NAMES for s in sources):
            raise ValueError("SOURCES 只能是 noaa、metar、wu 的组合（逗号分隔，按优先级）")
        ref = e.get("PREDICT_REF_CODE", "B00EA").strip()
        if ref and not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", ref):
            raise ValueError("PREDICT_REF_CODE 只能是字母、数字、- 或 _")
        token = e.get("SYNOPTIC_TOKEN", "").strip() or SYNOPTIC_DEMO_TOKEN
        if not re.fullmatch(r"[A-Za-z0-9]{8,64}", token):
            raise ValueError("SYNOPTIC_TOKEN 格式无效")
        wu_key = e.get("WU_API_KEY", "").strip() or WU_DEFAULT_KEY
        if not re.fullmatch(r"[A-Za-z0-9]{8,64}", wu_key):
            raise ValueError("WU_API_KEY 格式无效")
        if not cities and not slugs:
            raise ValueError("CITIES 和 MARKET_SLUGS 至少配置一个")
        return cls(
            token=e.get("TELEGRAM_BOT_TOKEN", "").strip(),
            admin_id=bounded_int(e, "ADMIN_USER_ID", 0, 0, 10 ** 15),
            db_path=e.get("STATE_DB", "./data/tianqi.sqlite3"),
            stations=stations, cities=cities, slugs=slugs,
            follow_days=bounded_int(e, "FOLLOW_DAYS", 1, 0, 7),
            keep_hours=bounded_int(e, "KEEP_HOURS", 30, 1, 240),
            slug_pattern=pattern,
            poll=bounded_int(e, "POLL_SECONDS", 60, 15, 3600),
            noaa_poll=bounded_int(e, "NOAA_POLL_SECONDS", 600, 60, 86400),
            wu_poll=bounded_int(e, "WU_POLL_SECONDS", 900, 60, 86400),
            predict_poll=bounded_int(e, "PREDICT_POLL_SECONDS", 20, 5, 3600),
            forecast_minutes=bounded_int(e, "FORECAST_REFRESH_MINUTES", 30, 5, 1440),
            sources=sources, synoptic_token=token, wu_key=wu_key,
            obs_stale_minutes=bounded_int(e, "OBS_STALE_MINUTES", 150, 30, 1440),
            forecast=flag(e, "FORECAST"),
            forecast_sigma=parse_bounded(e, "FORECAST_SIGMA", "1.2", 0.2, 5),
            forecast_bias=parse_bounded(e, "FORECAST_BIAS", "0", -3, 3),
            predict=flag(e, "PREDICT"),
            predict_api_key=e.get("PREDICT_API_KEY", "").strip(),
            predict_ref=ref,
            predict_fee_bps=bounded_int(e, "PREDICT_FEE_BPS", 200, 0, 1000),
            edge_alert=flag(e, "EDGE_ALERT"),
            edge_cents=parse_bounded(e, "EDGE_ALERT_CENTS", "10", 1, 50) / 100,
            edge_confirm=bounded_int(e, "EDGE_ALERT_CONFIRM_SECONDS", 60, 0, 3600),
            edge_cooldown=bounded_int(e, "EDGE_ALERT_COOLDOWN_SECONDS", 900, 0, 86400),
            max_alert=flag(e, "MAX_ALERT"),
            new_market_alert=flag(e, "NEW_MARKET_ALERT"),
        )


# --- persistence ----------------------------------------------------------------------------------------------------------
class Store:
    """Small persistent JSON records in SQLite. All calls use the event-loop thread."""
    UPSERT = "INSERT INTO records(k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v"

    def __init__(self, path: str):
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("CREATE TABLE IF NOT EXISTS records (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
        self.conn.commit()

    def get(self, key: str, default: Any = None) -> Any:
        row = self.conn.execute("SELECT v FROM records WHERE k=?", (key,)).fetchone()
        return json.loads(row[0]) if row else copy.deepcopy(default)

    def put(self, key: str, value: Any) -> None:
        with self.conn:
            self.conn.execute(self.UPSERT, (key, json.dumps(value, ensure_ascii=False)))

    @staticmethod
    def _bounds(prefix: str) -> tuple[str, str]:
        return prefix, (prefix[:-1] + chr(ord(prefix[-1]) + 1)) if prefix else "\U0010ffff"

    def keys(self, prefix: str) -> list[str]:
        return [k for (k,) in self.conn.execute("SELECT k FROM records WHERE k >= ? AND k < ? ORDER BY k", self._bounds(prefix))]

    def items(self, prefix: str) -> list[tuple[str, Any]]:
        rows = self.conn.execute("SELECT k, v FROM records WHERE k >= ? AND k < ? ORDER BY k", self._bounds(prefix))
        return [(k, json.loads(v)) for k, v in rows]

    def delete_keys(self, keys: list[str]) -> None:
        if keys:
            with self.conn:
                self.conn.executemany("DELETE FROM records WHERE k=?", [(k,) for k in keys])

    def close(self) -> None:
        self.conn.close()


# --- HTTP -----------------------------------------------------------------------------------------------------------------
class RemoteError(Exception):
    def __init__(self, message: str, retry_after: int = 0):
        super().__init__(message)
        self.retry_after = max(0, retry_after)


class PendingData(ValueError):
    """Data that simply has not arrived yet (first fetch still running)."""


USER_AGENT = f"TianqiMonitor/{VERSION} (+https://github.com/7CCCCC21X/tianqi)"
MAX_BODY = 8_000_000
SSL_CONTEXT = ssl.create_default_context()
HTTP_HINTS = {401: "接口拒绝（401）：token / key 无效或缺失", 403: "访问被拒绝（403）：请核对 token、key 与服务地区",
              429: "接口限流（429），稍后重试", 451: "部署所在地访问受限（451）"}
TELEGRAM_HINTS = {409: "Telegram 轮询冲突；同一个 Bot Token 只能运行一个实例"}


def _inflate(raw: bytes, encoding: str) -> bytes:
    encoding = (encoding or "").strip().lower()
    if encoding not in {"gzip", "x-gzip", "deflate"}:
        return raw
    try:
        out = zlib.decompressobj(zlib.MAX_WBITS | (16 if encoding != "deflate" else 0)).decompress(raw, MAX_BODY + 1)
    except zlib.error:
        return raw
    if len(out) > MAX_BODY:
        raise RemoteError("接口返回的数据过大")
    return out


def http_error_text(body: Any) -> str:
    if not isinstance(body, dict):
        return ""
    for key in ("description", "msg", "message", "detail", "error", "reason"):
        value = body.get(key)
        if isinstance(value, dict):
            value = value.get("message") or value.get("msg") or value.get("detail") or ""
        if isinstance(value, list):
            value = "；".join(str(v) for v in value if v)
        if isinstance(value, str) and value.strip():
            return value.strip()[:200]
    return ""


def _http_get(url: str, payload: dict | None = None, timeout: int = 15, headers: dict[str, str] | None = None) -> bytes:
    """GET (or POST ``payload`` as JSON) and return the body; HTTP / network failures become RemoteError."""
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "User-Agent": USER_AGENT, "Accept": "application/json", "Accept-Encoding": "gzip",
        **({"Content-Type": "application/json"} if data is not None else {}), **(headers or {}),
    })
    host = urllib.parse.urlsplit(url).hostname or ""
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CONTEXT) as response:
            raw = response.read(MAX_BODY + 1)
            if len(raw) > MAX_BODY:
                raise RemoteError("接口返回的数据过大")
            return _inflate(raw, response.headers.get("Content-Encoding", ""))
    except urllib.error.HTTPError as error:
        retry = 0
        with contextlib.suppress(ValueError, TypeError):
            retry = max(0, int(error.headers.get("Retry-After", "0")))
        description = ""
        with contextlib.suppress(ValueError, TypeError, AttributeError, RemoteError):
            body = json.loads(_inflate(error.read(4096), error.headers.get("Content-Encoding", "")))
            description = http_error_text(body)
            retry = max(retry, int(body.get("parameters", {}).get("retry_after", 0)))
        hints = TELEGRAM_HINTS if host.endswith("telegram.org") else HTTP_HINTS
        text = hints.get(error.code) or description or "接口请求失败"
        raise RemoteError(clean_error(f"HTTP {error.code}: {text}"), retry) from None
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        reason = getattr(error, "reason", None) if isinstance(error, urllib.error.URLError) else None
        detail = f": {clean_error(str(reason))[:80]}" if reason else ""
        raise RemoteError(f"网络错误 ({type(error).__name__}{detail})") from None
    except http.client.HTTPException as error:
        detail = clean_error(str(error))[:80]
        raise RemoteError(f"网络错误 ({type(error).__name__}{': ' + detail if detail else ''})") from None


def _http_json(url: str, payload: dict | None = None, timeout: int = 15, headers: dict[str, str] | None = None) -> Any:
    try:
        return json.loads(_http_get(url, payload, timeout, headers))
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise RemoteError("接口未返回有效 JSON") from None


async def http_json(url: str, payload: dict | None = None, timeout: int = 15, headers: dict[str, str] | None = None) -> Any:
    return await asyncio.to_thread(_http_json, url, payload, timeout, headers)


def with_query(url: str, params: dict[str, Any]) -> str:
    return url + ("&" if "?" in url else "?") + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})


# --- observations ------------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Obs:
    ts_ms: int               # observation time, epoch ms (UTC)
    temp: float | None       # °C as the source reports it (None = the row has no temperature)
    kind: str = ""           # METAR / SPECI / "" when the source does not say
    raw: str = ""            # the raw report when the source gives one

    def temp_in(self, unit: str) -> int | None:
        """The whole-degree value the page would print in the market's unit."""
        if self.temp is None:
            return None
        return whole_degrees(c_to_f(self.temp) if unit == "F" else self.temp)

    def to_json(self) -> list:
        return [self.ts_ms, self.temp, self.kind]

    @classmethod
    def from_json(cls, row: Any) -> "Obs | None":
        with contextlib.suppress(TypeError, ValueError, IndexError):
            return cls(int(row[0]), None if row[1] is None else float(row[1]), str(row[2] if len(row) > 2 else ""))
        return None


def parse_iso_ms(text: str) -> int | None:
    """'2026-10-13T06:00:00Z' / '2026-10-13T14:00:00+0800' / '2026-10-13 06:00:00' (UTC) -> epoch ms."""
    value = str(text or "").strip()
    if not value:
        return None
    value = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", value.replace("Z", "+00:00"))
    if " " in value and "T" not in value:
        value = value.replace(" ", "T", 1)
    try:
        when = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return int(when.timestamp() * 1000)


def number_or_none(value: Any, fahrenheit: bool = False) -> float | None:
    """A temperature as reported (°F converted to °C); None when absent, unreadable or outside what a station records."""
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    if fahrenheit:
        out = (out - 32) * 5 / 9
    return out if -90 <= out <= 70 else None


def metar_temp(raw: str) -> float | None:
    """The T/Td group of a raw METAR: '24/18' -> 24, 'M02/M05' -> -2."""
    match = re.search(r"\s(M?\d{2})/(M?\d{2}|//)?(?=\s|$)", str(raw or ""))
    if not match:
        return None
    text = match.group(1)
    return -float(text[1:]) if text.startswith("M") else float(text)


def metar_time(raw: str, near_ms: int) -> int | None:
    """The DDHHMMZ group of a raw METAR, placed in the month that puts it closest to ``near_ms``."""
    match = re.search(r"\b(\d{2})(\d{2})(\d{2})Z\b", str(raw or ""))
    if not match:
        return None
    day, hour, minute = int(match.group(1)), int(match.group(2)), int(match.group(3))
    best = None
    anchor = dt.datetime.fromtimestamp(near_ms / 1000, UTC)
    for shift in (0, -1, 1):
        month = anchor.month + shift
        year = anchor.year + (month - 1) // 12
        month = (month - 1) % 12 + 1
        with contextlib.suppress(ValueError):
            when = int(dt.datetime(year, month, day, hour, minute, tzinfo=UTC).timestamp() * 1000)
            if best is None or abs(when - near_ms) < abs(best - near_ms):
                best = when
    return best


def parse_synoptic(data: Any, stid: str) -> tuple[list[Obs], dict]:
    """Synoptic v2 stations/timeseries answer -> (observations, station metadata: TIMEZONE, LATITUDE, LONGITUDE, NAME).
    RESPONSE_CODE other than 1 (bad token, unknown station) is an error; a station without rows is an empty list."""
    if not isinstance(data, dict):
        raise RemoteError("Synoptic 接口返回格式异常")
    summary = data.get("SUMMARY") or {}
    code = summary.get("RESPONSE_CODE")
    if code not in (1, "1"):
        raise RemoteError(f"Synoptic: {summary.get('RESPONSE_MESSAGE') or f'RESPONSE_CODE {code}'}")
    stations = [s for s in data.get("STATION") or [] if isinstance(s, dict)]
    station = next((s for s in stations if str(s.get("STID", "")).upper() == stid.upper()), stations[0] if stations else None)
    if station is None:
        return [], {}
    meta = {k: station.get(k) for k in ("TIMEZONE", "LATITUDE", "LONGITUDE", "NAME", "STID", "ELEVATION") if station.get(k) not in (None, "")}
    obs_block = station.get("OBSERVATIONS") or {}
    times = obs_block.get("date_time") or []
    temps = next((v for k, v in obs_block.items() if k.startswith("air_temp") and isinstance(v, list)), [])
    units = (data.get("UNITS") or {}).get("air_temp", "Celsius")
    fahrenheit = str(units).lower().startswith("f")
    out = []
    for index, when in enumerate(times):
        ts = parse_iso_ms(when)
        if ts is None:
            continue
        out.append(Obs(ts, number_or_none(temps[index] if index < len(temps) else None, fahrenheit)))
    out.sort(key=lambda o: o.ts_ms)
    return out, meta


def parse_metar_json(data: Any, stid: str, now_ms: int) -> list[Obs]:
    """aviationweather.gov /api/data/metar?format=json -> observations (METAR and SPECI), oldest first."""
    rows = data if isinstance(data, list) else (data.get("data") if isinstance(data, dict) else None)
    if not isinstance(rows, list):
        raise RemoteError("METAR 接口返回格式异常")
    out: dict[tuple[int, str], Obs] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("icaoId") or row.get("station_id") or stid).upper() != stid.upper():
            continue
        raw = str(row.get("rawOb") or row.get("raw_text") or "")
        ts = None
        obs_time = row.get("obsTime")
        if isinstance(obs_time, (int, float)) and not isinstance(obs_time, bool):
            ts = int(obs_time * (1 if obs_time > 10 ** 11 else 1000))
        elif isinstance(obs_time, str):
            ts = parse_iso_ms(obs_time)
        if ts is None:
            ts = parse_iso_ms(str(row.get("reportTime") or row.get("observation_time") or "")) or metar_time(raw, now_ms)
        if ts is None:
            continue
        temp = number_or_none(row.get("temp"))
        if temp is None:
            temp = metar_temp(raw)
        kind = str(row.get("metarType") or ("SPECI" if raw.startswith("SPECI") else "METAR")).upper()
        key = (ts, kind)
        if key not in out:
            out[key] = Obs(ts, temp, kind, raw[:120])
    return sorted(out.values(), key=lambda o: o.ts_ms)


def parse_wu(data: Any) -> list[Obs]:
    """api.weather.com v1 historical observations (the Weather Underground history page's table), oldest first."""
    if not isinstance(data, dict):
        raise RemoteError("Weather Underground 接口返回格式异常")
    meta = data.get("metadata") or {}
    status = meta.get("status_code")
    if status not in (None, 200, "200"):
        raise RemoteError(f"Weather Underground: HTTP {status} {http_error_text(data) or ''}".strip())
    rows = data.get("observations")
    if rows is None:
        if data.get("errors"):
            raise RemoteError("Weather Underground: " + (http_error_text({"message": data["errors"][0].get("error", {}).get("message")})
                                                        if isinstance(data["errors"][0], dict) else "接口错误"))
        return []
    units = str(meta.get("units") or "m").lower()
    out = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        when = row.get("valid_time_gmt")
        if not isinstance(when, (int, float)) or isinstance(when, bool):
            continue
        out.append(Obs(int(when * 1000), number_or_none(row.get("temp"), units.startswith("e")), str(row.get("obs_type") or "")))
    out.sort(key=lambda o: o.ts_ms)
    return out


@dataclass
class DayData:
    """One source's observations for one station on one local calendar day."""
    source: str
    stid: str
    day: dt.date
    tz_name: str
    obs: list[Obs] = field(default_factory=list)   # within the local day, oldest first
    after: Obs | None = None                        # the first observation dated the following local day (the lock)
    fetched_ms: int = 0
    error: str = ""                                 # the latest refresh failure (the rows shown are then the previous ones)

    def readings(self, unit: str = "C") -> list[tuple[Obs, int]]:
        return [(o, o.temp_in(unit)) for o in self.obs if o.temp is not None]  # type: ignore[misc]

    def peak(self, unit: str = "C") -> tuple[int, Obs] | None:
        """(highest whole-degree reading of the day, the first observation that reached it)."""
        best: tuple[int, Obs] | None = None
        for obs, value in self.readings(unit):
            if best is None or value > best[0]:
                best = (value, obs)
        return best

    @property
    def latest(self) -> Obs | None:
        return next((o for o in reversed(self.obs) if o.temp is not None), None)

    @property
    def locked(self) -> bool:
        return self.after is not None

    def to_json(self) -> dict:
        return {"source": self.source, "stid": self.stid, "day": self.day.isoformat(), "tz": self.tz_name,
                "obs": [o.to_json() for o in self.obs], "after": self.after.to_json() if self.after else None,
                "fetched_ms": self.fetched_ms}

    @classmethod
    def from_json(cls, data: Any) -> "DayData | None":
        try:
            day = dt.date.fromisoformat(str(data["day"]))
            rows = [o for o in (Obs.from_json(r) for r in data.get("obs") or []) if o]
            after = Obs.from_json(data["after"]) if data.get("after") else None
            return cls(str(data["source"]), str(data["stid"]), day, str(data.get("tz") or ""), rows, after, int(data.get("fetched_ms") or 0))
        except (KeyError, TypeError, ValueError, AttributeError):
            return None


def slice_day(rows: list[Obs], day: dt.date, tz: dt.tzinfo) -> tuple[list[Obs], Obs | None]:
    """The rows dated ``day`` in the station's local time, and the first row dated the day after (None when none yet)."""
    start, end = day_bounds(day, tz)
    inside = [o for o in rows if start <= o.ts_ms < end]
    after = next((o for o in rows if o.ts_ms >= end and local_date(o.ts_ms, tz) == day + dt.timedelta(days=1)), None)
    return inside, after


def compare_days(old: DayData | None, new: DayData, unit: str = "C") -> list[str]:
    """What changed between two readings of the same source and day that a watcher must hear about: a stored row's
    value differing now (a revision) or a row that disappeared while later rows remain."""
    if old is None:
        return []
    notes = []
    old_rows = {o.ts_ms: o for o in old.obs if o.temp is not None}
    new_rows = {o.ts_ms: o for o in new.obs if o.temp is not None}
    tz = zone(new.tz_name) if new.tz_name else UTC
    for ts, row in old_rows.items():
        if ts in new_rows and new_rows[ts].temp_in(unit) != row.temp_in(unit):
            notes.append(f"{stamp(ts, tz, date=False)} 的读数由 {row.temp_in(unit)}° 改为 {new_rows[ts].temp_in(unit)}°")
    last_new = max(new_rows, default=0)
    gone = [ts for ts in old_rows if ts not in new_rows and ts < last_new]
    if gone:
        notes.append(f"{len(gone)} 条已发布的读数消失（{'、'.join(stamp(ts, tz, date=False) for ts in gone[:3])}）")
    return notes


class WeatherFeed:
    """Observations for every (station, local day) the bot follows, from each enabled source, kept in memory and in the
    store (a restart must not forget a revision it saw, nor re-announce the day's maximum)."""

    def __init__(self, config: Config, store: Store):
        self.config = config
        self.store = store
        self.days: dict[tuple[str, str, str], DayData] = {}   # (source, stid, day) -> data
        self.meta: dict[str, dict] = {}                        # stid -> Synoptic station metadata (TIMEZONE, LATITUDE...)
        self.errors: dict[tuple[str, str], str] = {}           # (source, stid) -> latest refresh failure
        self.refreshed: dict[tuple[str, str], float] = {}      # (source, stid) -> monotonic time of the last success
        self.revisions: list[tuple[str, str, dt.date, str]] = []  # (source, stid, day, note) found by the latest refresh
        for key, value in store.items("obs:"):
            data = DayData.from_json(value)
            if data:
                self.days[(data.source, data.stid, data.day.isoformat())] = data
        for key, value in store.items("station_meta:"):
            if isinstance(value, dict):
                self.meta[key.split(":", 1)[1]] = value

    # -- requests ------------------------------------------------------------------------------------------------------
    async def fetch_synoptic(self, stid: str, start_ms: int, end_ms: int) -> tuple[list[Obs], dict]:
        params = {"stid": stid, "token": self.config.synoptic_token, "vars": "air_temp", "units": "metric",
                  "obtimezone": "UTC", "output": "json", "showemptystations": "1",
                  "start": dt.datetime.fromtimestamp(start_ms / 1000, UTC).strftime("%Y%m%d%H%M"),
                  "end": dt.datetime.fromtimestamp(end_ms / 1000, UTC).strftime("%Y%m%d%H%M")}
        data = await http_json(with_query(SYNOPTIC_URL, params), timeout=SOURCE_TIMEOUT)
        return parse_synoptic(data, stid)

    async def fetch_metar(self, stid: str, start_ms: int, now_ms: int) -> list[Obs]:
        hours = min(72, max(2, int((now_ms - start_ms) / 3_600_000) + 2))
        data = await http_json(with_query(METAR_URL, {"ids": stid, "format": "json", "hours": hours}), timeout=SOURCE_TIMEOUT)
        return parse_metar_json(data, stid, now_ms)

    async def fetch_wu(self, station: StationSpec, day: dt.date) -> list[Obs]:
        if not station.country:
            raise RemoteError("未配置国家码，无法拼出 Weather Underground 站点 id")
        url = WU_URL.format(stid=station.stid, country=station.country)
        params = {"apiKey": self.config.wu_key, "units": "m", "startDate": day.strftime("%Y%m%d"),
                  "endDate": (day + dt.timedelta(days=1)).strftime("%Y%m%d")}
        data = await http_json(with_query(url, params), timeout=SOURCE_TIMEOUT)
        return parse_wu(data)

    # -- refresh -------------------------------------------------------------------------------------------------------
    def tz_for(self, station: StationSpec) -> dt.tzinfo:
        """The station's zone: the configured one, else what Synoptic says about the station."""
        if station.tz:
            return zone(station.tz)
        meta_tz = str((self.meta.get(station.stid) or {}).get("TIMEZONE") or "")
        if meta_tz:
            return zone(meta_tz)
        raise ValueError(f"{station.stid} 的时区未知（配置 STATIONS 或等待 Synoptic 站点信息）")

    def get(self, source: str, stid: str, day: dt.date) -> DayData | None:
        return self.days.get((source, stid, day.isoformat()))

    def official(self, stid: str, day: dt.date) -> DayData | None:
        """The day's data as the resolution page shows it. Synoptic (the page's own source) and the AWC METAR feed carry
        the same reports; whichever has the newer observation is used (Synoptic on a tie), so a new METAR is seen within
        one cycle even though Synoptic is read less often. Weather Underground only when neither has rows."""
        best: DayData | None = None
        for source in self.config.sources:
            if source == "wu":
                continue
            data = self.get(source, stid, day)
            if not data or not (data.obs or data.after):
                continue
            newest = max([o.ts_ms for o in data.obs] + ([data.after.ts_ms] if data.after else []))
            best_newest = max([o.ts_ms for o in best.obs] + ([best.after.ts_ms] if best.after else [])) if best else -1
            if best is None or newest > best_newest:
                best = data
        if best is None and "wu" in self.config.sources:
            data = self.get("wu", stid, day)
            if data and (data.obs or data.after):
                best = data
        return best

    async def probe(self, stid: str, now_ms: int) -> dict:
        """Station metadata (TIMEZONE, LATITUDE, LONGITUDE, NAME) from Synoptic, for a station the market rules name
        but the configuration does not."""
        if stid in self.meta:
            return self.meta[stid]
        _, meta = await self.fetch_synoptic(stid, now_ms - 3 * 3_600_000, now_ms)
        if not meta:
            raise RemoteError(f"Synoptic 没有站点 {stid} 的信息")
        self.meta[stid] = meta
        self.store.put(f"station_meta:{stid}", meta)
        return meta

    def due(self, source: str, stid: str) -> bool:
        interval = {"noaa": self.config.noaa_poll, "wu": self.config.wu_poll}.get(source, 0)
        return time.monotonic() - self.refreshed.get((source, stid), -1e9) >= interval

    async def refresh(self, station: StationSpec, days: list[dt.date], now_ms: int, force: bool = False) -> None:
        """Every enabled source that is due for every followed day of the station; a failing source, or one answering
        without the day's rows, keeps the rows already seen (with the failure noted)."""
        tz = self.tz_for(station)
        tz_name = zone_name(tz)
        self.revisions = [r for r in self.revisions if r[1] != station.stid]
        if not days:
            return
        first_start = min(day_bounds(d, tz)[0] for d in days)
        last_end = max(day_bounds(d, tz)[1] for d in days)
        window_end = min(now_ms + 60_000, last_end + 18 * 3_600_000)  # the following day's first rows are wanted too
        for source in self.config.sources:
            if not force and not self.due(source, station.stid):
                continue
            try:
                if source == "noaa":
                    rows, meta = await self.fetch_synoptic(station.stid, first_start - 3_600_000, window_end)
                    if meta:
                        self.meta[station.stid] = meta
                        self.store.put(f"station_meta:{station.stid}", meta)
                elif source == "metar":
                    rows = await self.fetch_metar(station.stid, first_start, now_ms)
                else:
                    rows = []
                    for day in days:
                        rows.extend(await self.fetch_wu(station, day))
                    rows = sorted({(o.ts_ms, o.temp): o for o in rows}.values(), key=lambda o: o.ts_ms)
            except (RemoteError, TimeoutError, OSError, ValueError) as error:
                text = clean_error(error) or type(error).__name__
                self.errors[(source, station.stid)] = text
                for day in days:
                    data = self.get(source, station.stid, day)
                    if data:
                        data.error = text
                continue
            self.errors.pop((source, station.stid), None)
            self.refreshed[(source, station.stid)] = time.monotonic()
            for day in days:
                inside, after = slice_day(rows, day, tz)
                old = self.get(source, station.stid, day)
                new = DayData(source, station.stid, day, tz_name, inside, after, now_ms)
                if old and not inside and old.obs:
                    # an answer without the day's rows (a short METAR window, a transient empty answer) is not a day
                    # that lost its readings: the rows already seen stay, and the card says so
                    new.obs, new.after = old.obs, old.after or after
                    new.error = "本次刷新未返回当日数据，沿用上次读数"
                for note in compare_days(old, new):
                    self.revisions.append((source, station.stid, day, note))
                self.days[(source, station.stid, day.isoformat())] = new
                self.store.put(f"obs:{source}:{station.stid}:{day.isoformat()}", new.to_json())

    def prune(self, now_ms: int) -> None:
        limit = (dt.datetime.fromtimestamp(now_ms / 1000, UTC) - dt.timedelta(days=OBS_KEEP_DAYS)).date().isoformat()
        stale = [k for k in self.days if k[2] < limit]
        for key in stale:
            self.days.pop(key)
        self.store.delete_keys([f"obs:{k[0]}:{k[1]}:{k[2]}" for k in stale])

    def source_line(self, station: StationSpec, day: dt.date, unit: str) -> list[str]:
        """One line per source for the status card: its peak, its row count, its error."""
        lines = []
        for source in self.config.sources:
            data = self.get(source, station.stid, day)
            error = self.errors.get((source, station.stid), "")
            name = SOURCE_NAMES[source]
            if data and data.obs:
                peak = data.peak(unit)
                tz = zone(data.tz_name) if data.tz_name else UTC
                text = (f"{name}：最高 {peak[0]}°{unit}（{stamp(peak[1].ts_ms, tz, date=False)}）｜{len(data.obs)} 条"
                        if peak else f"{name}：{len(data.obs)} 条，无温度读数")
                if data.after:
                    text += "｜次日首条已出"
                if error:
                    text += f"｜⚠️ 刷新失败：{brief_error(error, 50)}"
                lines.append(text)
            elif error:
                lines.append(f"{name}：❌ {brief_error(error, 70)}")
            elif data is not None:
                lines.append(f"{name}：暂无当日数据")
            else:
                lines.append(f"{name}：等待首次获取")
        return lines


# --- market brackets ------------------------------------------------------------------------------------------------------
@dataclass(frozen=True, order=True)
class Bracket:
    """One outcome of a highest-temperature market: whole degrees from ``low`` to ``high`` inclusive; None = open end."""
    sort_key: float = field(compare=True, repr=False)
    low: int | None = field(default=None, compare=False)
    high: int | None = field(default=None, compare=False)
    unit: str = field(default="C", compare=False)

    @classmethod
    def make(cls, low: int | None, high: int | None, unit: str = "C") -> "Bracket":
        return cls(float("-inf") if low is None else low, low, high, unit)

    def contains(self, value: int) -> bool:
        return (self.low is None or value >= self.low) and (self.high is None or value <= self.high)

    @property
    def label(self) -> str:
        u = f"°{self.unit}"
        if self.low is None and self.high is None:
            return "任意"
        if self.low is None:
            return f"≤{self.high}{u}"
        if self.high is None:
            return f"≥{self.low}{u}"
        if self.low == self.high:
            return f"{self.low}{u}"
        return f"{self.low}–{self.high}{u}"


DEG_UNIT = r"(?:\s*°\s*([cf])?|\s*([cf])\b)?"


def _unit(groups: tuple, default: str) -> str:
    for g in groups:
        if g:
            return g.upper()
    return default


def normalise_title(text: str) -> str:
    text = str(text or "").replace("º", "°").replace("˚", "°").replace("℃", "°C").replace("℉", "°F")
    text = text.replace("＜", "<").replace("＞", ">").replace("－", "-").replace("～", "~").replace("°", " ° ")
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text.replace(" ° ", "°")


def parse_bracket(text: str, default_unit: str = "C") -> Bracket | None:
    """'22°C or below' / '≤22°C' / '23°C' / '27-28°F' / '28°C or higher' / '28°C 或以上' / 'Will ... be 26°C on Oct 13?'
    -> Bracket; None when the text names no temperature (a bare number without a degree sign is not one)."""
    t = normalise_title(text)
    num = r"(-?\d{1,3})"
    patterns: list[tuple[str, Any]] = [
        (num + DEG_UNIT + r"\s*(?:or\s+|或\s*|及)?(?:below|lower|less|under|colder|以下)", lambda m: (None, int(m.group(1)), _unit(m.groups()[1:3], default_unit))),
        (r"(?:≤|<=|at most|no more than|不超过|不高于)\s*" + num + DEG_UNIT, lambda m: (None, int(m.group(1)), _unit(m.groups()[1:3], default_unit))),
        (num + DEG_UNIT + r"\s*(?:or\s+|或\s*|及)?(?:higher|above|more|over|warmer|hotter|以上)", lambda m: (int(m.group(1)), None, _unit(m.groups()[1:3], default_unit))),
        (r"(?:≥|>=|at least|不低于)\s*" + num + DEG_UNIT, lambda m: (int(m.group(1)), None, _unit(m.groups()[1:3], default_unit))),
        (r"\b(?:below|under)\s+" + num + r"\s*°\s*([cf])?", lambda m: (None, int(m.group(1)) - 1, _unit(m.groups()[1:2], default_unit))),
        (r"\b(?:above|over)\s+" + num + r"\s*°\s*([cf])?", lambda m: (int(m.group(1)) + 1, None, _unit(m.groups()[1:2], default_unit))),
        (num + r"(?:\s*°\s*([cf])?)?\s*(?:-|–|—|~|to|至|到|and)\s*" + num + r"\s*°\s*([cf])?",
         lambda m: (int(m.group(1)), int(m.group(3)), _unit((m.group(4), m.group(2)), default_unit))),
        (num + r"\s*°\s*([cf])?", lambda m: (int(m.group(1)), int(m.group(1)), _unit(m.groups()[1:2], default_unit))),
    ]
    for pattern, build in patterns:
        match = re.search(pattern, t)
        if match:
            low, high, unit = build(match)
            if low is not None and high is not None and low > high:
                low, high = high, low
            return Bracket.make(low, high, unit)
    return None


def rules_unit(rules: str) -> str:
    text = str(rules or "")
    match = re.search(r"(?i)degrees?\s+(celsius|fahrenheit)", text)
    if match:
        return "C" if match.group(1).lower() == "celsius" else "F"
    match = re.search(r"°\s*([CF])\b", text)
    return match.group(1).upper() if match else "C"


def rules_site(rules: str) -> str:
    """The NWS station the rules name: timeseries?site=zspd -> ZSPD ('' when absent)."""
    match = re.search(r"(?i)timeseries\?site=([a-z0-9]{3,8})", str(rules or ""))
    return match.group(1).upper() if match else ""


def rules_wu_id(rules: str) -> str:
    match = re.search(r"(?i)wunderground\.com/history/daily/([a-z]{2})/[^/\s]+/([a-z0-9]{3,8})", str(rules or ""))
    return f"{match.group(2).upper()}:9:{match.group(1).upper()}" if match else ""


# --- Predict.fun ------------------------------------------------------------------------------------------------------------
def predict_levels(rows: Any, bids: bool) -> tuple[tuple[D, D], ...]:
    """[[price, size], ...] or [{price, size}, ...] -> sorted (price, size) with empty levels dropped."""
    out = []
    for row in rows if isinstance(rows, list) else []:
        try:
            price, size = (row[0], row[1]) if isinstance(row, (list, tuple)) else (row.get("price"), row.get("size", row.get("quantity")))
            price, size = D(str(price)), D(str(size))
        except (IndexError, TypeError, AttributeError, decimal.InvalidOperation):
            continue
        if price.is_finite() and size.is_finite() and 0 < price < 1 and size > 0:
            out.append((price, size))
    out.sort(key=lambda level: level[0], reverse=bids)
    return tuple(out[:PREDICT_DEPTH])


def predict_markets(data: Any) -> list[dict]:
    """Market records ({id, conditionId, title, question}) anywhere in a GraphQL / REST answer."""
    found: list[dict] = []

    def walk(node: Any, depth: int = 0) -> None:
        if depth > 8:
            return
        if isinstance(node, dict):
            if node.get("id") is not None and ("conditionId" in node or "question" in node) and "title" in node:
                found.append({"id": str(node["id"]), "conditionId": str(node.get("conditionId") or ""),
                              "title": str(node.get("title") or node.get("question") or ""),
                              "question": str(node.get("question") or "")})
                return
            for value in node.values():
                walk(value, depth + 1)
        elif isinstance(node, list):
            for value in node:
                walk(value, depth + 1)
    walk(data)
    unique: dict[str, dict] = {}
    for market in found:
        unique.setdefault(market["id"], market)
    return list(unique.values())


def predict_resolution(market: dict) -> dict | None:
    """What a Predict market object says it settled on: {"index", "name", "split", "how"}; None while open / unreadable."""
    rows = sorted((o for o in market.get("outcomes") or [] if isinstance(o, dict)), key=lambda o: int(o.get("indexSet") or 0))
    names = [str(o.get("name") or "") for o in rows]
    won = [i for i, o in enumerate(rows) if str(o.get("status") or "").strip().upper() in {"WON", "WIN", "WINNER"}]
    if len(won) == 1:
        return {"index": won[0], "name": names[won[0]], "split": False, "how": "outcomes.status"}
    if len(won) == len(rows) == 2:
        return {"index": None, "name": "50/50", "split": True, "how": "outcomes.status"}
    pays = []
    for o in rows:
        value = next((o[k] for k in ("payout", "payoutNumerator", "payoutPerShare") if o.get(k) not in (None, "")), None)
        with contextlib.suppress(TypeError, ValueError):
            pays.append(float(value))
    if rows and len(pays) == len(rows) and sum(pays) > 0:
        top = max(pays)
        if all(abs(p - top) < 1e-9 for p in pays):
            return {"index": None, "name": "50/50", "split": True, "how": "outcomes.payout"}
        if sum(1 for p in pays if p > 0) == 1:
            i = pays.index(top)
            return {"index": i, "name": names[i], "split": False, "how": "outcomes.payout"}
    status = str(market.get("status") or "").upper()
    if not any(word in status for word in ("RESOLV", "SETTL", "FINAL")):
        return None
    res = next((market.get(k) for k in ("resolution", "resolvedOutcome", "winningOutcome", "winner") if market.get(k)), None)
    name = (str(res.get("name") or res.get("outcome") or "") if isinstance(res, dict) else res if isinstance(res, str) else "")
    index = None
    if isinstance(res, dict) and str(res.get("indexSet") or "").isdigit():
        index = next((i for i, o in enumerate(rows) if str(o.get("indexSet")) == str(res["indexSet"])), None)
    if index is None and name:
        index = next((i for i, n in enumerate(names) if n.strip().lower() == str(name).strip().lower()), None)
    if index is None:
        return None
    return {"index": index, "name": names[index], "split": False, "how": "resolution"}


@dataclass(frozen=True)
class PredictBook:
    market_id: str
    bids: tuple[tuple[D, D], ...]
    asks: tuple[tuple[D, D], ...]
    fetched_ms: int

    @property
    def bid(self) -> D | None:
        return self.bids[0][0] if self.bids else None

    @property
    def ask(self) -> D | None:
        return self.asks[0][0] if self.asks else None

    def stale(self, now_ms: int) -> bool:
        return now_ms - self.fetched_ms > PREDICT_STALE_MS

    def flipped(self) -> "PredictBook":
        """The complementary outcome's book: its bids are 1 − the asks, its asks 1 − the bids."""
        flip = lambda rows: tuple((D(1) - p, q) for p, q in rows)
        return PredictBook(self.market_id, flip(self.asks), flip(self.bids), self.fetched_ms)

    def text(self) -> str:
        bid = f"{float(self.bid) * 100:.0f}¢" if self.bid is not None else "—"
        ask = f"{float(self.ask) * 100:.0f}¢" if self.ask is not None else "—"
        return f"买1 {bid}/卖1 {ask}"


@dataclass
class Level:
    """One bracket market of a ladder with its latest book (priced for Yes) and details."""
    bracket: Bracket
    market_id: str
    title: str
    question: str = ""
    book: PredictBook | None = None
    error: str = ""
    meta: dict = field(default_factory=dict)   # outcomes, status, trading_status, fee_bps, rules, resolved

    @property
    def settled(self) -> bool:
        return bool(re.search(r"RESOLV|SETTL|FINAL", str(self.meta.get("status", "")).upper())) or self.meta.get("resolved") is not None

    @property
    def closed(self) -> bool:
        text = f"{self.meta.get('status', '')} {self.meta.get('trading_status', '')}".upper()
        return bool(re.search(r"\b(RESOLV\w*|SETTL\w*|FINAL\w*|CLOSED|ENDED|EXPIR\w*|CANCEL\w*)\b", text))

    def yes_book(self) -> tuple[PredictBook | None, str]:
        """The book priced as Yes whatever the market's outcome order; (None, why) when it cannot be told."""
        if self.book is None:
            return None, self.error or "暂无盘口"
        names = self.meta.get("outcomes") or []
        first = str(names[0]).strip().lower() if names else ""
        if first in {"yes", "是"} or not names:
            return self.book, "" if names else "结果顺序未取得，按第一结果为 Yes"
        if first in {"no", "否"}:
            return self.book.flipped(), ""
        return None, f"盘口方向未确认（结果名称：{'、'.join(map(str, names))}）"

    def resolved_yes(self) -> bool | None:
        """True when Predict settled this bracket as the winner, False when as a loser, None while open."""
        res = self.meta.get("resolved")
        if not res or res.get("split"):
            return None
        names = [str(n).strip().lower() for n in self.meta.get("outcomes") or []]
        name = str(res.get("name", "")).strip().lower()
        if name in {"yes", "是"}:
            return True
        if name in {"no", "否"}:
            return False
        index = res.get("index")
        if isinstance(index, int) and 0 <= index < len(names):
            return names[index] in {"yes", "是"}
        return None


@dataclass
class Ladder:
    slug: str
    levels: list[Level]
    fetched_ms: int = 0
    error: str = ""               # a failure that leaves the ladder unknown (network), "" when the rows are fresh
    listed: bool = True           # False = Predict has no such category (an answer, not a failure)

    @property
    def rules(self) -> str:
        return next((str(lv.meta.get("rules") or "") for lv in self.levels if lv.meta.get("rules")), "")

    @property
    def unit(self) -> str:
        return self.levels[0].bracket.unit if self.levels else rules_unit(self.rules)

    def level_for(self, value: int) -> Level | None:
        return next((lv for lv in self.levels if lv.bracket.contains(value)), None)

    def winner(self) -> Level | None:
        return next((lv for lv in self.levels if lv.resolved_yes()), None)

    @property
    def any_settled(self) -> bool:
        return any(lv.settled for lv in self.levels)


class PredictFeed:
    """Resolves each followed slug to its bracket markets and polls their orderbooks (GraphQL + REST, read-only)."""

    def __init__(self, config: Config):
        self.config = config
        self.ladders: dict[str, Ladder] = {}
        self.markets: dict[str, tuple[list[dict] | None, float]] = {}  # slug -> (markets or None = not listed, when)
        self.meta: dict[str, tuple[dict, float]] = {}                   # market id -> (details, when read)
        self.meta_errors: dict[str, str] = {}
        self.book_keys: dict[str, str] = {}
        self.gate = asyncio.Semaphore(PREDICT_PARALLEL)
        self.blocked_until = 0.0
        self.refreshed = -1e9

    def headers(self, url: str) -> dict[str, str]:
        return {"x-api-key": self.config.predict_api_key} if self.config.predict_api_key and url.startswith(PREDICT_REST) else {}

    async def fetch(self, url: str, payload: dict | None = None) -> Any:
        left = self.blocked_until - time.monotonic()
        if left > 0:
            raise RemoteError(f"Predict 接口限流冷却中（{int(left) + 1} 秒后重试）", int(left) + 1)
        async with self.gate:
            try:
                return await http_json(url, payload, PREDICT_TIMEOUT, self.headers(url))
            except RemoteError as error:
                if error.retry_after or "429" in str(error):
                    self.blocked_until = max(self.blocked_until, time.monotonic() + max(error.retry_after, PREDICT_BACKOFF_SECONDS))
                raise

    async def graphql(self, query: str, variables: dict) -> Any:
        data = await self.fetch(PREDICT_GRAPHQL, {"query": query, "variables": variables})
        if isinstance(data, dict) and data.get("errors"):
            raise RemoteError("Predict GraphQL 错误：" + clean_error(json.dumps(data["errors"], ensure_ascii=False))[:120])
        return data.get("data") if isinstance(data, dict) else None

    async def find(self, slug: str) -> list[dict]:
        """slug -> its markets via GraphQL category(id) → markets(categoryId); REST /categories/<slug> as fallback.
        [] = not listed; network trouble raises (and is not cached as "not listed")."""
        found: list[dict] = []
        errors: list[str] = []
        try:
            data = await self.graphql("query($id: ID!) { category(id: $id) { id __typename } }", {"id": slug})
            category = (data or {}).get("category") or {}
            if category.get("id") is not None:
                data = await self.graphql(
                    "query($f: MarketFilterInput!) { markets(filter: $f, pagination: { first: 50 }) "
                    "{ edges { node { id conditionId title question } } } }", {"f": {"categoryId": str(category["id"])}})
                found = predict_markets(data)
        except (RemoteError, TimeoutError, OSError) as error:
            errors.append(clean_error(error))
        if not found:
            try:
                found = predict_markets(await self.fetch(f"{PREDICT_REST}/categories/{urllib.parse.quote(slug)}"))
            except (RemoteError, TimeoutError, OSError) as error:
                if "HTTP 404" not in str(error) and errors:
                    errors.append(clean_error(error))
                else:
                    errors.clear()
        if not found and errors:
            raise RemoteError("；".join(errors))
        return found

    async def resolve(self, slug: str) -> list[dict] | None:
        cached = self.markets.get(slug)
        if cached and (cached[0] is not None or time.monotonic() - cached[1] < PREDICT_MISS_SECONDS):
            return cached[0]
        found = await self.find(slug) or None
        self.markets[slug] = (found, time.monotonic())
        return found

    async def market_details(self, market_id: str) -> dict:
        data = await self.fetch(f"{PREDICT_REST}/markets/{urllib.parse.quote(market_id)}")
        market = data.get("data", data) if isinstance(data, dict) else {}
        outcomes = market.get("outcomes") if isinstance(market, dict) else None
        if not isinstance(outcomes, list) or not outcomes:
            raise RemoteError("Predict 市场详情缺少 outcomes")
        rows = sorted((o for o in outcomes if isinstance(o, dict)), key=lambda o: int(o.get("indexSet") or 0))
        fee = None
        with contextlib.suppress(ValueError, TypeError):
            stated = int(market.get("feeRateBps"))
            fee = stated if 0 <= stated <= 10_000 else None
        return {"outcomes": [str(o.get("name") or "") for o in rows], "status": str(market.get("status") or ""),
                "trading_status": str(market.get("tradingStatus") or ""), "fee_bps": fee,
                "rules": str(market.get("description") or market.get("rules") or "")[:4000],
                "question": str(market.get("question") or ""), "title": str(market.get("title") or ""),
                "resolved": predict_resolution(market)}

    async def orderbook(self, market: dict) -> tuple[tuple, tuple]:
        keys = [k for k in dict.fromkeys([self.book_keys.get(market["id"]), market["id"], market.get("conditionId")]) if k]
        last = ""
        for key in keys:
            try:
                data = await self.fetch(f"{PREDICT_REST}/markets/{urllib.parse.quote(key)}/orderbook")
            except RemoteError as error:
                last = clean_error(error)
                if any(code in last for code in ("429", "401", "403")):
                    break
                continue
            book = data.get("data", data) if isinstance(data, dict) else {}
            if not isinstance(book, dict) or ("bids" not in book and "asks" not in book):
                last = "订单簿格式异常"
                continue
            self.book_keys[market["id"]] = key
            return predict_levels(book.get("bids"), True), predict_levels(book.get("asks"), False)
        raise RemoteError(last or "订单簿获取失败")

    async def level(self, slug: str, market: dict, unit: str, now_ms: int) -> Level | None:
        bracket = parse_bracket(market.get("title", ""), unit) or parse_bracket(market.get("question", ""), unit)
        if bracket is None:
            return None
        meta = self.meta.get(market["id"])
        if meta is None or time.monotonic() - meta[1] >= PREDICT_META_SECONDS:
            try:
                self.meta[market["id"]] = (await self.market_details(market["id"]), time.monotonic())
                self.meta_errors.pop(market["id"], None)
            except (RemoteError, TimeoutError, OSError) as error:
                self.meta_errors[market["id"]] = clean_error(error) or type(error).__name__
        details = (self.meta.get(market["id"]) or ({}, 0))[0]
        previous = next((lv for lv in (self.ladders.get(slug).levels if slug in self.ladders else []) if lv.market_id == market["id"]), None)
        level = Level(bracket, market["id"], market.get("title", ""), market.get("question", ""), None, "", details)
        if level.settled:
            return level  # a settled bracket has no live book (asking gets HTTP 400) and needs none
        try:
            bids, asks = await self.orderbook(market)
            level.book = PredictBook(market["id"], bids, asks, now_ms)
        except (RemoteError, TimeoutError, OSError) as error:
            level.book = previous.book if previous else None  # keep the last book (it ages: stale after PREDICT_STALE_MS)
            level.error = clean_error(error) or type(error).__name__
        return level

    async def refresh_one(self, slug: str, now_ms: int) -> None:
        try:
            markets = await self.resolve(slug)
        except (RemoteError, TimeoutError, OSError) as error:
            old = self.ladders.get(slug)
            self.ladders[slug] = Ladder(slug, old.levels if old else [], old.fetched_ms if old else 0, clean_error(error) or type(error).__name__)
            return
        if not markets:
            self.ladders[slug] = Ladder(slug, [], now_ms, "", listed=False)
            return
        unit = "C"
        for market in markets:  # the rules (read with the first market's details) say °C or °F for the whole ladder
            cached = self.meta.get(market["id"])
            if cached and cached[0].get("rules"):
                unit = rules_unit(cached[0]["rules"])
                break
        levels = [lv for lv in await asyncio.gather(*(self.level(slug, m, unit, now_ms) for m in markets)) if lv]
        if not levels:
            self.ladders[slug] = Ladder(slug, [], now_ms, "Predict 市场标题里没有可识别的气温档位")
            return
        levels.sort(key=lambda lv: lv.bracket)
        self.ladders[slug] = Ladder(slug, levels, now_ms, "")

    async def refresh(self, slugs: list[str], now_ms: int, force: bool = False) -> bool:
        if not force and time.monotonic() - self.refreshed < self.config.predict_poll:
            return False
        self.refreshed = time.monotonic()
        for slug in list(self.ladders):
            if slug not in slugs:
                self.ladders.pop(slug)
                self.markets.pop(slug, None)
        await asyncio.gather(*(self.refresh_one(slug, now_ms) for slug in slugs))
        return True


# --- forecast and model -------------------------------------------------------------------------------------------------------
class Forecast:
    """Open-Meteo hourly 2 m temperature for each station (only the model uses it; the market never does)."""

    def __init__(self, config: Config):
        self.config = config
        self.hourly: dict[str, list[tuple[int, float]]] = {}  # stid -> [(ts_ms, °C)] oldest first
        self.errors: dict[str, str] = {}
        self.refreshed: dict[str, float] = {}

    async def refresh(self, station: StationSpec, lat: float | None, lon: float | None, force: bool = False) -> None:
        if not self.config.forecast:
            return
        if lat is None or lon is None:
            self.errors[station.stid] = "缺少经纬度（STATIONS 里补上，或等待 Synoptic 站点信息）"
            return
        if not force and time.monotonic() - self.refreshed.get(station.stid, -1e9) < self.config.forecast_minutes * 60:
            return
        params = {"latitude": f"{lat:.4f}", "longitude": f"{lon:.4f}", "hourly": "temperature_2m", "timezone": "UTC",
                  "forecast_days": 3, "past_days": 1}
        try:
            data = await http_json(with_query(OPEN_METEO_URL, params), timeout=SOURCE_TIMEOUT)
            rows = parse_open_meteo(data)
        except (RemoteError, TimeoutError, OSError) as error:
            self.errors[station.stid] = clean_error(error) or type(error).__name__
            return
        self.hourly[station.stid] = rows
        self.errors.pop(station.stid, None)
        self.refreshed[station.stid] = time.monotonic()

    def peak(self, stid: str, day: dt.date, tz: dt.tzinfo, after_ms: int | None = None) -> tuple[float, int] | None:
        """(highest forecast °C, its hour) among the day's hours (after ``after_ms`` when given); None when none."""
        start, end = day_bounds(day, tz)
        rows = [(t, ts) for ts, t in self.hourly.get(stid, []) if start <= ts < end and (after_ms is None or ts > after_ms)]
        return max(rows) if rows else None


def parse_open_meteo(data: Any) -> list[tuple[int, float]]:
    if not isinstance(data, dict) or not isinstance(data.get("hourly"), dict):
        raise RemoteError("Open-Meteo: " + (http_error_text(data) or "返回格式异常"))
    times = data["hourly"].get("time") or []
    temps = data["hourly"].get("temperature_2m") or []
    offset = int(data.get("utc_offset_seconds") or 0)
    out = []
    for index, when in enumerate(times):
        ts = parse_iso_ms(str(when)) if "+" in str(when) or str(when).endswith("Z") else None
        if ts is None:
            try:
                naive = dt.datetime.fromisoformat(str(when))
            except ValueError:
                continue
            ts = int((naive.replace(tzinfo=UTC).timestamp() - offset) * 1000)
        temp = number_or_none(temps[index] if index < len(temps) else None)
        if temp is not None:
            out.append((ts, temp))
    out.sort()
    return out


@dataclass(frozen=True)
class Fair:
    probs: tuple[float, ...]       # per ladder level, same order
    note: str
    mu: float | None = None        # model mean of the remaining window's peak, market unit
    sigma: float | None = None
    observed: int | None = None    # the running maximum the probabilities build on
    locked: bool = False


def fair_probabilities(brackets: list[Bracket], observed: int | None, locked: bool, rest_peak: float | None,
                       sigma: float, bias: float, hours_left: float, unit: str = "C") -> Fair | None:
    """Where the day's maximum ends up. The reading already recorded is a floor (the market counts the highest reading
    of the day); the rest of the day is the forecast peak of the remaining hours plus a normal error whose width shrinks
    as the day runs out; a bracket entirely below the floor is impossible; a locked day is certain. None = no model."""
    if locked:
        if observed is None:
            return None
        return Fair(tuple(1.0 if b.contains(observed) else 0.0 for b in brackets), "次日首条观测已出，当日最高已锁定", None, None, observed, True)
    if rest_peak is None:
        if observed is None:
            return None
        mu, sd, note = observed - 1.0, 0.7, "无剩余时段预报：按已观测最高估计"
    else:
        scale = min(1.0, max(0.3, math.sqrt(max(hours_left, 0.0) / 10)))
        mu, sd = rest_peak + bias, max(0.3, sigma * scale)
        note = f"剩余时段预报峰值 {rest_peak:.1f}°C{bias:+.1f}，σ {sd:.2f}"
    if unit == "F":
        mu, sd = c_to_f(mu), sd * 1.8
        note += "（已换算为 °F）"

    def cdf_final(x: int | None) -> float:  # P(final maximum ≤ x), x whole degrees
        if x is None:
            return 1.0
        if observed is not None and x < observed:
            return 0.0
        return norm_cdf((x + 0.5 - mu) / sd)

    probs = []
    for b in brackets:
        p = cdf_final(b.high) - (0.0 if b.low is None else cdf_final(b.low - 1))
        probs.append(min(1.0, max(0.0, p)))
    total = sum(probs)
    if total > 0 and abs(total - 1) > 1e-6:  # ladder gaps / overlaps: show what the brackets cover, scaled
        probs = [p / total for p in probs] if total > 1 else probs
    return Fair(tuple(probs), note, mu, sd, observed, False)


def taker_fee(price: float, bps: int) -> float:
    """Predict's taker fee per $1 share: rate × min(p, 1 − p)."""
    return bps / 10_000 * min(price, 1 - price)


@dataclass(frozen=True)
class Edge:
    level_index: int
    side: str             # YES / NO
    price: float          # what one share costs
    fair: float           # the model's value of that share
    edge: float           # net of the taker fee
    sure: bool            # 0 / 1 by observation, not by forecast

    @property
    def key(self) -> str:
        return f"{self.level_index}:{self.side}"


def level_edges(index: int, fair_yes: float, book: PredictBook, fee_bps: int, sure: bool) -> list[Edge]:
    out = []
    if book.ask is not None:
        ask = float(book.ask)
        out.append(Edge(index, "YES", ask, fair_yes, fair_yes - ask - taker_fee(ask, fee_bps), sure))
    if book.bid is not None:
        bid = float(book.bid)
        out.append(Edge(index, "NO", 1 - bid, 1 - fair_yes, bid - fair_yes - taker_fee(bid, fee_bps), sure))
    return out


# --- Telegram -------------------------------------------------------------------------------------------------------------------
CHAT_GONE = ("bot was blocked by the user", "user is deactivated", "bot was kicked", "chat not found",
             "group chat was deleted", "message thread not found", "topic_closed", "topic_deleted", "not enough rights")
SEND_CHECK: contextvars.ContextVar[Any] = contextvars.ContextVar("send_check", default=None)


def chat_gone(error: BaseException) -> str:
    text = str(error).lower()
    return next((reason for reason in CHAT_GONE if reason in text), "")


class StaleMessage(Exception):
    """A queued message whose data went stale while it waited: dropped, never delivered late."""


class Telegram:
    CHAT_GAP = 1.1
    GLOBAL_GAP = 0.1
    PARALLEL = 4

    def __init__(self, token: str):
        self.root = f"https://api.telegram.org/bot{token}/"
        self.lock = asyncio.Lock()
        self.sends = asyncio.Semaphore(self.PARALLEL)
        self.next_send = 0.0
        self.chat_next: dict[Any, float] = {}
        self.chat_locks: dict[Any, asyncio.Lock] = {}

    async def call(self, method: str, payload: dict | None = None, timeout: int = 15) -> Any:
        data = await http_json(self.root + method, payload or {}, timeout)
        if not isinstance(data, dict) or not data.get("ok"):
            msg = data.get("description", "未知错误") if isinstance(data, dict) else "响应格式异常"
            retry = data.get("parameters", {}).get("retry_after", 0) if isinstance(data, dict) else 0
            raise RemoteError(f"Telegram: {clean_error(msg)}", int(retry))
        return data.get("result")

    async def paced(self, method: str, payload: dict) -> Any:
        """One message per second per chat, a small gap between any two; a 429 holds only its chat."""
        chat = payload.get("chat_id")
        if len(self.chat_locks) > 2000:
            self.chat_locks = {c: lock for c, lock in self.chat_locks.items() if lock.locked()}
            self.chat_next = {c: t for c, t in self.chat_next.items() if t > time.monotonic()}
        async with self.chat_locks.setdefault(chat, asyncio.Lock()):
            await asyncio.sleep(max(0, self.chat_next.get(chat, 0) - time.monotonic()))
            async with self.lock:
                slot = max(self.next_send, time.monotonic())
                self.next_send = slot + self.GLOBAL_GAP
            await asyncio.sleep(max(0, slot - time.monotonic()))
            async with self.sends:
                check = SEND_CHECK.get()
                if check is not None and not check():
                    raise StaleMessage("排队等待发送期间数据已失效，未发送")
                try:
                    return await self.call(method, payload)
                except RemoteError as error:
                    self.chat_next[chat] = time.monotonic() + max(self.CHAT_GAP, error.retry_after)
                    raise
                finally:
                    self.chat_next[chat] = max(self.chat_next.get(chat, 0), time.monotonic() + self.CHAT_GAP)

    async def send(self, chat: int, thread: int, text: str) -> None:
        for chunk in split_text(text):
            payload: dict[str, Any] = {"chat_id": chat, "text": chunk, "link_preview_options": {"is_disabled": True}}
            if thread:
                payload["message_thread_id"] = thread
            await self.paced("sendMessage", payload)
            SEND_CHECK.set(None)


def split_text(text: str, limit: int = 3600) -> list[str]:
    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        cut = limit
        block = remaining.rfind("\n\n", 0, cut + 1)
        line = remaining.rfind("\n", 0, cut + 1)
        cut = block if block >= cut * 0.6 else line if line > 0 else cut
        chunks.append(remaining[:max(cut, 1)])
        remaining = remaining[max(cut, 1):].lstrip("\n")
    return chunks or [""]


def target_from_message(message: dict) -> tuple[int, int]:
    return int(message["chat"]["id"]), int(message.get("message_thread_id") or 0)


def subscription_key(chat: int, thread: int) -> str:
    return f"{chat}:{thread}"


def is_admin(message: dict, config: Config) -> bool:
    sender = message.get("from") or {}
    return bool(config.admin_id and not sender.get("is_bot") and sender.get("id") == config.admin_id)


@dataclass(frozen=True)
class Command:
    name: str
    description: str
    example: str = ""

    @property
    def usage(self) -> str:
        return f"/{self.name} {self.example}".rstrip()

    def menu_entry(self) -> dict[str, str]:
        text = f"{self.description}｜用法 {self.usage}" if self.example else self.description
        return {"command": self.name, "description": text[:256]}


COMMANDS: tuple[Command, ...] = (
    Command("subscribe", "在当前私聊/群组话题订阅提醒"),
    Command("unsubscribe", "取消当前订阅"),
    Command("status", "各市场：观测最高、档位盘口、模型概率"),
    Command("obs", "当日逐条观测读数", "shanghai 10-13"),
    Command("book", "各档位订单簿与模型优势", "shanghai 10-13"),
    Command("markets", "当前跟踪的市场与链接"),
    Command("forecast", "逐小时预报与模型输入", "shanghai 10-13"),
    Command("edge", "优势提醒门槛（¢），off 关闭", "10"),
    Command("pause", "暂停当前订阅"),
    Command("resume", "恢复当前订阅"),
    Command("diag", "逐个检测数据源"),
    Command("test", "发送测试消息"),
    Command("id", "查看用户 ID、聊天 ID、话题 ID"),
    Command("help", "显示说明"),
)
COMMAND_ALIASES = {"/start": "/help"}


@dataclass(frozen=True)
class Request:
    command: str
    args: list[str]
    chat: int
    thread: int
    user_id: int

    @property
    def sub_id(self) -> str:
        return subscription_key(self.chat, self.thread)


@dataclass(frozen=True)
class Target:
    """One followed market: a station's local day and the Predict slug that settles on it."""
    slug: str
    city: str
    station: StationSpec
    day: dt.date

    @property
    def key(self) -> str:
        return self.slug

    @property
    def title(self) -> str:
        return f"{CITY_NAMES.get(self.city, self.city.replace('-', ' ').title())} {self.day.strftime('%m-%d')} 最高气温"


# --- the bot ------------------------------------------------------------------------------------------------------------------------
class Bot:
    def __init__(self, config: Config, store: Store, telegram: Telegram | None):
        self.config, self.store, self.telegram = config, store, telegram
        self.clock = time.time  # seconds; tests replace it
        self.weather = WeatherFeed(config, store)
        self.predict = PredictFeed(config)
        self.forecast = Forecast(config)
        self.deliveries: dict[str, asyncio.Task] = {}
        self.faults: dict[tuple[str, str], float] = {}
        self.edge_seen: dict[str, float] = {}        # edge key (slug:level:side) -> when it was first seen (self.clock())
        self.reply_seq = 0
        self.last_send_error = ""
        self.stopping = asyncio.Event()
        self.started = self.clock()
        self.last_cycle = 0.0
        self.last_log: dict[str, float] = {}
        self.username = ""
        self.loops_running = False
        self.target_errors: dict[str, str] = {}      # slug -> why the market cannot be followed (unknown station...)
        self.pending_sites: set[str] = set()         # stations named by rules whose time zone Synoptic has yet to tell
        self.pruned = 0.0
        self.handlers = {
            "/help": self.cmd_help, "/id": self.cmd_id, "/status": self.cmd_status, "/test": self.cmd_test,
            "/subscribe": self.cmd_subscribe, "/resume": self.cmd_resume, "/pause": self.cmd_pause,
            "/unsubscribe": self.cmd_unsubscribe, "/obs": self.cmd_obs, "/book": self.cmd_book,
            "/markets": self.cmd_markets, "/forecast": self.cmd_forecast, "/edge": self.cmd_edge, "/diag": self.cmd_diag,
        }

    # -- settings / subscriptions ------------------------------------------------------------------------------------
    def now_ms(self) -> int:
        return int(self.clock() * 1000)

    def settings(self) -> dict:
        return {"edge_cents": self.config.edge_cents, "edge_alert": self.config.edge_alert, **self.store.get("settings", {})}

    def subscriptions(self) -> dict:
        return self.store.get("subscriptions", {})

    def set_subscription(self, chat: int, thread: int, active: bool) -> None:
        subs = self.subscriptions()
        subs[subscription_key(chat, thread)] = {"chat": chat, "thread": thread, "active": active}
        self.store.put("subscriptions", subs)

    def active_subscriptions(self) -> list[dict]:
        return [s for s in self.subscriptions().values() if s.get("active")]

    async def wait(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.stopping.wait(), timeout=seconds)

    def log_limited(self, key: str, text: str, every: int = 60) -> None:
        now = time.monotonic()
        if now - self.last_log.get(key, -1e9) >= every:
            LOG.warning("%s", clean_error(text))
            self.last_log[key] = now

    # -- followed markets ----------------------------------------------------------------------------------------------
    def station_for_slug(self, slug: str) -> tuple[str, StationSpec, dt.date] | None:
        parsed = parse_slug(slug, self.config.slug_pattern)
        if not parsed:
            self.target_errors[slug] = "slug 不是「<城市> <日期> 最高气温」的格式，无法知道它按哪个站点、哪一天结算"
            return None
        city, day = parsed
        station = self.config.stations.get(city)
        ladder = self.predict.ladders.get(slug)
        site = rules_site(ladder.rules) if ladder else ""
        if station is None:
            if not site:
                self.target_errors[slug] = f"城市 {city} 没有站点配置（STATIONS），且市场规则里还读不到 site=；先等待 Predict 规则"
                return None
            meta = self.weather.meta.get(site) or {}
            tz = str(meta.get("TIMEZONE") or "")
            if not tz:
                self.target_errors[slug] = f"城市 {city} 没有站点配置；规则指定站点 {site}，正在等待 Synoptic 站点时区信息"
                self.pending_sites.add(site)
                station = StationSpec(city, site, "", "", None, None)
            else:
                self.pending_sites.discard(site)
                station = StationSpec(city, site, tz, rules_wu_id(ladder.rules).split(":")[-1] if ladder else "",
                                      number_or_none_wide(meta.get("LATITUDE")), number_or_none_wide(meta.get("LONGITUDE")),
                                      str(meta.get("NAME") or ""))
                self.target_errors.pop(slug, None)
        else:
            if site and site != station.stid:
                self.target_errors[slug] = f"⚠️ 市场规则指定站点 {site}，而配置里 {city} 是 {station.stid}：请核对 STATIONS"
            else:
                self.target_errors.pop(slug, None)
        if not station.tz:
            return None
        return city, station, day

    def targets(self, now_ms: int) -> list[Target]:
        """Today and the next FOLLOW_DAYS days (each station's local date) for every configured city, a finished day
        while KEEP_HOURS after its midnight, and the extra MARKET_SLUGS."""
        out: dict[str, Target] = {}
        for city in self.config.cities:
            station = self.config.stations[city]
            tz = zone(station.tz)
            today = local_date(now_ms, tz)
            days = [today + dt.timedelta(days=i) for i in range(self.config.follow_days + 1)]
            yesterday = today - dt.timedelta(days=1)
            if now_ms < day_bounds(yesterday, tz)[1] + self.config.keep_hours * 3_600_000:
                days.insert(0, yesterday)
            for day in days:
                slug = make_slug(city, day, self.config.slug_pattern)
                out[slug] = Target(slug, city, station, day)
        for slug in self.config.slugs:
            if slug in out:
                continue
            found = self.station_for_slug(slug)
            if found:
                city, station, day = found
                out[slug] = Target(slug, city, station, day)
        return list(out.values())

    def day_state(self, slug: str) -> dict:
        return self.store.get(f"day:{slug}", {})

    def save_day_state(self, slug: str, state: dict) -> None:
        self.store.put(f"day:{slug}", state)

    # -- refresh cycles ----------------------------------------------------------------------------------------------
    async def weather_cycle(self, now_ms: int, targets: list[Target], force: bool = False) -> None:
        by_station: dict[str, tuple[StationSpec, list[dt.date]]] = {}
        for t in targets:
            by_station.setdefault(t.station.stid, (t.station, []))[1].append(t.day)
        for stid, (station, days) in by_station.items():
            try:
                await self.weather.refresh(station, sorted(set(days)), now_ms, force)
            except Exception as error:
                self.log_limited(f"weather:{stid}", f"{stid} 观测刷新异常：{clean_error(error)}")
        for site in list(self.pending_sites):
            try:
                await self.weather.probe(site, now_ms)
                self.pending_sites.discard(site)
            except Exception as error:
                self.log_limited(f"probe:{site}", f"{site} 站点信息获取失败：{clean_error(error)}")
        if self.clock() - self.pruned > 3600:
            self.pruned = self.clock()
            self.weather.prune(now_ms)
            self.prune_records(now_ms)

    def prune_records(self, now_ms: int) -> None:
        """Delivery records, edge cool-downs and day states older than OBS_KEEP_DAYS."""
        limit = now_ms / 1000 - OBS_KEEP_DAYS * 86400
        stale = [k for k, v in self.store.items("sent:") if isinstance(v, (int, float)) and v < limit]
        stale += [k for k, v in self.store.items("edge:") if isinstance(v, (int, float)) and v < limit]
        stale += [k for k, v in self.store.items("day:") if isinstance(v, dict) and 0 < int(v.get("first_seen") or 0) / 1000 < limit]
        self.store.delete_keys(stale)

    async def forecast_cycle(self, targets: list[Target], force: bool = False) -> None:
        seen = set()
        for t in targets:
            if t.station.stid in seen:
                continue
            seen.add(t.station.stid)
            lat, lon = t.station.lat, t.station.lon
            meta = self.weather.meta.get(t.station.stid) or {}
            if lat is None or lon is None:
                lat, lon = number_or_none_wide(meta.get("LATITUDE")), number_or_none_wide(meta.get("LONGITUDE"))
            await self.forecast.refresh(t.station, lat, lon, force)

    async def predict_cycle(self, now_ms: int, targets: list[Target], force: bool = False) -> bool:
        if not self.config.predict:
            return False
        slugs = [t.slug for t in targets] + [s for s in self.config.slugs if s not in {t.slug for t in targets}]
        return await self.predict.refresh(slugs, now_ms, force)

    # -- model -----------------------------------------------------------------------------------------------------------
    def model(self, t: Target, now_ms: int) -> tuple[Fair | None, DayData | None, Ladder | None]:
        ladder = self.predict.ladders.get(t.slug)
        data = self.weather.official(t.station.stid, t.day)
        if not ladder or not ladder.levels:
            return None, data, ladder
        unit = ladder.unit
        tz = zone(t.station.tz)
        peak = data.peak(unit) if data else None
        observed = peak[0] if peak else None
        locked = bool(data and data.locked)
        after_ms = max(now_ms, data.latest.ts_ms if data and data.latest else 0)
        rest = self.forecast.peak(t.station.stid, t.day, tz, after_ms) if self.config.forecast else None
        hours_left = max(0.0, (day_bounds(t.day, tz)[1] - now_ms) / 3_600_000)
        fair = fair_probabilities([lv.bracket for lv in ladder.levels], observed, locked, rest[0] if rest else None,
                                  self.config.forecast_sigma, self.config.forecast_bias, hours_left, unit)
        if fair is None and observed is None and rest is None and day_bounds(t.day, tz)[0] > now_ms:
            whole = self.forecast.peak(t.station.stid, t.day, tz)  # a day not begun: the whole day's forecast peak
            if whole:
                fair = fair_probabilities([lv.bracket for lv in ladder.levels], None, False, whole[0], self.config.forecast_sigma,
                                          self.config.forecast_bias, 24.0 + hours_left, unit)
        return fair, data, ladder

    def edges(self, t: Target, now_ms: int) -> list[tuple[Edge, Level]]:
        fair, data, ladder = self.model(t, now_ms)
        if not ladder or not ladder.levels:
            return []
        out = []
        for index, level in enumerate(ladder.levels):
            book, _ = level.yes_book()
            if book is None or book.stale(now_ms) or level.closed:
                continue
            resolved = level.resolved_yes()
            if resolved is not None:
                p, sure = (1.0 if resolved else 0.0), True
            elif fair is not None:
                p = fair.probs[index]
                sure = fair.locked or (fair.observed is not None and level.bracket.high is not None and level.bracket.high < fair.observed)
            else:
                continue
            fee = level.meta.get("fee_bps")
            out.extend((e, level) for e in level_edges(index, p, book, fee if isinstance(fee, int) else self.config.predict_fee_bps, sure))
        return out

    # -- alerts ----------------------------------------------------------------------------------------------------------
    async def plan_alerts(self, now_ms: int, targets: list[Target]) -> None:
        subs = self.active_subscriptions()
        for t in targets:
            try:
                self.plan_target(t, now_ms, subs)
            except Exception as error:
                self.log_limited(f"plan:{t.slug}", f"{t.slug} 提醒规划异常：{clean_error(error)}")

    def plan_target(self, t: Target, now_ms: int, subs: list[dict]) -> None:
        state = self.day_state(t.slug)
        changed = False
        tz = zone(t.station.tz)
        ladder = self.predict.ladders.get(t.slug)
        unit = ladder.unit if ladder and ladder.levels else "C"
        data = self.weather.official(t.station.stid, t.day)
        peak = data.peak(unit) if data else None
        key = f"{t.title}"
        # new market listed
        if ladder and ladder.levels and not state.get("listed_announced") and self.config.new_market_alert:
            state["listed_announced"] = now_ms
            changed = True
            if state.get("first_seen"):  # not on the very first look after a restart with an old day
                self.broadcast(subs, f"new:{t.slug}", self.new_market_text(t, ladder, now_ms))
        if not state.get("first_seen"):
            state["first_seen"] = now_ms
            changed = True
        # running maximum: announced when it moves the day into another bracket (every new reading of a rising morning
        # would be noise: 22, 23 and 24 all sit in "24°C or below"); without a ladder every new maximum is announced
        if peak and (state.get("max") is None or peak[0] > int(state["max"])) and not data.locked:
            previous = state.get("max")
            level = ladder.level_for(peak[0]) if ladder and ladder.levels else None
            label = level.bracket.label if level else None
            state["max"], state["max_at"] = peak[0], peak[1].ts_ms
            changed = True
            if self.config.max_alert and (label is None or label != state.get("bracket")):
                self.broadcast(subs, f"max:{t.slug}:{peak[0]}", self.max_text(t, data, peak, previous, now_ms))
            if label is not None:
                state["bracket"] = label
        elif peak and state.get("max") is not None and peak[0] < int(state["max"]) and not state.get("drop_noted") == peak[0]:
            state["drop_noted"] = peak[0]
            changed = True
            self.broadcast(subs, f"drop:{t.slug}:{peak[0]}",
                           f"⚠️ {t.title}｜观测最高下调\n之前记录的最高 {state['max']}°{unit} 现在只剩 {peak[0]}°{unit}："
                           f"数据源修正或删除了读数。结算以来源页最终数据为准。\n{self.links(t)}")
        # lock: the following day's first observation
        if data and data.locked and not state.get("lock_announced"):
            state["lock_announced"] = now_ms
            state["max"] = peak[0] if peak else state.get("max")
            changed = True
            self.broadcast(subs, f"lock:{t.slug}", self.lock_text(t, data, ladder, now_ms))
        # revisions
        seen = state.setdefault("revisions", [])
        for source, stid, day, note in self.weather.revisions:
            if stid != t.station.stid or day != t.day or note in seen:
                continue
            seen.append(note)
            changed = True
            self.broadcast(subs, f"rev:{t.slug}:{len(seen)}", f"✏️ {t.title}｜{SOURCE_NAMES[source]} 读数被修正\n{note}\n"
                                                             f"市场规则：次日首条数据出现前的修正有效。\n{self.links(t)}")
        # sources disagree about the peak
        peaks = {}
        for source in self.config.sources:
            d = self.weather.get(source, t.station.stid, t.day)
            p = d.peak(unit) if d and d.obs else None
            if p:
                peaks[source] = p[0]
        if len(set(peaks.values())) > 1:
            mark = ",".join(f"{s}={v}" for s, v in sorted(peaks.items()))
            if state.get("disagree") != mark:
                state["disagree"] = mark
                changed = True
                self.broadcast(subs, f"dis:{t.slug}:{mark}", f"⚠️ {t.title}｜各数据源的当日最高不一致\n" +
                               "\n".join(f"{SOURCE_NAMES[s]}：{v}°{unit}" for s, v in peaks.items()) +
                               f"\n结算以 NOAA 页面为准（无数据时才用 Weather Underground）。\n{self.links(t)}")
        # settlement on Predict
        if ladder and ladder.any_settled and not state.get("settle_announced"):
            state["settle_announced"] = now_ms
            changed = True
            self.broadcast(subs, f"settle:{t.slug}", self.settle_text(t, ladder, data, now_ms))
        # faults: no source answers / observations stop during the day
        if subs:
            fault = self.target_fault(t, data, now_ms)
            for sub in subs:
                self.notice(sub, key, fault)
        # edges
        self.edge_alerts(t, now_ms, subs)
        if changed:
            self.save_day_state(t.slug, state)

    def target_fault(self, t: Target, data: DayData | None, now_ms: int) -> str | None:
        stid = t.station.stid
        errors = [self.weather.errors.get((s, stid), "") for s in self.config.sources]
        if all(errors) and errors:
            return "所有观测来源都失败：" + "；".join(f"{SOURCE_NAMES[s]} {brief_error(e, 50)}" for s, e in zip(self.config.sources, errors))
        tz = zone(t.station.tz)
        start, end = day_bounds(t.day, tz)
        if start <= now_ms < end and data and data.latest and not data.locked:
            age_min = (now_ms - data.latest.ts_ms) / 60_000
            if age_min > self.config.obs_stale_minutes and now_ms - start > self.config.obs_stale_minutes * 60_000:
                return f"观测停更：最新读数 {stamp(data.latest.ts_ms, tz, date=False)}，已 {int(age_min)} 分钟没有新数据"
        return None

    def edge_alerts(self, t: Target, now_ms: int, subs: list[dict]) -> None:
        settings = self.settings()
        if not settings.get("edge_alert") or not subs:
            return
        threshold = float(settings.get("edge_cents", self.config.edge_cents))
        now = self.clock()
        live = set()
        for edge, level in self.edges(t, now_ms):
            if edge.edge < threshold:
                continue
            ekey = f"{t.slug}:{edge.key}"
            live.add(ekey)
            first = self.edge_seen.setdefault(ekey, now)
            if now - first < self.config.edge_confirm:
                continue
            last = self.store.get(f"edge:{ekey}", 0)
            if now - float(last or 0) < self.config.edge_cooldown:
                continue
            self.store.put(f"edge:{ekey}", now)
            self.broadcast(subs, f"edge:{ekey}:{int(now)}", self.edge_text(t, edge, level, now_ms))
        for ekey in [k for k in self.edge_seen if k.startswith(t.slug + ":") and k not in live]:
            self.edge_seen.pop(ekey)

    # -- texts -------------------------------------------------------------------------------------------------------------
    def links(self, t: Target) -> str:
        lines = [f"市场：{predict_url(t.slug, self.config.predict_ref)}", f"NOAA：{NOAA_PAGE.format(site=t.station.stid.lower())}"]
        if t.station.country:
            lines.append(f"WU：{WU_PAGE.format(cc=t.station.country.lower(), city=t.city, stid=t.station.stid, date=t.day.isoformat())}")
        return "\n".join(lines)

    def ladder_lines(self, t: Target, ladder: Ladder | None, fair: Fair | None, observed: int | None, now_ms: int, depth: int = 0) -> list[str]:
        if not ladder:
            return ["Predict：等待首次获取"]
        if not ladder.listed:
            return [f"Predict：还没有这个市场（{ladder.slug}）"]
        if not ladder.levels:
            return [f"Predict：{ladder.error or '无档位'}"]
        lines = []
        for index, lv in enumerate(ladder.levels):
            book, why = lv.yes_book()
            resolved = lv.resolved_yes()
            if resolved is not None:
                quote = "✅ 已结算：Yes" if resolved else "已结算：No"
            elif lv.settled:
                quote = "已结算"
            elif book is None:
                quote = brief_error(why, 30)
            else:
                quote = book.text() + ("（旧）" if book.stale(now_ms) else "")
            prob = f"｜模型 {pct(fair.probs[index])}" if fair else ""
            mark = " ← 当前最高" if observed is not None and lv.bracket.contains(observed) else ""
            lines.append(f"{lv.bracket.label:>8}  {quote}{prob}{mark}")
            if depth and book is not None:
                bids = " ".join(f"{float(p) * 100:.0f}¢×{float(q):.0f}" for p, q in book.bids[:depth]) or "无"
                asks = " ".join(f"{float(p) * 100:.0f}¢×{float(q):.0f}" for p, q in book.asks[:depth]) or "无"
                lines.append(f"          买 {bids}\n          卖 {asks}")
        if ladder.error:
            lines.append(f"（刷新失败：{brief_error(ladder.error, 60)}，显示上次盘口）")
        return lines

    def card(self, t: Target, now_ms: int, depth: int = 0) -> str:
        fair, data, ladder = self.model(t, now_ms)
        unit = ladder.unit if ladder and ladder.levels else "C"
        tz = zone(t.station.tz)
        peak = data.peak(unit) if data else None
        lines = [f"🌡 {t.title}（{t.station.label} {t.station.stid}，{zone_name(tz)} 本地日）"]
        if error := self.target_errors.get(t.slug):
            lines.append(error)
        if data and peak:
            lines.append(f"观测最高：{peak[0]}°{unit}（{stamp(peak[1].ts_ms, tz, date=False)}，{SOURCE_NAMES[data.source]}）"
                         f"｜最新 {deg(data.latest.temp_in(unit))} {stamp(data.latest.ts_ms, tz, date=False)}｜{len(data.obs)} 条")
        elif data and data.obs:
            lines.append(f"当日 {len(data.obs)} 条观测，暂无温度读数")
        else:
            lines.append("当日暂无观测数据")
        if data and data.locked:
            lines.append(f"🔒 已锁定：次日首条观测 {stamp(data.after.ts_ms, tz)}（{deg(data.after.temp_in(unit))}）已发布")
        lines.extend(self.weather.source_line(t.station, t.day, unit))
        if self.config.forecast:
            whole = self.forecast.peak(t.station.stid, t.day, tz)
            if whole:
                rest = self.forecast.peak(t.station.stid, t.day, tz, now_ms)
                text = f"预报：当日峰值 {whole[0]:.1f}°C（{stamp(whole[1], tz, date=False)}）"
                if rest and not (data and data.locked):
                    text += f"｜剩余时段峰值 {rest[0]:.1f}°C（{stamp(rest[1], tz, date=False)}）"
                lines.append(text)
            elif err := self.forecast.errors.get(t.station.stid):
                lines.append(f"预报：❌ {brief_error(err, 60)}")
        if fair:
            lines.append(f"模型：{fair.note}")
        lines.append("档位（Yes 买1/卖1）：")
        lines.extend(self.ladder_lines(t, ladder, fair, peak[0] if peak else None, now_ms, depth))
        edges = sorted(self.edges(t, now_ms), key=lambda e: -e[0].edge)
        good = [e for e in edges if e[0].edge >= float(self.settings().get("edge_cents", self.config.edge_cents))]
        if good:
            lines.append("优势（净，吃单）：")
            for edge, level in good[:4]:
                lines.append(f"  {level.bracket.label} {edge.side} @{cents(edge.price)}｜模型 {cents(edge.fair)}｜{cents(edge.edge, True)}"
                             + ("｜观测已定" if edge.sure else ""))
        lines.append(self.links(t))
        return "\n".join(lines)

    def max_text(self, t: Target, data: DayData, peak: tuple[int, Obs], previous: Any, now_ms: int) -> str:
        fair, _, ladder = self.model(t, now_ms)
        unit = ladder.unit if ladder and ladder.levels else "C"
        tz = zone(t.station.tz)
        head = f"🌡 {t.title}｜新高 {peak[0]}°{unit}" + (f"（此前 {previous}°）" if previous is not None else "（当日首个最高）")
        lines = [head, f"观测 {stamp(peak[1].ts_ms, tz, date=False)}（{SOURCE_NAMES[data.source]}{', ' + peak[1].kind if peak[1].kind else ''}）"
                 f"｜最新 {deg(data.latest.temp_in(unit))} {stamp(data.latest.ts_ms, tz, date=False)}"]
        if ladder and ladder.levels:
            level = ladder.level_for(peak[0])
            if level:
                book, _ = level.yes_book()
                lines.append(f"当前档位 {level.bracket.label}：{book.text() if book else '无盘口'}"
                             + (f"｜模型 {pct(fair.probs[ladder.levels.index(level)])}" if fair else ""))
            else:
                lines.append(f"⚠️ 没有档位包含 {peak[0]}°{unit}")
            below = [lv for lv in ladder.levels if lv.bracket.high is not None and lv.bracket.high < peak[0]]
            if below:
                lines.append(f"已不可能：{'、'.join(lv.bracket.label for lv in below)}")
        if self.config.forecast:
            rest = self.forecast.peak(t.station.stid, t.day, tz, max(now_ms, peak[1].ts_ms))
            if rest:
                lines.append(f"剩余时段预报峰值 {rest[0]:.1f}°C（{stamp(rest[1], tz, date=False)}）")
        lines.append(self.links(t))
        return "\n".join(lines)

    def lock_text(self, t: Target, data: DayData, ladder: Ladder | None, now_ms: int) -> str:
        unit = ladder.unit if ladder and ladder.levels else "C"
        tz = zone(t.station.tz)
        peak = data.peak(unit)
        lines = [f"🔒 {t.title}｜次日首条观测已发布，当日最高锁定",
                 f"次日首条：{stamp(data.after.ts_ms, tz)}（{deg(data.after.temp_in(unit))}，{SOURCE_NAMES[data.source]}）"]
        if peak:
            lines.append(f"当日最高：{peak[0]}°{unit}（{stamp(peak[1].ts_ms, tz, date=False)}）｜共 {len(data.obs)} 条")
            if ladder and ladder.levels:
                level = ladder.level_for(peak[0])
                if level:
                    book, _ = level.yes_book()
                    lines.append(f"应结算档位：{level.bracket.label}（Yes {book.text() if book else '无盘口'}）")
                else:
                    lines.append(f"⚠️ 没有档位包含 {peak[0]}°{unit}")
        else:
            lines.append("⚠️ 当日没有温度读数：按规则将结算到最低档位")
        lines.append("注意：按规则，次日首条数据出现后不再接受修正；以 Predict 实际结算为准。")
        lines.append(self.links(t))
        return "\n".join(lines)

    def settle_text(self, t: Target, ladder: Ladder, data: DayData | None, now_ms: int) -> str:
        unit = ladder.unit
        winner = ladder.winner()
        peak = data.peak(unit) if data else None
        lines = [f"🏁 {t.title}｜Predict 已结算"]
        if winner:
            lines.append(f"结算档位：{winner.bracket.label}")
            if peak:
                ours = ladder.level_for(peak[0])
                lines.append(f"观测最高 {peak[0]}°{unit} → 应为 {ours.bracket.label if ours else '无档位'}"
                             + ("，一致 ✅" if ours is winner else "，不一致 ⚠️ 请核对来源页"))
        else:
            settled = [lv.bracket.label for lv in ladder.levels if lv.settled]
            lines.append(f"已结算档位：{'、'.join(settled)}（未读到赢家）")
        lines.append(self.links(t))
        return "\n".join(lines)

    def new_market_text(self, t: Target, ladder: Ladder, now_ms: int) -> str:
        fair, data, _ = self.model(t, now_ms)
        peak = data.peak(ladder.unit) if data else None
        lines = [f"🆕 {t.title}｜Predict 已上线", "档位（Yes 买1/卖1）："]
        lines.extend(self.ladder_lines(t, ladder, fair, peak[0] if peak else None, now_ms))
        if self.config.forecast:
            whole = self.forecast.peak(t.station.stid, t.day, zone(t.station.tz))
            if whole:
                lines.append(f"预报当日峰值 {whole[0]:.1f}°C（{stamp(whole[1], zone(t.station.tz), date=False)}）")
        lines.append(self.links(t))
        return "\n".join(lines)

    def edge_text(self, t: Target, edge: Edge, level: Level, now_ms: int) -> str:
        why = "观测已锁定该档位的结果" if edge.sure else "模型估计（预报峰值 + 已观测最高），有误差"
        return (f"💡 {t.title}｜{level.bracket.label} {edge.side} 优势 {cents(edge.edge, True)}\n"
                f"吃单价 {cents(edge.price)}｜模型 {cents(edge.fair)}｜{why}\n"
                f"只是提示，不构成建议；请自行核对盘口深度与规则。\n{self.links(t)}")

    # -- delivery ------------------------------------------------------------------------------------------------------------
    async def tell(self, chat: int, thread: int, text: str, fresh: Any = None) -> bool:
        if self.telegram is None:
            print(f"[chat {chat}] {text}\n")
            return True
        token = SEND_CHECK.set(fresh)
        try:
            await self.telegram.send(chat, thread, text)
            return True
        except StaleMessage as error:
            LOG.info("Telegram message dropped: %s", error)
            return False
        except Exception as error:
            gone = chat_gone(error)
            if gone:
                self.suspend_target(chat, thread, gone)
            self.last_send_error = f"{stamp(self.clock() * 1000)} chat {chat}：{brief_error(clean_error(error), 80)}"
            self.log_limited(f"telegram_send:{chat}", f"Telegram 发送失败（chat {chat}）：{clean_error(error)}")
            return False
        finally:
            SEND_CHECK.reset(token)

    def suspend_target(self, chat: int, thread: int, why: str) -> None:
        subs = self.subscriptions()
        sub_id = subscription_key(chat, thread)
        sub = subs.get(sub_id)
        if not sub or not sub.get("active"):
            return
        subs[sub_id] = {**sub, "active": False, "suspended": why, "suspended_at": self.clock()}
        self.store.put("subscriptions", subs)
        LOG.warning("订阅 %s 已自动暂停：Telegram 拒绝投递（%s）", sub_id, why)
        if self.config.admin_id and (chat, thread) != (self.config.admin_id, 0):
            text = (f"⏸ 订阅 {sub_id} 已自动暂停\nTelegram 拒绝向该聊天投递：{why}\n处理后在该聊天发送 /resume 恢复。")
            self.deliver(f"notice:suspended:{sub_id}", functools.partial(self.tell, self.config.admin_id, 0, text))

    def delivering(self, key: str) -> bool:
        task = self.deliveries.get(key)
        return task is not None and not task.done()

    def deliver(self, key: str, job: Any) -> bool:
        """Run one Telegram delivery in the background so sampling never waits on Telegram; one per key at a time."""
        if self.delivering(key) or self.stopping.is_set():
            return False

        async def guarded() -> None:
            try:
                await job()
            except Exception as error:
                self.log_limited("deliver", f"消息投递异常：{clean_error(error)}")
        self.deliveries = {k: t for k, t in self.deliveries.items() if not t.done()}
        self.deliveries[key] = asyncio.create_task(guarded(), name=f"deliver:{key}")
        return True

    def broadcast(self, subs: list[dict], key: str, text: str) -> None:
        """The same alert to every active subscription, each as its own delivery, recorded once Telegram took it."""
        for sub in subs:
            record_key = f"sent:{key}:{sub['chat']}:{sub['thread']}"
            if self.store.get(record_key):
                continue

            async def send(sub: dict = sub, record_key: str = record_key) -> None:
                if await self.tell(sub["chat"], sub["thread"], text):
                    self.store.put(record_key, int(self.clock()))
            self.deliver(record_key, send)

    async def drain_deliveries(self) -> None:
        while pending := [t for t in self.deliveries.values() if not t.done()]:
            await asyncio.gather(*pending, return_exceptions=True)
        self.deliveries.clear()

    def notice(self, sub: dict, key: str, error: str | None) -> None:
        """A fault / recovery notice: announced once the fault lasted NOTICE_GRACE_SECONDS, then at most every
        NOTICE_REPEAT_SECONDS; the recovery note follows only a fault that was announced."""
        sub_id = subscription_key(sub["chat"], sub["thread"])
        record_key = f"notice:{sub_id}:{key}"
        if self.delivering(record_key):
            return
        now = self.clock()
        old = self.store.get(record_key, {})
        if error:
            since = self.faults.setdefault((sub_id, key), now)
            if now - since < NOTICE_GRACE_SECONDS:
                return
            if old.get("active") and now - old.get("sent", 0) < NOTICE_REPEAT_SECONDS:
                return
            lasting = f"已持续 {int((now - since) // 60)} 分钟｜" if now - since >= 60 else ""
            text = f"⚠️ 观测监控异常｜{key}\n{lasting}{error}\n恢复后继续。这不代表气温没有变化。"
            state = {"active": True, "sent": now}
        else:
            self.faults.pop((sub_id, key), None)
            if not old.get("active"):
                return
            text, state = f"✅ 数据恢复｜{key}", {"active": False, "sent": now}

        async def send() -> None:
            if await self.tell(sub["chat"], sub["thread"], text):
                self.store.put(record_key, state)
        self.deliver(record_key, send)

    # -- commands --------------------------------------------------------------------------------------------------------------
    def parse_request(self, message: dict) -> Request | None:
        text = str(message.get("text", "")).strip()
        if not text.startswith("/"):
            return None
        parts = text.split()
        raw_command, _, mention = parts[0].lower().partition("@")
        if mention and self.username and mention != self.username.lower():
            return None
        chat, thread = target_from_message(message)
        user_id = int((message.get("from") or {}).get("id") or 0)
        return Request(COMMAND_ALIASES.get(raw_command, raw_command), parts[1:], chat, thread, user_id)

    async def process_message(self, message: dict) -> None:
        req = self.parse_request(message)
        if req is None:
            return
        if not is_admin(message, self.config):
            if req.command in {"/id", "/help"}:
                await self.reply(req, id_text(req.user_id, req.chat, req.thread) +
                                 ("\n\n管理员尚未配置：请把上面的用户 ID 填到 ADMIN_USER_ID。" if not self.config.admin_id else
                                  "\n\n只有管理员可以使用其他命令。"))
            return
        try:
            handler = self.handlers.get(req.command)
            if handler is None:
                if req.chat < 0 and "@" not in str(message.get("text", "")).split()[0]:
                    return
                reply: Any = "未知命令。发送 /help 查看用法。"
            else:
                reply = handler(req)
            if inspect.isawaitable(reply):
                reply = await reply
        except ValueError as error:
            reply = "❌ " + clean_error(error)
        except Exception as error:
            LOG.exception("命令 %s 处理失败", req.command)
            reply = f"❌ 命令执行失败：{clean_error(error) or type(error).__name__}"
        await self.reply(req, str(reply))

    async def reply(self, req: Request, text: str) -> None:
        if not self.loops_running:
            await self.tell(req.chat, req.thread, text)
            return
        self.reply_seq += 1
        self.deliver(f"reply:{self.reply_seq}", functools.partial(self.tell, req.chat, req.thread, text))

    def pick_target(self, args: list[str]) -> Target:
        """/obs [city] [date]: the followed market named (city as in the slug or its Chinese name, date as MM-DD /
        YYYY-MM-DD / today / tomorrow / yesterday); without arguments the first followed market."""
        now_ms = self.now_ms()
        targets = self.targets(now_ms)
        by_name = {v: k for k, v in CITY_NAMES.items()}
        city, day_text = "", ""
        for arg in args:
            value = arg.strip().lower()
            if arg in by_name:
                city = by_name[arg]
            elif value in self.config.stations:
                city = value
            elif re.fullmatch(r"[a-z][a-z-]*", value) and value not in {"today", "tomorrow", "yesterday"}:
                raise ValueError(f"未知城市 {value}；可用：{'、'.join(self.config.stations)}")
            elif not day_text:
                day_text = value
            else:
                raise ValueError("用法：/obs shanghai 10-13（城市和日期都可省略）")
        if not city:
            city = targets[0].city if targets else (self.config.cities[0] if self.config.cities else "")
        station = self.config.stations.get(city) or next((t.station for t in targets if t.city == city), None)
        if station is None:
            raise ValueError(f"未知城市 {city}；可用：{'、'.join(self.config.stations)}")
        day = parse_day(day_text, local_date(now_ms, zone(station.tz))) if day_text else None
        found = [t for t in targets if t.city == city and (day is None or t.day == day)]
        if found:
            return found[0]
        if day is None:
            raise ValueError(f"当前没有跟踪 {city} 的市场")
        return Target(make_slug(city, day, self.config.slug_pattern), city, station, day)  # a date outside the window: ad hoc

    def cmd_help(self, req: Request) -> str:
        lines = [f"🌡 Predict 天气市场监控 v{VERSION}", "监控「最高气温」市场：NOAA 观测 × Predict 盘口。只读，不下单。", "",
                 "命令："] + [f"{c.usage}  {c.description}" for c in COMMANDS]
        lines += ["", "提醒：当日新高/档位、次日首条观测（锁定）、读数修正、来源不一致、数据异常、模型优势、市场上线/结算。",
                  "规则要点：按 NOAA 页面当日（站点本地日期）Temp 列最高读数（整数 °C）结算；次日首条数据发布后不再接受修正。"]
        return "\n".join(lines)

    def cmd_id(self, req: Request) -> str:
        return id_text(req.user_id, req.chat, req.thread)

    def cmd_test(self, req: Request) -> str:
        return f"✅ 测试消息｜{stamp(self.clock() * 1000, seconds=True)}（北京时间）\n能收到就说明推送通道正常。"

    def cmd_subscribe(self, req: Request) -> str:
        self.set_subscription(req.chat, req.thread, True)
        return f"✅ 已订阅（{req.sub_id}）。发 /status 看当前状态，/unsubscribe 取消。"

    def cmd_unsubscribe(self, req: Request) -> str:
        subs = self.subscriptions()
        if req.sub_id in subs:
            subs.pop(req.sub_id)
            self.store.put("subscriptions", subs)
            return "已取消本聊天的订阅。"
        return "本聊天没有订阅。"

    def cmd_pause(self, req: Request) -> str:
        if req.sub_id not in self.subscriptions():
            return "本聊天没有订阅；先发 /subscribe。"
        self.set_subscription(req.chat, req.thread, False)
        return "⏸ 已暂停本聊天的提醒；/resume 恢复。"

    def cmd_resume(self, req: Request) -> str:
        self.set_subscription(req.chat, req.thread, True)
        return "▶️ 已恢复本聊天的提醒。"

    def cmd_edge(self, req: Request) -> str:
        if not req.args:
            s = self.settings()
            return f"优势提醒：{'开' if s.get('edge_alert') else '关'}｜门槛 {float(s.get('edge_cents', 0)) * 100:.0f}¢（净，吃单）\n/edge 8 改门槛；/edge off 关闭；/edge on 打开"
        value = req.args[0].lower()
        settings = self.settings()
        if value in {"off", "关", "关闭"}:
            settings["edge_alert"] = False
        elif value in {"on", "开", "打开"}:
            settings["edge_alert"] = True
        else:
            try:
                c = float(value.rstrip("¢c"))
            except ValueError:
                raise ValueError("用法：/edge 10（¢）或 /edge off") from None
            if not 1 <= c <= 50:
                raise ValueError("门槛应在 1～50¢ 之间")
            settings["edge_cents"], settings["edge_alert"] = c / 100, True
        self.store.put("settings", settings)
        return self.cmd_edge(Request("/edge", [], req.chat, req.thread, req.user_id))

    def cmd_status(self, req: Request) -> str:
        now_ms = self.now_ms()
        targets = self.targets(now_ms)
        if not targets:
            return "当前没有跟踪的市场。"
        cards = [self.card(t, now_ms) for t in targets]
        head = [f"📊 状态 v{VERSION}｜{stamp(now_ms, seconds=True)}（北京时间）｜订阅 {len(self.active_subscriptions())}",
                f"观测每 {self.config.poll} 秒，盘口每 {self.config.predict_poll} 秒，预报每 {self.config.forecast_minutes} 分钟"]
        if self.last_send_error:
            head.append(f"最近发送失败：{self.last_send_error}")
        for slug, error in self.target_errors.items():
            if slug not in {t.slug for t in targets}:
                head.append(f"{slug}：{error}")
        return "\n".join(head) + "\n\n" + "\n\n".join(cards)

    def cmd_obs(self, req: Request) -> str:
        t = self.pick_target(req.args)
        now_ms = self.now_ms()
        ladder = self.predict.ladders.get(t.slug)
        unit = ladder.unit if ladder and ladder.levels else "C"
        tz = zone(t.station.tz)
        lines = [f"📋 {t.title}｜{t.station.stid}（{zone_name(tz)}）"]
        for source in self.config.sources:
            data = self.weather.get(source, t.station.stid, t.day)
            if not data or not data.obs:
                lines.append(f"\n{SOURCE_NAMES[source]}：无数据" + (f"（{brief_error(self.weather.errors.get((source, t.station.stid), ''), 60)}）"
                                                               if self.weather.errors.get((source, t.station.stid)) else ""))
                continue
            peak = data.peak(unit)
            lines.append(f"\n{SOURCE_NAMES[source]}：{len(data.obs)} 条" + (f"，最高 {peak[0]}°{unit}" if peak else ""))
            rows = data.obs[-48:]
            for o in rows:
                value = o.temp_in(unit)
                mark = " ★" if peak and value == peak[0] else ""
                lines.append(f"{stamp(o.ts_ms, tz, date=False)} {'—' if value is None else str(value) + '°'}"
                             f"{(' ' + o.kind) if o.kind and o.kind != 'METAR' else ''}{mark}")
            if len(data.obs) > len(rows):
                lines.insert(len(lines) - len(rows), f"（只显示最近 {len(rows)} 条）")
            if data.after:
                lines.append(f"次日首条：{stamp(data.after.ts_ms, tz)} {deg(data.after.temp_in(unit))}")
        return "\n".join(lines)

    def cmd_book(self, req: Request) -> str:
        t = self.pick_target(req.args)
        now_ms = self.now_ms()
        fair, data, ladder = self.model(t, now_ms)
        unit = ladder.unit if ladder and ladder.levels else "C"
        peak = data.peak(unit) if data else None
        lines = [f"📚 {t.title}｜Predict 订单簿（Yes）"]
        lines.extend(self.ladder_lines(t, ladder, fair, peak[0] if peak else None, now_ms, depth=3))
        edges = sorted(self.edges(t, now_ms), key=lambda e: -e[0].edge)
        if edges:
            lines.append("\n净优势（吃单，含手续费）：")
            for edge, level in edges[:8]:
                lines.append(f"  {level.bracket.label} {edge.side} @{cents(edge.price)}｜模型 {cents(edge.fair)}｜{cents(edge.edge, True)}"
                             + ("｜观测已定" if edge.sure else ""))
        if fair:
            lines.append(f"\n模型：{fair.note}")
        lines.append(self.links(t))
        return "\n".join(lines)

    def cmd_markets(self, req: Request) -> str:
        now_ms = self.now_ms()
        targets = self.targets(now_ms)
        lines = ["🗂 跟踪的市场："]
        for t in targets:
            ladder = self.predict.ladders.get(t.slug)
            state = "等待获取" if ladder is None else "未上线" if not ladder.listed else f"{len(ladder.levels)} 档" + ("，已结算" if ladder.any_settled else "")
            lines.append(f"{t.title}｜{state}\n{predict_url(t.slug, self.config.predict_ref)}")
        for slug, error in self.target_errors.items():
            lines.append(f"{slug}｜{error}")
        return "\n".join(lines)

    def cmd_forecast(self, req: Request) -> str:
        t = self.pick_target(req.args)
        tz = zone(t.station.tz)
        start, end = day_bounds(t.day, tz)
        rows = [(ts, temp) for ts, temp in self.forecast.hourly.get(t.station.stid, []) if start <= ts < end]
        if not rows:
            err = self.forecast.errors.get(t.station.stid, "尚未获取" if self.config.forecast else "预报已关闭（FORECAST=off）")
            return f"🔮 {t.title}｜无预报：{err}"
        lines = [f"🔮 {t.title}｜Open-Meteo 逐小时气温（°C，{zone_name(tz)}）"]
        lines.append("  ".join(f"{stamp(ts, tz, date=False)[:2]}h {temp:.0f}" for ts, temp in rows))
        peak = max((temp, ts) for ts, temp in rows)
        lines.append(f"峰值 {peak[0]:.1f}°C（{stamp(peak[1], tz, date=False)}）｜模型 σ {self.config.forecast_sigma}°C，偏差 {self.config.forecast_bias:+.1f}°C")
        return "\n".join(lines)

    async def cmd_diag(self, req: Request) -> str:
        return await self.diagnose()

    async def diagnose(self) -> str:
        now_ms = self.now_ms()
        targets = self.targets(now_ms)
        lines = [f"🩺 数据源检测 v{VERSION}｜{stamp(now_ms, seconds=True)}（北京时间）"]
        stations = {t.station.stid: t.station for t in targets}
        for stid, station in stations.items():
            tz = zone(station.tz)
            today = local_date(now_ms, tz)
            start, _ = day_bounds(today, tz)
            for source in self.config.sources:
                started = time.monotonic()
                try:
                    if source == "noaa":
                        rows, meta = await self.weather.fetch_synoptic(stid, start, now_ms)
                        note = f"{len(rows)} 条，站点 {meta.get('NAME', '?')}，时区 {meta.get('TIMEZONE', '?')}"
                    elif source == "metar":
                        rows = await self.weather.fetch_metar(stid, start, now_ms)
                        note = f"{len(rows)} 条" + (f"，最新 {rows[-1].raw[:40]}" if rows else "")
                    else:
                        rows = await self.weather.fetch_wu(station, today)
                        note = f"{len(rows)} 条"
                    lines.append(f"OK {SOURCE_NAMES[source]} {stid}：{note}（{time.monotonic() - started:.1f}s）")
                except Exception as error:
                    lines.append(f"FAIL {SOURCE_NAMES[source]} {stid}：{clean_error(error)}")
            if self.config.forecast:
                lat, lon = station.lat, station.lon
                if lat is None:
                    lines.append(f"SKIP 预报 {stid}：缺少经纬度")
                else:
                    try:
                        await self.forecast.refresh(station, lat, lon, force=True)
                        lines.append(f"OK 预报 {stid}：{len(self.forecast.hourly.get(stid, []))} 小时" if stid in self.forecast.hourly
                                     else f"FAIL 预报 {stid}：{self.forecast.errors.get(stid, '?')}")
                    except Exception as error:
                        lines.append(f"FAIL 预报 {stid}：{clean_error(error)}")
        if self.config.predict:
            for t in targets[:3]:
                try:
                    markets = await self.predict.find(t.slug)
                    lines.append(f"OK Predict {t.slug}：{len(markets)} 个市场" if markets else f"OK Predict {t.slug}：未上线")
                except Exception as error:
                    lines.append(f"FAIL Predict {t.slug}：{clean_error(error)}")
        return "\n".join(lines)

    # -- loops -------------------------------------------------------------------------------------------------------------
    async def one_cycle(self) -> None:
        now_ms = self.now_ms()
        targets = self.targets(now_ms)
        await self.weather_cycle(now_ms, targets)
        await self.forecast_cycle(targets)
        await self.plan_alerts(now_ms, targets)
        self.last_cycle = self.clock()

    async def monitor_loop(self) -> None:
        while not self.stopping.is_set():
            started = time.monotonic()
            try:
                await self.one_cycle()
                self.heartbeat()
            except Exception as error:
                self.log_limited("monitor", "监控轮次异常：" + clean_error(error))
            await self.wait(max(1.0, self.config.poll - (time.monotonic() - started)))

    async def predict_loop(self) -> None:
        while not self.stopping.is_set():
            started = time.monotonic()
            try:
                now_ms = self.now_ms()
                targets = self.targets(now_ms)
                if await self.predict_cycle(now_ms, targets, force=True):
                    self.last_cycle = self.clock()
                    await self.plan_alerts(now_ms, targets)
            except Exception as error:
                self.log_limited("predict", "盘口轮次异常：" + clean_error(error))
            await self.wait(max(1.0, self.config.predict_poll - (time.monotonic() - started)))

    def heartbeat(self) -> None:
        if time.monotonic() - self.last_log.get("heartbeat", -1e9) >= 300:
            LOG.info("heartbeat: targets=%s ladders=%s active_subscriptions=%s deliveries=%s",
                     len(self.targets(self.now_ms())), len(self.predict.ladders), len(self.active_subscriptions()),
                     sum(not t.done() for t in self.deliveries.values()))
            self.last_log["heartbeat"] = time.monotonic()

    async def process_update(self, update: dict) -> None:
        message = update.get("message")
        if isinstance(message, dict):
            age = self.clock() - float(message.get("date", self.clock()))
            if -60 <= age <= 900:
                await self.process_message(message)

    async def commands_loop(self) -> None:
        assert self.telegram is not None
        offset = int(self.store.get("telegram_offset", 0))
        failures = 0
        while not self.stopping.is_set():
            try:
                updates = await self.telegram.call("getUpdates", {"offset": offset, "timeout": 20, "allowed_updates": ["message"]}, timeout=30)
                if not isinstance(updates, list):
                    raise ValueError("Telegram 更新格式异常")
                failures = 0
                for update in updates:
                    if self.stopping.is_set():
                        break
                    try:
                        await self.process_update(update)
                    except Exception as error:
                        self.log_limited("command", "命令处理异常：" + clean_error(error))
                    offset = int(update["update_id"]) + 1
                    self.store.put("telegram_offset", offset)
            except Exception as error:
                failures += 1
                self.log_limited("poll", "TG 轮询异常：" + clean_error(error))
                await self.wait(max(getattr(error, "retry_after", 0), min(60, 2 ** min(failures, 6))))

    async def watchdog(self) -> None:
        while not self.stopping.is_set():
            await self.wait(30)
            idle = self.clock() - max(self.last_cycle, self.started)
            if not self.stopping.is_set() and idle > WATCHDOG_SECONDS:
                LOG.error("监控循环已 %s 秒没有完成新一轮：退出，等待 Railway 重启", int(idle))
                return

    async def register_menu(self) -> None:
        assert self.telegram is not None
        try:
            await self.telegram.call("setMyCommands", {"commands": [c.menu_entry() for c in COMMANDS]})
            await self.telegram.call("setChatMenuButton", {"menu_button": {"type": "commands"}})
        except Exception as error:
            LOG.warning("注册命令菜单失败（不影响手动输入命令）：%s", clean_error(error))

    async def run(self) -> int:
        assert self.telegram is not None
        webhook = await self.telegram.call("getWebhookInfo")
        if webhook.get("url"):
            raise ValueError("该 Bot Token 已绑定 Webhook。请为本项目使用独立机器人，或在原服务移除 Webhook 后重试")
        me = await self.telegram.call("getMe")
        self.username = me.get("username", "")
        LOG.info("Starting @%s; cities=%s slugs=%s administrator_configured=%s", self.username, ",".join(self.config.cities),
                 ",".join(self.config.slugs), bool(self.config.admin_id))
        await self.register_menu()
        if not self.config.admin_id:
            LOG.warning("ADMIN_USER_ID 尚未配置：只能使用 /id；没有任何订阅")
        elif not self.subscriptions():
            self.set_subscription(self.config.admin_id, 0, True)  # the administrator's private chat is the first subscriber
        if self.config.synoptic_token == SYNOPTIC_DEMO_TOKEN:
            LOG.warning("SYNOPTIC_TOKEN 未配置：使用公开演示 token，可能随时失效；请到 synopticdata.com 申请免费 token")
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.stopping.set)
        loop.set_default_executor(ThreadPoolExecutor(max_workers=12, thread_name_prefix="io"))
        self.loops_running = True
        core = [asyncio.create_task(self.monitor_loop(), name="monitor"),
                asyncio.create_task(self.predict_loop(), name="predict"),
                asyncio.create_task(self.commands_loop(), name="commands"),
                asyncio.create_task(self.watchdog(), name="watchdog")]
        exit_code = 0
        stopper = asyncio.create_task(self.stopping.wait(), name="stopping")
        try:
            await asyncio.wait([stopper, *core], return_when=asyncio.FIRST_COMPLETED)
            for task in core:
                if task.done() and not task.cancelled():
                    error = task.exception()
                    if error is None and self.stopping.is_set():
                        continue
                    LOG.error("核心循环 %s 意外结束：%s；进程退出等待重启", task.get_name(),
                              clean_error(error) if error else "监控循环卡住" if task.get_name() == "watchdog" else "无异常")
                    exit_code = 1
        finally:
            self.stopping.set()
            stopper.cancel()
            pending = [task for task in self.deliveries.values() if not task.done()]
            if pending:
                await asyncio.wait(pending, timeout=SHUTDOWN_GRACE_SECONDS)
            for task in [*core, *pending]:
                task.cancel()
            await asyncio.gather(*core, *pending, stopper, return_exceptions=True)
            LOG.info("Stopped safely (exit %s)", exit_code)
        return exit_code


def number_or_none_wide(value: Any) -> float | None:
    """A coordinate from station metadata (strings allowed)."""
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def id_text(user_id: int, chat: int, thread: int) -> str:
    return f"用户 ID：{user_id}\n聊天 ID：{chat}\n话题 ID：{thread or '无'}"


# --- entry points ---------------------------------------------------------------------------------------------------------------
async def check_once(config: Config) -> int:
    """`python main.py --check`: one refresh of every source and market, the status cards printed; nothing is sent."""
    store = Store(":memory:")
    bot = Bot(config, store, None)
    try:
        now_ms = int(time.time() * 1000)
        targets = bot.targets(now_ms)
        await bot.predict_cycle(now_ms, targets, force=True)
        await bot.weather_cycle(now_ms, targets, force=True)
        await bot.forecast_cycle(targets, force=True)
        targets = bot.targets(now_ms)  # markets whose station came from the rules
        for t in targets:
            print(bot.card(t, now_ms, depth=3))
            print()
        failed = [k for k in bot.weather.errors] + [slug for slug, lad in bot.predict.ladders.items() if lad.error]
        return 1 if failed and not any(bot.weather.official(t.station.stid, t.day) for t in targets) else 0
    finally:
        store.close()


async def run_diagnostics(config: Config) -> int:
    store = Store(":memory:")
    bot = Bot(config, store, None)
    try:
        text = await bot.diagnose()
        print(text)
        return 0 if "FAIL" not in text else 1
    finally:
        store.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="刷新一次观测、预报和盘口并打印状态卡；不发送 TG")
    parser.add_argument("--diag", action="store_true", help="逐个检测数据源并打印报告（同 /diag）；不发送 TG")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    store = None
    try:
        config = Config.from_env()
        if args.check:
            return asyncio.run(check_once(config))
        if args.diag:
            return asyncio.run(run_diagnostics(config))
        if not re.fullmatch(r"\d+:[A-Za-z0-9_-]{20,}", config.token):
            raise ValueError("请在 Railway Variables 配置 TELEGRAM_BOT_TOKEN，不要写进代码或提交到 GitHub")
        store = Store(config.db_path)
        return asyncio.run(Bot(config, store, Telegram(config.token)).run())
    except KeyboardInterrupt:
        return 0
    except Exception as error:
        LOG.error("启动失败：%s", clean_error(error))
        return 1
    finally:
        if store:
            store.close()


if __name__ == "__main__":
    raise SystemExit(main())
