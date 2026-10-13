"""Observation parsers, local-day slicing, revisions, the weather feed's refresh and persistence."""
import sys, asyncio, datetime as dt, json
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401
import main as m

ms = m.parse_iso_ms
tz = m.zone("Asia/Shanghai")
DAY = dt.date(2026, 10, 13)

# --- Synoptic (the NOAA page's source) -----------------------------------------------------------------------------
def synoptic(rows, units="Celsius", code=1, message="OK", stid="ZSPD"):
    return {"UNITS": {"air_temp": units},
            "STATION": [{"STID": stid, "NAME": "Shanghai Pudong International Airport", "TIMEZONE": "Asia/Shanghai", "LATITUDE": "31.1433",
                         "LONGITUDE": "121.805", "OBSERVATIONS": {"date_time": [r[0] for r in rows], "air_temp_set_1": [r[1] for r in rows]}}],
            "SUMMARY": {"RESPONSE_CODE": code, "RESPONSE_MESSAGE": message}}

rows, meta = m.parse_synoptic(synoptic([("2026-10-13T04:00:00Z", 25.0), ("2026-10-13T02:00:00Z", None), ("2026-10-13T06:00:00Z", 26.0)]), "ZSPD")
assert [o.ts_ms for o in rows] == [ms("2026-10-13T02:00:00Z"), ms("2026-10-13T04:00:00Z"), ms("2026-10-13T06:00:00Z")]  # sorted
assert rows[0].temp is None and rows[2].temp == 26.0 and meta["TIMEZONE"] == "Asia/Shanghai" and meta["LATITUDE"] == "31.1433"
rows, _ = m.parse_synoptic(synoptic([("2026-10-13T14:00:00+0800", 78.8)], units="Fahrenheit"), "ZSPD")  # local-time stamps, °F units
assert rows[0].ts_ms == ms("2026-10-13T06:00:00Z") and abs(rows[0].temp - 26.0) < 1e-9
for bad in [synoptic([], code=2, message="Invalid token"), synoptic([], code=-1, message="No stations found"), {"SUMMARY": {}}, [], "x"]:
    try:
        m.parse_synoptic(bad, "ZSPD")
        assert False, bad
    except m.RemoteError as error:
        assert "Synoptic" in str(error)
assert m.parse_synoptic({"STATION": [], "SUMMARY": {"RESPONSE_CODE": 1}}, "ZSPD") == ([], {})
assert m.parse_synoptic(synoptic([("2026-10-13T04:00:00Z", 25.0)], stid="KSFO"), "ZSPD")[0][0].temp == 25.0  # the only station answers

# --- aviationweather.gov METAR ------------------------------------------------------------------------------------------
now_ms = ms("2026-10-13T06:30:00Z")
metar = [
    {"icaoId": "ZSPD", "obsTime": ms("2026-10-13T06:00:00Z") // 1000, "temp": 26.0, "rawOb": "ZSPD 130600Z 09008KT 9999 SCT030 26/18 Q1017 NOSIG", "metarType": "METAR"},
    {"icaoId": "ZSPD", "obsTime": ms("2026-10-13T05:30:00Z") // 1000, "temp": None, "rawOb": "SPECI ZSPD 130530Z 09008KT 9999 SCT030 25/18 Q1017", "metarType": "SPECI"},
    {"icaoId": "ZSPD", "obsTime": ms("2026-10-13T05:30:00Z") // 1000, "temp": 25.0, "rawOb": "SPECI ZSPD 130530Z ...", "metarType": "SPECI"},  # duplicate
    {"icaoId": "ZSSS", "obsTime": ms("2026-10-13T06:00:00Z") // 1000, "temp": 30.0, "rawOb": "ZSSS 130600Z 30/20"},  # another station
    {"icaoId": "ZSPD", "reportTime": "2026-10-13 05:00:00", "temp": 24.0, "rawOb": "ZSPD 130500Z 24/18 Q1017"},  # no obsTime: reportTime
    {"icaoId": "ZSPD", "rawOb": "ZSPD 130400Z 09008KT 9999 SCT030 M02/M05 Q1017"},  # only the raw report
    {"icaoId": "ZSPD", "obsTime": ms("2026-10-13T03:00:00Z"), "temp": "23.0"},  # ms and a string
    "junk",
]
rows = m.parse_metar_json(metar, "ZSPD", now_ms)
assert [(m.stamp(o.ts_ms, tz, date=False), o.temp, o.kind) for o in rows] == [
    ("11:00", 23.0, "METAR"), ("12:00", -2.0, "METAR"), ("13:00", 24.0, "METAR"), ("13:30", 25.0, "SPECI"), ("14:00", 26.0, "METAR")], rows
assert m.parse_metar_json({"data": metar[:1]}, "ZSPD", now_ms)[0].temp == 26.0
try:
    m.parse_metar_json({"error": "x"}, "ZSPD", now_ms)
    assert False
except m.RemoteError:
    pass
assert m.metar_temp("ZSPD 130600Z 09008KT 9999 SCT030 24/18 Q1017") == 24.0 and m.metar_temp("... M02/M05 ...") == -2.0 and m.metar_temp("nothing") is None
assert m.metar_temp("ZSPD 130600Z 24/// Q1017") == 24.0
assert m.metar_time("ZSPD 130600Z 09008KT", now_ms) == ms("2026-10-13T06:00:00Z")
assert m.metar_time("ZSPD 312300Z", ms("2026-11-01T00:30:00Z")) == ms("2026-10-31T23:00:00Z")  # previous month

# --- Weather Underground ---------------------------------------------------------------------------------------------------
wu = {"metadata": {"units": "m", "status_code": 200}, "observations": [
    {"valid_time_gmt": ms("2026-10-13T06:00:00Z") // 1000, "temp": 26, "obs_type": ""}, {"valid_time_gmt": ms("2026-10-13T05:00:00Z") // 1000, "temp": 24.4},
    {"valid_time_gmt": "x", "temp": 1}, {"valid_time_gmt": ms("2026-10-13T04:00:00Z") // 1000, "temp": None}]}
rows = m.parse_wu(wu)
assert [(o.temp) for o in rows] == [None, 24.4, 26] and rows[-1].ts_ms == ms("2026-10-13T06:00:00Z")
rows = m.parse_wu({"metadata": {"units": "e", "status_code": 200}, "observations": [{"valid_time_gmt": 1, "temp": 77}]})
assert abs(rows[0].temp - 25) < 1e-9
assert m.parse_wu({"metadata": {"units": "m", "status_code": 200}, "observations": None}) == []
for bad in [{"metadata": {"status_code": 401}}, {"errors": [{"error": {"code": "x", "message": "Invalid apiKey"}}]}, [], "x"]:
    try:
        m.parse_wu(bad)
        assert False, bad
    except m.RemoteError as error:
        assert "Weather Underground" in str(error)
assert m.parse_wu({"metadata": {}, "observations": []}) == []

# --- Open-Meteo --------------------------------------------------------------------------------------------------------------
om = {"utc_offset_seconds": 0, "hourly": {"time": ["2026-10-13T06:00", "2026-10-13T07:00", "x"], "temperature_2m": [26.1, 26.4, 1]}}
assert m.parse_open_meteo(om) == [(ms("2026-10-13T06:00:00Z"), 26.1), (ms("2026-10-13T07:00:00Z"), 26.4)]
om2 = {"utc_offset_seconds": 28800, "hourly": {"time": ["2026-10-13T14:00"], "temperature_2m": [26.1]}}  # local stamps + offset
assert m.parse_open_meteo(om2) == [(ms("2026-10-13T06:00:00Z"), 26.1)]
try:
    m.parse_open_meteo({"error": True, "reason": "Invalid latitude"})
    assert False
except m.RemoteError as error:
    assert "Invalid latitude" in str(error)

# --- slicing the local day --------------------------------------------------------------------------------------------------------
rows = [m.Obs(ms("2026-10-12T15:30:00Z"), 20.0), m.Obs(ms("2026-10-12T16:00:00Z"), 21.0), m.Obs(ms("2026-10-13T06:00:00Z"), 26.0),
        m.Obs(ms("2026-10-13T15:59:00Z"), 22.0), m.Obs(ms("2026-10-13T16:00:00Z"), None), m.Obs(ms("2026-10-13T17:00:00Z"), 21.0)]
inside, after = m.slice_day(rows, DAY, tz)
assert [o.temp for o in inside] == [21.0, 26.0, 22.0] and after.ts_ms == ms("2026-10-13T16:00:00Z")  # midnight belongs to the next day; it locks whatever it carries
assert m.slice_day(rows[:3], DAY, tz)[1] is None
assert m.slice_day([m.Obs(ms("2026-10-14T16:00:00Z"), 20.0)], DAY, tz) == ([], None)  # two days later is not "the following date"
data = m.DayData("noaa", "ZSPD", DAY, "Asia/Shanghai", inside, after, 1)
assert data.peak() == (26, inside[1]) and data.latest.temp == 22.0 and data.locked
assert data.peak("F") == (79, inside[1])
assert m.DayData("x", "ZSPD", DAY, "Asia/Shanghai", [m.Obs(1, None)], None, 1).peak() is None
again = m.DayData.from_json(json.loads(json.dumps(data.to_json())))
assert again.obs == data.obs and again.after == data.after and again.day == DAY and again.source == "noaa" and again.tz_name == "Asia/Shanghai"
assert m.DayData.from_json({"bad": 1}) is None and m.Obs.from_json("x") is None

# --- revisions -----------------------------------------------------------------------------------------------------------------------
old = m.DayData("noaa", "ZSPD", DAY, "Asia/Shanghai", [m.Obs(ms("2026-10-13T04:00:00Z"), 25.0), m.Obs(ms("2026-10-13T05:00:00Z"), 26.0), m.Obs(ms("2026-10-13T06:00:00Z"), 26.0)])
new = m.DayData("noaa", "ZSPD", DAY, "Asia/Shanghai", [m.Obs(ms("2026-10-13T04:00:00Z"), 25.0), m.Obs(ms("2026-10-13T05:00:00Z"), 25.0), m.Obs(ms("2026-10-13T06:00:00Z"), 26.0)])
assert m.compare_days(old, new) == ["13:00 的读数由 26° 改为 25°"]
assert m.compare_days(old, m.DayData("noaa", "ZSPD", DAY, "Asia/Shanghai", [old.obs[0], old.obs[2]])) == ["1 条已发布的读数消失（13:00）"]
assert m.compare_days(old, m.DayData("noaa", "ZSPD", DAY, "Asia/Shanghai", old.obs[:2])) == []  # the newest row gone: a feed lagging, not a revision
assert m.compare_days(None, new) == [] and m.compare_days(old, old) == []
assert m.compare_days(old, m.DayData("noaa", "ZSPD", DAY, "Asia/Shanghai", old.obs[:2] + [m.Obs(ms("2026-10-13T06:00:00Z"), 26.4)])) == []  # same whole degree


# --- the feed: windows, failures keep rows, revisions, persistence, source precedence ------------------------------------------------
class Net:
    def __init__(self):
        self.synoptic = []   # (iso, temp)
        self.metar = []      # (iso, temp, kind)
        self.wu = []         # (iso, temp)
        self.fail = set()
        self.calls = []

    async def http_json(self, url, payload=None, timeout=15, headers=None):
        self.calls.append(url)
        q = dict(__import__("urllib.parse").parse.parse_qsl(url.split("?", 1)[1])) if "?" in url else {}
        if "synopticdata" in url:
            if "noaa" in self.fail:
                raise m.RemoteError("HTTP 503: down")
            start, end = (dt.datetime.strptime(q[k], "%Y%m%d%H%M").replace(tzinfo=m.UTC).timestamp() * 1000 for k in ("start", "end"))
            rows = [(iso, t) for iso, t in self.synoptic if start <= ms(iso) <= end]
            return synoptic(rows)
        if "aviationweather" in url:
            if "metar" in self.fail:
                raise m.RemoteError("网络错误 (TimeoutError)")
            return [{"icaoId": "ZSPD", "obsTime": ms(iso) // 1000, "temp": t, "metarType": kind, "rawOb": f"{kind} ZSPD"} for iso, t, kind in self.metar]
        if "api.weather.com" in url:
            if "wu" in self.fail:
                raise m.RemoteError("HTTP 401: bad key")
            return {"metadata": {"units": "m", "status_code": 200}, "observations": [{"valid_time_gmt": ms(iso) // 1000, "temp": t} for iso, t in self.wu]}
        raise AssertionError(url)


async def run():
    net = Net()
    m.http_json = net.http_json
    store = m.Store(":memory:")
    config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"})
    feed = m.WeatherFeed(config, store)
    station = config.stations["shanghai"]
    net.synoptic = [("2026-10-12T15:00:00Z", 20.0), ("2026-10-12T16:00:00Z", 21.0), ("2026-10-13T04:00:00Z", 25.0), ("2026-10-13T06:00:00Z", 26.0)]
    net.metar = [("2026-10-13T04:00:00Z", 25.0, "METAR"), ("2026-10-13T06:00:00Z", 26.0, "METAR")]
    net.wu = [("2026-10-13T04:00:00Z", 25.0), ("2026-10-13T06:00:00Z", 26.0)]
    now = ms("2026-10-13T06:10:00Z")
    await feed.refresh(station, [DAY], now)
    assert len([u for u in net.calls if "synoptic" in u]) == 1 and len([u for u in net.calls if "aviation" in u]) == 1 and len([u for u in net.calls if "weather.com" in u]) == 1
    assert "start=202610121500" in net.calls[0] and "end=202610130611" in net.calls[0] and "units=metric" in net.calls[0] and "stid=ZSPD" in net.calls[0]
    assert "hours=" in net.calls[1] and "startDate=20261013" in net.calls[2] and "endDate=20261014" in net.calls[2] and "ZSPD:9:CN" in net.calls[2]
    noaa = feed.get("noaa", "ZSPD", DAY)
    assert [o.temp for o in noaa.obs] == [21.0, 25.0, 26.0] and noaa.peak() == (26, noaa.obs[2]) and not noaa.locked
    assert feed.official("ZSPD", DAY).source == "noaa" and feed.meta["ZSPD"]["TIMEZONE"] == "Asia/Shanghai"
    assert feed.get("metar", "ZSPD", DAY).peak()[0] == 26 and feed.get("wu", "ZSPD", DAY).peak()[0] == 26
    assert store.get("obs:noaa:ZSPD:2026-10-13")["obs"][2][1] == 26.0 and store.get("station_meta:ZSPD")["NAME"]
    assert feed.source_line(station, DAY, "C")[0].startswith("NOAA/Synoptic：最高 26°C（14:00）｜3 条")

    # not due: only the METAR channel is read on the next cycle; a newer METAR makes it the official data
    net.calls.clear()
    net.metar.append(("2026-10-13T07:00:00Z", 27.0, "METAR"))
    await feed.refresh(station, [DAY], ms("2026-10-13T07:05:00Z"))
    assert len(net.calls) == 1 and "aviationweather" in net.calls[0]
    assert feed.official("ZSPD", DAY).source == "metar" and feed.official("ZSPD", DAY).peak()[0] == 27

    # a failing source keeps its rows and notes the error; a revision in the Synoptic rows is reported once
    net.fail = {"metar", "wu"}
    net.synoptic[2] = ("2026-10-13T04:00:00Z", 24.0)
    net.synoptic.append(("2026-10-13T07:00:00Z", 27.0))
    await feed.refresh(station, [DAY], ms("2026-10-13T07:10:00Z"), force=True)
    assert feed.errors[("metar", "ZSPD")].startswith("网络错误") and feed.errors[("wu", "ZSPD")].startswith("HTTP 401")
    assert feed.get("metar", "ZSPD", DAY).peak()[0] == 27 and feed.get("metar", "ZSPD", DAY).error.startswith("网络错误")
    assert feed.revisions == [("noaa", "ZSPD", DAY, "12:00 的读数由 25° 改为 24°")], feed.revisions
    assert feed.official("ZSPD", DAY).source == "noaa"  # same newest observation: Synoptic wins the tie
    assert sum("⚠️ 刷新失败" in line for line in feed.source_line(station, DAY, "C")) == 2  # rows kept, failure noted
    net.fail = set()
    await feed.refresh(station, [DAY], ms("2026-10-13T07:20:00Z"), force=True)
    assert not feed.errors and feed.revisions == [] and feed.get("metar", "ZSPD", DAY).error == ""

    # an answer without the day's rows keeps what was seen
    net.synoptic = [("2026-10-12T15:00:00Z", 20.0)]
    await feed.refresh(station, [DAY], ms("2026-10-13T07:30:00Z"), force=True)
    noaa = feed.get("noaa", "ZSPD", DAY)
    assert noaa.peak()[0] == 27 and "沿用上次" in noaa.error and feed.revisions == []

    # the lock: the following local day's first row
    net.synoptic = [("2026-10-13T04:00:00Z", 24.0), ("2026-10-13T06:00:00Z", 26.0), ("2026-10-13T07:00:00Z", 27.0), ("2026-10-13T16:00:00Z", 22.0)]
    await feed.refresh(station, [DAY, DAY + dt.timedelta(days=1)], ms("2026-10-13T16:05:00Z"), force=True)
    noaa = feed.get("noaa", "ZSPD", DAY)
    assert noaa.locked and noaa.after.ts_ms == ms("2026-10-13T16:00:00Z") and noaa.peak()[0] == 27
    assert feed.get("noaa", "ZSPD", DAY + dt.timedelta(days=1)).obs[0].temp == 22.0

    # a restart reloads the store
    feed2 = m.WeatherFeed(config, store)
    assert feed2.official("ZSPD", DAY).peak()[0] == 27 and feed2.get("noaa", "ZSPD", DAY).locked and feed2.meta["ZSPD"]["TIMEZONE"]
    feed2.prune(ms("2026-11-13T00:00:00Z"))
    assert feed2.get("noaa", "ZSPD", DAY) is None and not store.keys("obs:")

    # probe: a station the rules name
    await feed.probe("VHHH", now)
    assert feed.meta["VHHH"]["TIMEZONE"] == "Asia/Shanghai" and store.get("station_meta:VHHH")
    # tz_for: configured zone, else Synoptic's, else an error
    assert m.zone_name(feed.tz_for(m.StationSpec("x", "VHHH", ""))) == "Asia/Shanghai"
    try:
        feed.tz_for(m.StationSpec("x", "ZZZZ", ""))
        assert False
    except ValueError:
        pass
    # Weather Underground is the last resort
    config2 = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SOURCES": "wu,noaa"})
    feed3 = m.WeatherFeed(config2, m.Store(":memory:"))
    net.fail = {"noaa"}
    await feed3.refresh(station, [DAY], now, force=True)
    assert feed3.official("ZSPD", DAY).source == "wu"
    store.close()

asyncio.run(run())
print("SOURCES_OK")
