"""Brackets, slugs, stations, configuration, zones, rounding."""
import sys, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401
import main as m

B = m.Bracket.make
cases = {
    "22°C or below": B(None, 22), "28°C or higher": B(28, None), "25°C": B(25, 25), "27-28°F": B(27, 28, "F"),
    "Will the highest temperature in Shanghai be 26°C or below on October 13?": B(None, 26),
    "Will the highest temperature in Shanghai be 27°C on October 13?": B(27, 27),
    "Will the highest temperature in NYC be between 70 and 71°F on October 13?": B(70, 71, "F"),
    "≤22°C": B(None, 22), ">=31°C": B(31, None), "28°C 或以上": B(28, None), "22°C 或以下": B(None, 22),
    "80-81°F": B(80, 81, "F"), "80°F-81°F": B(80, 81, "F"), "22 °C or lower": B(None, 22), "31°C or above": B(31, None),
    "-3°C or below": B(None, -3), "-2°C": B(-2, -2), "0°C": B(0, 0), "26C or below": B(None, 26), "27 ℃": B(27, 27),
    "below 20°C": B(None, 19), "above 30°C": B(31, None), "29°c": B(29, 29), "30°": B(30, 30),
}
for text, want in cases.items():
    got = m.parse_bracket(text)
    assert got is not None and (got.low, got.high, got.unit) == (want.low, want.high, want.unit), (text, got)
for text in ["October 13", "Highest temperature in Shanghai", "", "Will it rain?", "13"]:
    assert m.parse_bracket(text) is None, text
assert m.parse_bracket("75", "F") is None  # a bare number is never a bracket
assert m.parse_bracket("27° or higher", "F").unit == "F"  # unit from the rules when the title has none
assert m.Bracket.make(None, 24).label == "≤24°C" and m.Bracket.make(28, None).label == "≥28°C"
assert m.Bracket.make(25, 25).label == "25°C" and m.Bracket.make(80, 81, "F").label == "80–81°F"
assert m.Bracket.make(None, 24).contains(-5) and not m.Bracket.make(None, 24).contains(25)
assert m.Bracket.make(28, None).contains(40) and m.Bracket.make(25, 26).contains(26)
assert sorted([B(28, None), B(25, 25), B(None, 24), B(26, 27)]) == [B(None, 24), B(25, 25), B(26, 27), B(28, None)]

# rules
rules = ("This market will resolve to the temperature range that contains the highest temperature recorded by NOAA at the "
         "Shanghai Pudong International Airport Station in degrees Celsius on 13 Oct '26. ... available here: "
         "https://www.weather.gov/wrh/timeseries?site=zspd ... the Weather Underground Daily Observations table")
assert m.rules_unit(rules) == "C" and m.rules_site(rules) == "ZSPD"
assert m.rules_unit("in degrees Fahrenheit on") == "F" and m.rules_unit("") == "C"
assert m.rules_site("no site here") == ""
assert m.rules_wu_id("https://www.wunderground.com/history/daily/cn/shanghai/ZSPD/date/2026-10-13") == "ZSPD:9:CN"

# slugs
assert m.parse_slug("highest-temperature-in-shanghai-on-october-13-2026") == ("shanghai", dt.date(2026, 10, 13))
assert m.parse_slug("highest-temperature-in-hong-kong-on-october-9-2026") == ("hong-kong", dt.date(2026, 10, 9))
assert m.parse_slug("HIGHEST-temperature-in-new-york-city-on-january-1-2027") == ("new-york-city", dt.date(2027, 1, 1))
assert m.parse_slug("unitree-up-or-down-on-october-13-2026") is None
assert m.parse_slug("highest-temperature-in-shanghai-on-octember-13-2026") is None
assert m.parse_slug("highest-temperature-in-shanghai-on-february-30-2026") is None
assert m.make_slug("shanghai", dt.date(2026, 10, 13)) == "highest-temperature-in-shanghai-on-october-13-2026"
assert m.make_slug("hong-kong", dt.date(2026, 1, 5)) == "highest-temperature-in-hong-kong-on-january-5-2026"
assert m.predict_url("x", "B00EA") == "https://predict.fun/zh-cn/market/x?ref=B00EA" and m.predict_url("x") == "https://predict.fun/zh-cn/market/x"

# stations
st = m.parse_stations("shanghai=ZSPD:Asia/Shanghai:CN:31.1434,121.8052;hong-kong=VHHH:+08:00:HK")
assert st["shanghai"].stid == "ZSPD" and st["shanghai"].lat == 31.1434 and st["shanghai"].name == "上海浦东机场"
assert st["hong-kong"].tz == "+08:00" and st["hong-kong"].country == "HK" and st["hong-kong"].lat == 22.3080  # a known city: coordinates filled in
assert m.parse_stations("shanghai=ZSPD")["shanghai"].tz == "Asia/Shanghai"  # known city: the rest is filled in
assert m.parse_stations("") == {}
for bad in ["shanghai", "shanghai=", "foo=ZSPD", "shanghai=ZSPD:Mars/Olympus", "x=ABCD:+08:00:CN:1,2,3", "x=ABCD:+08:00:CN:99,0"]:
    try:
        m.parse_stations(bad)
        assert False, bad
    except ValueError:
        pass
assert m.parse_stations("tokyo=RJTT:Asia/Tokyo:JP")["tokyo"].lat == 35.5494

# config
c = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"})
assert c.cities == ("shanghai",) and c.slugs == () and c.sources == ("noaa", "metar", "wu") and c.follow_days == 1
assert c.synoptic_token == m.SYNOPTIC_DEMO_TOKEN and c.wu_key == m.WU_DEFAULT_KEY and c.predict_ref == "B00EA"
assert c.poll == 60 and c.noaa_poll == 600 and c.wu_poll == 900 and c.predict_poll == 20 and c.edge_cents == 0.10
c2 = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "CITIES": "", "MARKET_SLUGS": "https://predict.fun/zh-cn/market/highest-temperature-in-shanghai-on-october-13-2026?ref=B00EA, foo-bar",
                        "SOURCES": "metar", "PREDICT_REF_CODE": "", "SYNOPTIC_TOKEN": "abcdefgh12345678", "FORECAST": "off", "EDGE_ALERT_CENTS": "5"})
assert c2.cities == () and c2.slugs == ("highest-temperature-in-shanghai-on-october-13-2026", "foo-bar") and c2.sources == ("metar",)
assert c2.predict_ref == "" and c2.synoptic_token == "abcdefgh12345678" and not c2.forecast and abs(c2.edge_cents - 0.05) < 1e-9
for bad in [{"CITIES": "mars"}, {"CITIES": "", "MARKET_SLUGS": ""}, {"SOURCES": "nasa"}, {"POLL_SECONDS": "1"}, {"SLUG_PATTERN": "x-{city}"},
            {"MARKET_SLUGS": "bad slug"}, {"PREDICT_REF_CODE": "a&b"}, {"FORECAST_SIGMA": "9"}, {"ADMIN_USER_ID": "x"}]:
    try:
        m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", **bad})
        assert False, bad
    except ValueError:
        pass
c3 = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "CITIES": "shanghai,hong-kong", "STATIONS": "hong-kong=VHHH:Asia/Hong_Kong:HK"})
assert c3.cities == ("shanghai", "hong-kong") and c3.stations["hong-kong"].stid == "VHHH"

# zones and days
tz = m.zone("Asia/Shanghai")
assert m.day_bounds(dt.date(2026, 10, 13), tz) == (m.parse_iso_ms("2026-10-12T16:00:00Z"), m.parse_iso_ms("2026-10-13T16:00:00Z"))
assert m.local_date(m.parse_iso_ms("2026-10-13T15:59:00Z"), tz) == dt.date(2026, 10, 13)
assert m.local_date(m.parse_iso_ms("2026-10-13T16:00:00Z"), tz) == dt.date(2026, 10, 14)
assert m.zone("+08:00").utcoffset(None) == dt.timedelta(hours=8) and m.zone("-0530").utcoffset(None) == -dt.timedelta(hours=5, minutes=30)
assert m.zone("UTC+9").utcoffset(None) == dt.timedelta(hours=9)
for bad in ["", "+25:00", "Nowhere/Nothing", "12345"]:
    try:
        m.zone(bad)
        assert False, bad
    except ValueError:
        pass
assert m.stamp(m.parse_iso_ms("2026-10-13T06:00:00Z"), tz) == "10-13 14:00" and m.stamp(0, tz, date=False) == "08:00"
assert m.stamp(m.parse_iso_ms("2026-10-13T06:00:05Z"), tz, seconds=True) == "10-13 14:00:05"

# rounding the way the page prints
assert [m.whole_degrees(v) for v in (24.5, 24.49, -0.5, -0.4, 23.0, 26.999)] == [25, 24, -1, 0, 23, 27]
assert m.Obs(0, 24.0).temp_in("F") == 75 and m.Obs(0, 25.0).temp_in("F") == 77 and m.Obs(0, 23.9).temp_in("F") == 75
assert m.Obs(0, None).temp_in("C") is None and m.Obs(0, 26.0).temp_in("C") == 26
assert m.deg(None) == "—" and m.deg(26, "C") == "26°C"
for c_value in range(-10, 45):  # whole-degree METAR readings survive the page's °F round trip
    f = m.whole_degrees(m.c_to_f(c_value))
    assert m.whole_degrees((f - 32) * 5 / 9) == c_value, c_value

# dates in commands
today = dt.date(2026, 10, 13)
assert m.parse_day("10-13", today) == today and m.parse_day("2026-10-14", today) == dt.date(2026, 10, 14)
assert m.parse_day("today", today) == today and m.parse_day("明天", today) == dt.date(2026, 10, 14) and m.parse_day("1012", today) == dt.date(2026, 10, 12)
for bad in ["13", "2026-13-01", "x"]:
    try:
        m.parse_day(bad, today)
        assert False, bad
    except ValueError:
        pass

# errors never leak secrets
assert "[REDACTED]" in m.clean_error("https://x/y?token=abc123def fails") or "[URL]" in m.clean_error("https://x/y?token=abc123def fails")
assert "TOKEN REDACTED" in m.clean_error("Telegram 123456:ABCDEFGHIJKLMNOPQRSTUV said no")
assert m.clean_error("apiKey=e1f10a1e78da46f5b10a1e78da96f525 bad") == "apiKey=[REDACTED] bad"
print("PARSE_OK")
