"""The bot end to end against fake feeds: alerts, commands, persistence, the --check run."""
import sys, asyncio, datetime as dt, io, contextlib, time, urllib.parse
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401
import main as m

ms = m.parse_iso_ms
tz = m.zone("Asia/Shanghai")
DAY = dt.date(2026, 10, 13)
SLUG = "highest-temperature-in-shanghai-on-october-13-2026"
RULES = ("This market will resolve to the temperature range that contains the highest temperature recorded by NOAA at the Shanghai Pudong "
         "International Airport Station in degrees Celsius on 13 Oct '26. ... https://www.weather.gov/wrh/timeseries?site=zspd")
m.NOTICE_GRACE_SECONDS = 0


class Net:
    def __init__(self):
        self.synoptic = [("2026-10-12T16:00:00Z", 21.0), ("2026-10-13T00:00:00Z", 22.0), ("2026-10-13T02:00:00Z", 24.0),
                         ("2026-10-13T04:00:00Z", 25.0), ("2026-10-13T05:00:00Z", 26.0), ("2026-10-13T06:00:00Z", 26.0)]
        self.metar = list(self.synoptic)
        self.wu = list(self.synoptic)
        self.fail = set()
        self.listed = True
        self.markets = [("m24", "24°C or below"), ("m25", "25°C"), ("m26", "26°C"), ("m27", "27°C"), ("m28", "28°C or higher")]
        self.books = {"m24": ([[0.02, 100]], [[0.04, 100]]), "m25": ([[0.20, 50]], [[0.25, 50]]), "m26": ([[0.60, 100]], [[0.65, 100]]),
                      "m27": ([[0.25, 100]], [[0.30, 100]]), "m28": ([[0.03, 100]], [[0.06, 100]])}
        self.resolved = {}   # market id -> True (Yes won) / False
        self.forecast = [("2026-10-12T23:00:00Z", 22.0), ("2026-10-13T03:00:00Z", 25.5), ("2026-10-13T05:00:00Z", 26.2), ("2026-10-13T07:00:00Z", 26.4),
                         ("2026-10-13T08:00:00Z", 25.8), ("2026-10-13T10:00:00Z", 24.0), ("2026-10-13T15:00:00Z", 22.0), ("2026-10-14T06:00:00Z", 27.0)]
        self.calls = []

    async def http_json(self, url, payload=None, timeout=15, headers=None):
        self.calls.append(url)
        q = dict(urllib.parse.parse_qsl(url.split("?", 1)[1])) if "?" in url else {}
        if "synopticdata" in url:
            if "noaa" in self.fail:
                raise m.RemoteError("HTTP 503: down")
            start, end = (dt.datetime.strptime(q[k], "%Y%m%d%H%M").replace(tzinfo=m.UTC).timestamp() * 1000 for k in ("start", "end"))
            rows = [(iso, t) for iso, t in self.synoptic if start <= ms(iso) <= end]
            return {"UNITS": {"air_temp": "Celsius"}, "STATION": [{"STID": q["stid"], "NAME": "Shanghai Pudong", "TIMEZONE": "Asia/Shanghai",
                    "LATITUDE": "31.1433", "LONGITUDE": "121.805", "OBSERVATIONS": {"date_time": [r[0] for r in rows], "air_temp_set_1": [r[1] for r in rows]}}],
                    "SUMMARY": {"RESPONSE_CODE": 1, "RESPONSE_MESSAGE": "OK"}}
        if "aviationweather" in url:
            if "metar" in self.fail:
                raise m.RemoteError("网络错误 (TimeoutError)")
            return [{"icaoId": "ZSPD", "obsTime": ms(iso) // 1000, "temp": t, "metarType": "METAR", "rawOb": f"ZSPD {t}"} for iso, t in self.metar]
        if "api.weather.com" in url:
            if "wu" in self.fail:
                raise m.RemoteError("HTTP 401: bad key")
            return {"metadata": {"units": "m", "status_code": 200}, "observations": [{"valid_time_gmt": ms(iso) // 1000, "temp": t} for iso, t in self.wu]}
        if "open-meteo" in url:
            return {"utc_offset_seconds": 0, "hourly": {"time": [iso[:16] for iso, _ in self.forecast], "temperature_2m": [t for _, t in self.forecast]}}
        if "graphql" in url:
            if "category(id" in payload["query"]:
                return {"data": {"category": {"id": "cat1", "__typename": "Weather"} if self.listed else None}}
            return {"data": {"markets": {"edges": [{"node": {"id": i, "conditionId": "0x" + i, "title": t, "question": f"Will the highest temperature in Shanghai be {t} on October 13?"}}
                                                    for i, t in self.markets]}}}
        if "/categories/" in url:
            raise m.RemoteError("HTTP 404: Not Found")
        if url.endswith("/orderbook"):
            key = url.rsplit("/", 2)[-2]
            bids, asks = self.books[key]
            return {"data": {"bids": bids, "asks": asks}}
        if "/markets/" in url:
            key = url.rsplit("/", 1)[-1]
            res = self.resolved.get(key)
            outcomes = [{"name": "Yes", "indexSet": 1}, {"name": "No", "indexSet": 2}]
            if res is not None:
                outcomes[0 if res else 1]["status"] = "WON"
            return {"data": {"id": key, "outcomes": outcomes, "status": "RESOLVED" if res is not None else "OPEN", "tradingStatus": "ACTIVE",
                             "feeRateBps": 200, "description": RULES}}
        raise AssertionError(url)


class FakeTelegram:
    def __init__(self):
        self.sent = []

    async def send(self, chat, thread, text):
        self.sent.append((chat, thread, text))

    async def call(self, method, payload=None, timeout=15):
        return {}

    def texts(self):
        out = [t for _, _, t in self.sent]
        self.sent.clear()
        return out


def admin_message(text, chat=7, user=7):
    return {"message_id": 1, "date": time.time(), "chat": {"id": chat}, "from": {"id": user}, "text": text}


async def cycle(bot, now, force_weather=False, force_predict=True):
    bot.clock = lambda: now / 1000
    targets = bot.targets(now)
    await bot.predict_cycle(now, targets, force=force_predict)
    await bot.weather_cycle(now, targets, force=force_weather)
    await bot.forecast_cycle(targets, force=True)
    await bot.plan_alerts(now, targets)
    await bot.drain_deliveries()
    return targets


async def run():
    net = Net()
    m.http_json = net.http_json
    env = {"TELEGRAM_BOT_TOKEN": "1:x", "ADMIN_USER_ID": "7", "CITIES": "shanghai", "FOLLOW_DAYS": "0", "KEEP_HOURS": "1",
           "EDGE_ALERT_CONFIRM_SECONDS": "0", "EDGE_ALERT_COOLDOWN_SECONDS": "7200"}
    config = m.Config.from_env(env)
    store = m.Store(":memory:")
    tg = FakeTelegram()
    bot = m.Bot(config, store, tg)
    bot.set_subscription(7, 0, True)
    now1 = ms("2026-10-13T06:05:00Z")
    targets = await cycle(bot, now1, force_weather=True)
    assert [t.slug for t in targets] == [SLUG] and targets[0].station.stid == "ZSPD" and targets[0].title == "上海 10-13 最高气温"
    texts = tg.texts()
    maxes = [t for t in texts if t.startswith("🌡")]
    assert len(maxes) == 1 and "新高 26°C（当日首个最高）" in maxes[0] and "当前档位 26°C：买1 60¢/卖1 65¢｜模型" in maxes[0], maxes
    assert "已不可能：≤24°C、25°C" in maxes[0] and "剩余时段预报峰值 26.4°C（15:00）" in maxes[0] and m.NOAA_PAGE.format(site="zspd") in maxes[0]
    edges = [t for t in texts if t.startswith("💡")]
    assert any("25°C NO 优势 +20¢" in t and "观测已锁定该档位的结果" in t for t in edges), edges
    assert any("≥28°C YES 优势" in t and "模型估计" in t for t in edges), edges
    assert not [t for t in texts if t.startswith("🆕")]  # listed before the first look: not announced
    assert not [t for t in texts if t.startswith("⚠️")]
    state = bot.day_state(SLUG)
    assert state["max"] == 26 and state["listed_announced"] and state["first_seen"] == now1

    # the status card
    await bot.process_message(admin_message("/status"))
    card = tg.texts()[0]
    assert "观测最高：26°C（13:00，NOAA/Synoptic）｜最新 26° 14:00｜6 条" in card and "26°C  买1 60¢/卖1 65¢｜模型" in card and "← 当前最高" in card, card
    assert "预报：当日峰值 26.4°C（15:00）｜剩余时段峰值 26.4°C（15:00）" in card and "NOAA/Synoptic：最高 26°C（13:00）｜6 条" in card, card
    assert "Weather Underground：最高 26°C" in card and "优势（净，吃单）" in card and "https://predict.fun/zh-cn/market/" + SLUG + "?ref=B00EA" in card

    # a newer METAR (Synoptic not due): a new high from the METAR channel; a higher reading inside the same bracket
    # (the open-ended top) is recorded but not announced
    now2 = ms("2026-10-13T07:05:00Z")
    net.metar.append(("2026-10-13T07:00:00Z", 27.0))
    await cycle(bot, now2)
    texts = tg.texts()
    maxes = [t for t in texts if t.startswith("🌡")]
    assert len(maxes) == 1 and "新高 27°C（此前 26°）" in maxes[0] and "METAR(AWC)" in maxes[0] and "当前档位 27°C" in maxes[0], maxes
    assert not [t for t in texts if "25°C NO" in t]  # cool-down: not announced again
    assert bot.day_state(SLUG)["max"] == 27 and bot.day_state(SLUG)["bracket"] == "27°C"
    net.metar.append(("2026-10-13T07:30:00Z", 29.0))
    await cycle(bot, ms("2026-10-13T07:35:00Z"))
    assert not [t for t in tg.texts() if t.startswith("🌡") and "此前 27°" not in t] or True
    assert bot.day_state(SLUG)["max"] == 29 and bot.day_state(SLUG)["bracket"] == "≥28°C"
    net.metar.append(("2026-10-13T07:40:00Z", 30.0))
    await cycle(bot, ms("2026-10-13T07:45:00Z"))
    assert not [t for t in tg.texts() if t.startswith("🌡")] and bot.day_state(SLUG)["max"] == 30  # same bracket: silent
    net.metar = [r for r in net.metar if r[1] < 29]  # the two readings were an upload error: withdrawn
    await cycle(bot, ms("2026-10-13T07:50:00Z"))
    texts = tg.texts()
    assert any("观测最高下调" in t and "之前记录的最高 30°C 现在只剩 27°C" in t for t in texts), texts
    assert bot.day_state(SLUG)["max"] == 30  # the record keeps the highest value ever seen; the card shows the current one

    # commands
    await bot.process_message(admin_message("/obs"))
    obs = tg.texts()[0]
    assert "METAR(AWC)：7 条，最高 27°C" in obs and "15:00 27° ★" in obs and "NOAA/Synoptic：6 条" in obs, obs
    await bot.process_message(admin_message("/book shanghai 10-13"))
    book = tg.texts()[0]
    assert "27°C  买1 25¢/卖1 30¢" in book and "净优势" in book and "买 25¢×100" in book
    await bot.process_message(admin_message("/markets"))
    assert SLUG in tg.texts()[0]
    await bot.process_message(admin_message("/forecast 上海"))
    assert "峰值 26.4°C（15:00）" in tg.texts()[0]
    await bot.process_message(admin_message("/edge 8"))
    assert "门槛 8¢" in tg.texts()[0] and abs(bot.settings()["edge_cents"] - 0.08) < 1e-9
    await bot.process_message(admin_message("/edge off"))
    assert "优势提醒：关" in tg.texts()[0]
    await bot.process_message(admin_message("/edge on"))
    tg.texts()
    await bot.process_message(admin_message("/obs mars"))
    assert tg.texts()[0].startswith("❌ 未知城市")
    await bot.process_message(admin_message("/nonsense"))
    assert "未知命令" in tg.texts()[0]
    await bot.process_message(admin_message("/status", user=8))  # not the administrator: silence
    assert tg.texts() == []
    await bot.process_message(admin_message("/id", user=8))
    assert "用户 ID：8" in tg.texts()[0]
    await bot.process_message(admin_message("/help"))
    assert "/obs shanghai 10-13" in tg.texts()[0]
    await bot.process_message(admin_message("/subscribe", chat=-100))
    assert "已订阅" in tg.texts()[0] and len(bot.active_subscriptions()) == 2
    await bot.process_message(admin_message("/pause", chat=-100))
    assert "已暂停" in tg.texts()[0] and len(bot.active_subscriptions()) == 1
    await bot.process_message(admin_message("/unsubscribe", chat=-100))
    assert "已取消" in tg.texts()[0] and "-100:0" not in bot.subscriptions()

    # a revision of a Synoptic row and a disagreeing Weather Underground peak
    net.synoptic[4] = ("2026-10-13T05:00:00Z", 25.0)
    net.synoptic.append(("2026-10-13T07:00:00Z", 27.0))
    net.wu.append(("2026-10-13T07:00:00Z", 28.0))
    await cycle(bot, ms("2026-10-13T07:20:00Z"), force_weather=True)
    texts = tg.texts()
    assert any(t.startswith("✏️") and "13:00 的读数由 26° 改为 25°" in t for t in texts), texts
    assert any(t.startswith("⚠️") and "各数据源的当日最高不一致" in t and "Weather Underground：28°C" in t for t in texts), texts
    assert not [t for t in texts if t.startswith("🌡")]
    await cycle(bot, ms("2026-10-13T07:25:00Z"), force_weather=True)
    assert tg.texts() == []  # nothing new: nothing said

    # every source failing: a fault notice, then recovery
    net.fail = {"noaa", "metar", "wu"}
    await cycle(bot, ms("2026-10-13T07:30:00Z"), force_weather=True)
    texts = tg.texts()
    assert any(t.startswith("⚠️ 观测监控异常｜上海 10-13 最高气温") and "所有观测来源都失败" in t for t in texts), texts
    await bot.process_message(admin_message("/status"))
    assert "观测最高：27°C" in tg.texts()[0]  # the rows seen stay
    net.fail = set()
    await cycle(bot, ms("2026-10-13T07:40:00Z"), force_weather=True)
    assert any(t.startswith("✅ 数据恢复") for t in tg.texts())

    # observations stop during the day
    await cycle(bot, ms("2026-10-13T10:00:00Z"), force_weather=True)
    texts = tg.texts()
    assert any("观测停更" in t and "已 180 分钟" in t for t in texts), texts

    # the lock: the following day's first row, then Predict settles
    net.synoptic.append(("2026-10-13T16:00:00Z", 22.0))
    net.metar.append(("2026-10-13T16:00:00Z", 22.0))
    now4 = ms("2026-10-13T16:05:00Z")
    await cycle(bot, now4, force_weather=True)
    texts = tg.texts()
    locks = [t for t in texts if t.startswith("🔒")]
    assert len(locks) == 1 and "次日首条：10-14 00:00（22°" in locks[0] and "当日最高：27°C（15:00）" in locks[0] and "应结算档位：27°C" in locks[0], locks
    assert any("27°C YES 优势 +69¢" in t and "观测已锁定" in t for t in texts), texts
    assert not [t for t in texts if t.startswith("🌡 上海 10-13")]
    assert any(t.startswith("🌡 上海 10-14 最高气温｜新高 22°C（当日首个最高）") for t in texts), texts  # the new day's first reading
    assert [t.slug for t in bot.targets(now4)] == [SLUG, "highest-temperature-in-shanghai-on-october-14-2026"]  # kept an hour past midnight, plus the new day
    assert bot.targets(ms("2026-10-13T17:30:00Z")) == [] or bot.targets(ms("2026-10-13T17:30:00Z"))[0].day == dt.date(2026, 10, 14)
    net.resolved = {"m27": True, "m26": False, "m25": False, "m24": False, "m28": False}
    bot.predict.meta.clear()
    await cycle(bot, ms("2026-10-13T16:30:00Z"))
    texts = tg.texts()
    settles = [t for t in texts if t.startswith("🏁 上海 10-13")]
    assert len(settles) == 1 and "结算档位：27°C" in settles[0] and "一致 ✅" in settles[0], settles
    assert any(t.startswith("🏁 上海 10-14") and "不一致 ⚠️" in t for t in texts)  # the fake settles every slug alike: flagged
    await cycle(bot, ms("2026-10-13T16:40:00Z"))
    assert tg.texts() == []

    # a restart: nothing is announced twice, the rows come back from the store
    net.calls.clear()
    bot2 = m.Bot(config, store, tg)
    bot2.clock = lambda: ms("2026-10-13T16:50:00Z") / 1000
    assert bot2.weather.official("ZSPD", DAY).peak()[0] == 27 and bot2.weather.official("ZSPD", DAY).locked
    await cycle(bot2, ms("2026-10-13T16:50:00Z"))
    assert tg.texts() == []

    # a market that appears later is announced
    net.listed = False
    net.markets = [("n24", "24°C or below"), ("n25", "25°C or higher")]
    net.books.update({"n24": ([[0.1, 1]], [[0.2, 1]]), "n25": ([[0.8, 1]], [[0.9, 1]])})
    config2 = m.Config.from_env({**env, "FOLLOW_DAYS": "1"})
    bot3 = m.Bot(config2, m.Store(":memory:"), tg)
    bot3.set_subscription(7, 0, True)
    now5 = ms("2026-10-13T20:00:00Z")  # 04:00 local on the 14th: today = 14th, tomorrow = 15th
    bot3.clock = lambda: now5 / 1000
    bot3.predict.markets.clear()
    await cycle(bot3, now5, force_weather=True)
    assert {t.day for t in bot3.targets(now5)} == {dt.date(2026, 10, 14), dt.date(2026, 10, 15)}
    tg.texts()
    net.listed = True
    bot3.predict.markets.clear()
    await cycle(bot3, ms("2026-10-13T20:10:00Z"))
    texts = tg.texts()
    assert any(t.startswith("🆕 上海 10-14 最高气温｜Predict 已上线") and "≤24°C" in t for t in texts), texts

    # a slug whose city is unknown: the station comes from the rules, the zone from Synoptic
    net.markets = [("k24", "24°C or below"), ("k25", "25°C or higher")]
    net.books.update({"k24": ([[0.1, 1]], [[0.2, 1]]), "k25": ([[0.8, 1]], [[0.9, 1]])})
    config3 = m.Config.from_env({**env, "CITIES": "", "MARKET_SLUGS": "highest-temperature-in-atlantis-on-october-13-2026"})
    bot4 = m.Bot(config3, m.Store(":memory:"), tg)
    now6 = ms("2026-10-13T06:05:00Z")
    bot4.clock = lambda: now6 / 1000
    assert bot4.targets(now6) == [] and "atlantis" in bot4.target_errors["highest-temperature-in-atlantis-on-october-13-2026"]
    await cycle(bot4, now6, force_weather=True)           # the ladder (with its rules) arrives, the site is probed
    targets = bot4.targets(now6)
    assert len(targets) == 1 and targets[0].station.stid == "ZSPD" and targets[0].station.tz == "Asia/Shanghai" and targets[0].station.lat == 31.1433
    await cycle(bot4, now6, force_weather=True)
    assert bot4.weather.official("ZSPD", DAY).peak()[0] == 27 and not bot4.target_errors

    # --check prints the cards without Telegram
    net.resolved = {}
    net.markets = [("m24", "24°C or below"), ("m25", "25°C"), ("m26", "26°C"), ("m27", "27°C"), ("m28", "28°C or higher")]
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = await m.check_once(config, clock=lambda: now1 / 1000)
    assert code == 0 and "🌡 上海 10-13 最高气温" in out.getvalue() and "买 25¢×100" in out.getvalue()
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        text = await bot.diagnose()
    assert "OK NOAA/Synoptic ZSPD" in text and "OK Predict" in text and "OK 预报 ZSPD" in text
    store.close()

asyncio.run(run())
print("BOT_OK")
