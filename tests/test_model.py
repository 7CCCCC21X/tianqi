"""Fair probabilities, edges, fees; Predict helpers: levels, markets, resolution, books, ladders, the feed."""
import sys, asyncio, datetime as dt, math, time
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401
import main as m

D = m.D
B = m.Bracket.make
br = [B(None, 24), B(25, 25), B(26, 26), B(27, 27), B(28, None)]

# --- the model ----------------------------------------------------------------------------------------------------------------
f = m.fair_probabilities(br, 26, False, 25.8, 1.2, 0.0, 8.0)
assert f.probs[0] == 0 and f.probs[1] == 0 and abs(sum(f.probs) - 1) < 1e-9 and f.probs[2] > 0.6 and f.observed == 26 and not f.locked
assert abs(f.sigma - 1.2 * math.sqrt(0.8)) < 1e-9 and f.mu == 25.8 and "σ" in f.note
f2 = m.fair_probabilities(br, None, False, 26.6, 1.2, 0.0, 20.0)  # the day not started: a normal around the forecast peak
assert abs(sum(f2.probs) - 1) < 1e-9 and f2.probs[3] == max(f2.probs) and f2.sigma == 1.2 and f2.observed is None
f3 = m.fair_probabilities(br, 26, True, None, 1.2, 0.0, 0)  # locked
assert f3.probs == (0, 0, 1, 0, 0) and f3.locked
assert m.fair_probabilities(br, None, True, None, 1.2, 0.0, 0) is None and m.fair_probabilities(br, None, False, None, 1.2, 0.0, 5) is None
f4 = m.fair_probabilities(br, 27, False, None, 1.2, 0.0, 2.0)  # no forecast left: the floor stays, little chance to rise
assert f4.probs[3] > 0.9 and abs(sum(f4.probs) - 1) < 1e-9 and f4.probs[4] < 0.1 and "无剩余" in f4.note
f5 = m.fair_probabilities(br, 24, False, 23.0, 1.2, 0.0, 0.5)  # late in the day: σ floors at 0.3 × 1.2
assert abs(f5.sigma - 0.36) < 1e-9 and f5.probs[0] > 0.99
f6 = m.fair_probabilities(br, 26, False, 25.8, 1.2, 1.0, 8.0)  # the bias shifts the mean
assert f6.mu == 26.8 and f6.probs[3] > f.probs[3]
brf = [B(None, 76, "F"), B(77, 78, "F"), B(79, 80, "F"), B(81, None, "F")]
f7 = m.fair_probabilities(brf, 79, False, 26.0, 1.2, 0.0, 8.0, "F")
assert f7.probs[0] == 0 and f7.probs[1] == 0 and abs(f7.mu - 78.8) < 1e-9 and abs(f7.sigma - 1.2 * math.sqrt(0.8) * 1.8) < 1e-9 and "°F" in f7.note
f8 = m.fair_probabilities([B(25, 25), B(27, 27)], None, False, 26.0, 0.3, 0.0, 10.0)  # a ladder with a gap: scaled to what it covers only when over 1
assert sum(f8.probs) < 1
f9 = m.fair_probabilities([B(None, 30), B(20, None)], None, False, 25.0, 1.0, 0.0, 10.0)  # overlap: scaled down to 1
assert abs(sum(f9.probs) - 1) < 1e-9

# --- edges ---------------------------------------------------------------------------------------------------------------------
assert abs(m.taker_fee(0.2, 200) - 0.004) < 1e-12 and abs(m.taker_fee(0.9, 200) - 0.002) < 1e-12 and m.taker_fee(0.5, 0) == 0
book = m.PredictBook("m25", ((D("0.20"), D("50")),), ((D("0.25"), D("50")),), 0)
edges = {e.side: e for e in m.level_edges(1, 0.0, book, 200, True)}
assert abs(edges["NO"].edge - (0.20 - 0.004)) < 1e-9 and abs(edges["NO"].price - 0.8) < 1e-9 and edges["NO"].fair == 1.0 and edges["NO"].sure
assert abs(edges["YES"].edge - (0.0 - 0.25 - 0.005)) < 1e-9 and edges["YES"].key == "1:YES"
edges = {e.side: e for e in m.level_edges(4, 0.179, m.PredictBook("m28", ((D("0.03"), D(1)),), ((D("0.06"), D(1)),), 0), 200, False)}
assert abs(edges["YES"].edge - (0.179 - 0.06 - 0.0012)) < 1e-9 and not edges["YES"].sure
assert m.level_edges(0, 0.5, m.PredictBook("x", (), (), 0), 200, False) == []
assert [e.side for e in m.level_edges(0, 0.5, m.PredictBook("x", (), ((D("0.5"), D(1)),), 0), 200, False)] == ["YES"]

# --- Predict helpers ------------------------------------------------------------------------------------------------------------
bids = m.predict_levels([["0.80", "400"], ["0.81", "110.5"], {"price": 0.79, "size": 100}, ["0.5", "0"], ["x", "1"], ["1.2", "3"]], True)
assert bids == ((D("0.81"), D("110.5")), (D("0.80"), D("400")), (D("0.79"), D("100"))), bids
assert m.predict_levels([["0.99", "2000"], ["0.85", "12"]], False)[0] == (D("0.85"), D("12")) and m.predict_levels("x", True) == ()
assert m.predict_markets({"data": {"markets": {"edges": [{"node": {"id": 7, "conditionId": "0xab", "title": "26°C", "question": "q"}}]}}}) \
    == [{"id": "7", "conditionId": "0xab", "title": "26°C", "question": "q"}]
assert m.predict_markets({"data": {"markets": [{"id": "1", "question": "Will it?", "title": "A"}, {"id": "1", "question": "dup", "title": "A"}]}}) == \
    [{"id": "1", "conditionId": "", "title": "A", "question": "Will it?"}]
assert m.predict_markets({"id": 1, "title": "x"}) == []
assert m.predict_resolution({"outcomes": [{"name": "Yes", "indexSet": 1, "status": "WON"}, {"name": "No", "indexSet": 2, "status": "LOST"}]}) == \
    {"index": 0, "name": "Yes", "split": False, "how": "outcomes.status"}
assert m.predict_resolution({"outcomes": [{"name": "Yes", "indexSet": 1}, {"name": "No", "indexSet": 2}], "status": "OPEN"}) is None
assert m.predict_resolution({"outcomes": [{"name": "Yes", "indexSet": 1, "payout": 0}, {"name": "No", "indexSet": 2, "payout": 1}]})["name"] == "No"
assert m.predict_resolution({"outcomes": [{"name": "Yes", "indexSet": 1}, {"name": "No", "indexSet": 2}], "status": "RESOLVED", "winningOutcome": {"name": "No"}})["index"] == 1
assert m.predict_resolution({"outcomes": [{"name": "Yes", "status": "WON"}, {"name": "No", "status": "WON"}]})["split"]

flipped = book.flipped()
assert flipped.bids == ((D("0.75"), D("50")),) and flipped.asks == ((D("0.80"), D("50")),) and flipped.bid == D("0.75")
assert book.text() == "买1 20¢/卖1 25¢" and m.PredictBook("x", (), (), 0).text() == "买1 —/卖1 —"
assert book.stale(m.PREDICT_STALE_MS + 1) and not book.stale(1000)

lv = m.Level(B(25, 25), "m25", "25°C", book=book, meta={"outcomes": ["Yes", "No"], "status": "OPEN", "trading_status": "ACTIVE"})
assert lv.yes_book()[0] is book and not lv.settled and not lv.closed and lv.resolved_yes() is None
lv_no_first = m.Level(B(25, 25), "m25", "25°C", book=book, meta={"outcomes": ["No", "Yes"]})
assert lv_no_first.yes_book()[0].bid == D("0.75")
assert m.Level(B(25, 25), "m25", "25°C", book=book, meta={"outcomes": ["Up", "Down"]}).yes_book()[0] is None
assert m.Level(B(25, 25), "m25", "25°C", book=book, meta={}).yes_book()[1].startswith("结果顺序未取得")
assert m.Level(B(25, 25), "m25", "25°C", error="HTTP 404").yes_book() == (None, "HTTP 404")
won = m.Level(B(27, 27), "m27", "27°C", meta={"outcomes": ["Yes", "No"], "status": "RESOLVED",
                                             "resolved": {"index": 0, "name": "Yes", "split": False, "how": "outcomes.status"}})
lost = m.Level(B(26, 26), "m26", "26°C", meta={"outcomes": ["Yes", "No"], "status": "RESOLVED",
                                              "resolved": {"index": 1, "name": "No", "split": False, "how": "outcomes.status"}})
assert won.settled and won.closed and won.resolved_yes() is True and lost.resolved_yes() is False
assert m.Level(B(26, 26), "m26", "26°C", meta={"status": "OPEN", "trading_status": "CLOSED"}).closed
ladder = m.Ladder("slug", sorted([won, lost, lv], key=lambda l: l.bracket))
assert [l.bracket.label for l in ladder.levels] == ["25°C", "26°C", "27°C"] and ladder.level_for(27) is won and ladder.level_for(30) is None
assert ladder.winner() is won and ladder.any_settled and ladder.unit == "C" and ladder.rules == ""
assert m.Ladder("s", [], listed=False).unit == "C"


# --- the feed against a fake API --------------------------------------------------------------------------------------------------
class Net:
    def __init__(self):
        self.listed = True
        self.graphql_down = False
        self.rest_markets = None
        self.calls = []
        self.books = {"m25": {"bids": [["0.20", "50"]], "asks": [["0.25", "50"]]}, "m26": {"bids": [[0.6, 100]], "asks": [[0.65, 100]]}}
        self.details = {"m25": {"outcomes": [{"name": "Yes", "indexSet": 1}, {"name": "No", "indexSet": 2}], "status": "OPEN", "feeRateBps": 150,
                                "description": "in degrees Celsius ... https://www.weather.gov/wrh/timeseries?site=zspd"},
                        "m26": {"outcomes": [{"name": "No", "indexSet": 1}, {"name": "Yes", "indexSet": 2}], "status": "OPEN"}}
        self.markets = [{"id": "m25", "conditionId": "0x25", "title": "25°C", "question": "Will ... be 25°C"},
                        {"id": "m26", "conditionId": "0x26", "title": "26°C", "question": ""},
                        {"id": "mx", "conditionId": "0xx", "title": "Something else", "question": "No temperature here"}]

    async def http_json(self, url, payload=None, timeout=15, headers=None):
        self.calls.append((url, payload, headers))
        if "graphql" in url:
            if self.graphql_down:
                raise m.RemoteError("HTTP 502: bad gateway")
            if "category(id" in payload["query"]:
                return {"data": {"category": {"id": "cat1", "__typename": "WeatherCategory"} if self.listed else None}}
            return {"data": {"markets": {"edges": [{"node": mk} for mk in self.markets]}}}
        if "/categories/" in url:
            if self.rest_markets is None:
                raise m.RemoteError("HTTP 404: Not Found")
            if self.rest_markets == "boom":
                raise m.RemoteError("HTTP 500: broken")
            return {"data": {"markets": self.rest_markets}}
        if url.endswith("/orderbook"):
            key = url.rsplit("/", 2)[-2]
            if key not in self.books:
                raise m.RemoteError("HTTP 404: no book")
            return {"data": self.books[key]}
        if "/markets/" in url:
            key = url.rsplit("/", 1)[-1]
            if key not in self.details:
                raise m.RemoteError("HTTP 404")
            return {"data": {"id": key, **self.details[key]}}
        raise AssertionError(url)


async def run():
    net = Net()
    m.http_json = net.http_json
    config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "PREDICT_API_KEY": "k123"})
    feed = m.PredictFeed(config)
    now = int(time.time() * 1000)
    net.books["m25"] = {"bids": [["0.20", "50"]], "asks": [["0.25", "50"]]}
    del net.books["m26"]
    net.books["0x26"] = {"bids": [[0.6, 100]], "asks": [[0.65, 100]]}  # this market's book answers only to its conditionId
    assert await feed.refresh(["slug"], now, force=True)
    ladder = feed.ladders["slug"]
    assert ladder.listed and not ladder.error and [lv.bracket.label for lv in ladder.levels] == ["25°C", "26°C"]
    assert ladder.rules.startswith("in degrees") and ladder.unit == "C"
    l25, l26 = ladder.levels
    assert l25.meta["fee_bps"] == 150 and l25.book.bid == D("0.20") and l25.yes_book()[0] is l25.book
    assert l26.yes_book()[0].bid == D("0.35") and feed.book_keys["m26"] == "0x26"  # No first: flipped; the key that answered is remembered
    rest_headers = [h for u, p, h in net.calls if u.startswith(m.PREDICT_REST)]
    assert rest_headers and all(h == {"x-api-key": "k123"} for h in rest_headers) and all(h == {} for u, p, h in net.calls if "graphql" in u)
    assert not await feed.refresh(["slug"], now)  # not due
    # a book failure keeps the previous book and notes the error
    net.books.pop("m25")
    await feed.refresh(["slug"], now, force=True)
    l25 = feed.ladders["slug"].levels[0]
    assert l25.book is not None and l25.book.bid == D("0.20") and "HTTP 404" in l25.error
    # settled: no book is asked for
    net.details["m25"]["outcomes"][0]["status"] = "WON"
    net.details["m25"]["status"] = "RESOLVED"
    feed.meta.clear()
    net.calls.clear()
    await feed.refresh(["slug"], now, force=True)
    l25 = feed.ladders["slug"].levels[0]
    assert l25.settled and l25.resolved_yes() is True and l25.book is None and not any(u.endswith("m25/orderbook") for u, _, _ in net.calls)
    assert feed.ladders["slug"].winner() is l25
    # not listed: an answer, cached for a while
    net.listed = False
    feed.markets.clear()
    await feed.refresh(["other"], now, force=True)
    assert not feed.ladders["other"].listed and "slug" not in feed.ladders
    # GraphQL down but REST answers 404: REST's answer counts, the category is not listed
    net.graphql_down = True
    feed.markets.clear()
    await feed.refresh(["slug"], now, force=True)
    assert not feed.ladders["slug"].listed and not feed.ladders["slug"].error
    # GraphQL down and REST failing too: unknown, not "not listed"
    net.rest_markets = "boom"
    feed.markets.clear()
    await feed.refresh(["slug"], now, force=True)
    assert feed.ladders["slug"].error.startswith("HTTP 502") and feed.ladders["slug"].listed
    # GraphQL down, REST answers
    net.rest_markets = net.markets
    feed.markets.clear()
    await feed.refresh(["slug"], now, force=True)
    assert not feed.ladders["slug"].error and len(feed.ladders["slug"].levels) == 2
    # 429 pauses every request
    net.graphql_down = False
    feed.markets.clear()
    feed.blocked_until = time.monotonic() + 30
    await feed.refresh(["slug"], now, force=True)
    assert "限流" in feed.ladders["slug"].error
    feed.blocked_until = 0

asyncio.run(run())
print("MODEL_OK")
