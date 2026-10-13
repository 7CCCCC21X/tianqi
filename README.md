# TG 天气市场监控机器人（Predict.fun 最高气温盘）

监控 Predict.fun 上「**某城市某日最高气温**」这一类市场，例如 [10月13日上海最高气温？](https://predict.fun/zh-cn/market/highest-temperature-in-shanghai-on-october-13-2026)：

- 读取市场结算所依据的 **NOAA 观测数据**（上海浦东机场 ZSPD），实时计算当日最高气温落在哪个档位；
- 读取 Predict 各档位（22°C 或以下 … 28°C 或以上）的**订单簿**，和观测、预报对照；
- 当日**新高 / 档位变化、次日首条观测出现（市场锁定）、读数被修正、来源不一致、数据异常、模型优势、市场上线 / 结算**时推送 Telegram。

代码存放 GitHub，Railway 常驻运行。**仅监控**，不会自动交易、下单或撤单，不需要钱包、私钥或交易所 API Key。结构参考 [tg-close-alert-bot](https://github.com/7CCCCC21X/tg-close-alert-bot)：单文件 `main.py`，只用 Python 标准库，SQLite 保存状态。

## 先看懂市场规则

以上海市场为例（规则原文摘要）：

> 结算为 NOAA 在上海浦东国际机场站 **当日** 记录的最高气温（整数 °C）所在的区间。结算源是 NOAA 时间序列页
> `https://www.weather.gov/wrh/timeseries?site=zspd` 上 "Temp" 一列当天所有时刻中的最高读数。若次日美东 23:59 前 NOAA 仍无数据，改用
> Weather Underground 的 Daily Observations 表；两者都没有则结算到最低档。**次日第一条数据发布后**（或次日美东 23:59，取先到者）市场结算；
> 在此之前对当日读数的修正有效，之后的修改不再考虑。

机器人据此采用的口径：

| 项目 | 口径 |
| --- | --- |
| "当日" | **站点本地日期**（ZSPD 为 Asia/Shanghai，00:00–24:00），不是 UTC，也不是美东 |
| 读数 | 页面显示的整数：读数按四舍五入（半数进位）取整后比较；ZSPD 的 METAR 本来就是整数 °C |
| 当日最高 | 当日所有读数（含 SPECI 特殊观测）的最大值，第一次达到该值的时刻作为"观测时刻" |
| 锁定 | 出现**次日本地日期的第一条观测**（通常 00:00 的 METAR）即视为锁定，推送 🔒 并给出应结算档位 |
| 修正 | 已发布读数的数值变化、读数消失，推送 ✏️；最高值被下调推送 ⚠️ |
| 单位 | 从市场规则里读 degrees Celsius / Fahrenheit；°F 市场按每条读数换算后取整再取最高（与页面切换单位的方式一致） |

NOAA 页面本身是浏览器应用，没有可直接读取的数据接口；它的表格来自 **Synoptic Data 的站点 API**，本程序直接读同一接口。

## 数据源

| 键 | 来源 | 角色 | 默认频率 |
| --- | --- | --- | --- |
| `noaa` | Synoptic Data `stations/timeseries`（NOAA/NWS 时间序列页的数据来源） | 结算源同源数据 | 每 600 秒（免费 token 每月 5,000 次） |
| `metar` | NOAA 航空气象中心 aviationweather.gov 的 METAR 接口 | 同一批 METAR/SPECI 报文，无需 token，**每轮都读**，用于最快发现新读数 | 每 60 秒 |
| `wu` | Weather Underground 历史页的数据接口（api.weather.com） | 规则指定的备用结算源，用于对照 | 每 900 秒 |
| 预报 | Open-Meteo 逐小时气温（免费，无 key） | 只用于模型概率，不参与结算判断 | 每 30 分钟 |
| 盘口 | Predict.fun GraphQL + REST（只读） | 各档位订单簿、结果顺序、规则文本、结算状态 | 每 20 秒 |

推送所依据的"当日数据"：`noaa` 和 `metar` 是同一份报文，谁的最新观测更新就用谁（相同时以 `noaa` 为准）；两者都没有数据时才用 `wu`。几个来源的当日最高不一致时会单独推送 ⚠️ 提醒。

`SYNOPTIC_TOKEN` 留空时使用 Synoptic 文档里的公开演示 token，可能随时失效，请到 [synopticdata.com](https://synopticdata.com/weatherapi/) 免费申请。即使 Synoptic 不可用，`metar` 通道仍能工作（同一份 METAR 报文）。

## 推送内容

| 图标 | 触发 | 内容 |
| --- | --- | --- |
| 🌡 | 当日最高进入另一个档位（含当日第一条读数；升温早晨 22→23→24 都在"24°C 或以下"里时不重复发） | 新高、观测时刻与来源、当前档位盘口与模型概率、已不可能的档位、剩余时段预报峰值 |
| 🔒 | 次日首条观测发布 | 当日最高、应结算档位及其盘口；此后不再接受修正 |
| ✏️ | 已发布读数被修正或消失 | 修改前后数值 |
| ⚠️ | 最高被下调 / 各来源不一致 / 数据异常（所有来源失败、白天停更超 `OBS_STALE_MINUTES`） | 异常持续 3 分钟才首次提示，之后最多每 30 分钟一次；恢复后发 ✅ |
| 💡 | 某档位某一方向的净优势 ≥ `EDGE_ALERT_CENTS` 并持续 `EDGE_ALERT_CONFIRM_SECONDS` | 吃单价、模型价、是"观测已定"还是"模型估计" |
| 🆕 / 🏁 | 跟踪的市场在 Predict 上线 / 结算 | 档位列表与盘口 / 结算档位是否与观测一致 |

每条提醒都带市场、NOAA 页面和 Weather Underground 页面链接。同一提醒每个聊天只发一次（SQLite 记录），重启不会重复。

## 命令

只有 `ADMIN_USER_ID` 指定的管理员可用（非管理员只能用 `/id`、`/help`）。

| 命令 | 作用 |
| --- | --- |
| `/subscribe` `/unsubscribe` `/pause` `/resume` | 在当前私聊或群组话题订阅 / 取消 / 暂停 / 恢复提醒（管理员私聊会自动订阅） |
| `/status` | 每个跟踪市场一张卡：观测最高、各来源状态、预报、模型、各档位买1/卖1、优势 |
| `/obs [城市] [日期]` | 当日逐条读数（★ 为最高），例 `/obs shanghai 10-13`、`/obs 上海 明天` |
| `/book [城市] [日期]` | 各档位三档深度与净优势 |
| `/forecast [城市] [日期]` | 逐小时预报与模型参数 |
| `/markets` | 跟踪的市场与链接 |
| `/edge 8` / `/edge off` / `/edge on` | 优势提醒门槛（¢）与开关 |
| `/diag` | 逐个请求数据源并报告 |
| `/test` `/id` `/help` | 测试消息 / 查看 ID / 说明 |

命令行：`python main.py --diag` 检测数据源，`python main.py --check` 刷新一次并打印状态卡，两者都不需要 Telegram Token。

## 跟踪哪些市场

- `CITIES=shanghai`：按 slug 模板 `highest-temperature-in-{city}-on-{month}-{day}-{year}` 自动跟踪**今天和之后 `FOLLOW_DAYS` 天**（按站点本地日期），昨天的市场在本地午夜后再保留 `KEEP_HOURS` 小时以等待结算。
- `MARKET_SLUGS=...`：手动追加任意 slug 或市场链接。城市不在配置里时，机器人从市场规则里读 `site=xxxx` 站号，并向 Synoptic 询问站点时区、经纬度后开始跟踪。
- `STATIONS=city=站号:时区:国家码:纬度,经度`：覆盖或新增城市站点。内置 shanghai（ZSPD）、hong-kong（VHHH）、beijing（ZBAA）、tokyo（RJTT）、singapore（WSSS）、taipei（RCTP）；规则里的站号与配置不一致时 `/status` 会标出。

## 模型概率与优势（仅供参考）

当日最高 = max(已观测最高, 剩余时段峰值)。剩余时段峰值取 Open-Meteo 剩余小时预报的最大值加 `FORECAST_BIAS`，误差按正态分布，σ 从 `FORECAST_SIGMA`（默认 1.2°C）随剩余时间缩短而收缩（最低 0.3×）。于是：

- 已观测最高之下的档位概率为 0（"观测已定"）；
- 锁定后当前档位概率为 1；
- 其余档位按整数取整边界（k±0.5）积分。

净优势 = 模型价 − 吃单价 − 手续费，手续费按 Predict 的 `feeRateBps × min(p, 1−p)`（市场未声明时用 `PREDICT_FEE_BPS`）。💡 提醒只是提示：预报有误差、盘口可能很薄、规则可能有特殊情形，请自行核实。

## 部署到 Railway

1. Telegram 里找 @BotFather 建机器人，拿到 Token。
2. Railway 新建项目 → Deploy from GitHub repo → 选本仓库（`railway.json` 指定用 `Dockerfile` 构建；构建时会跑离线测试）。
3. 添加 Volume，挂载到 `/data`（SQLite 状态文件）。
4. Variables 里填 `TELEGRAM_BOT_TOKEN`、`ADMIN_USER_ID=0`，部署后给机器人发 `/id`，把用户 ID 填回 `ADMIN_USER_ID` 再部署。
5. 申请 Synoptic token 填到 `SYNOPTIC_TOKEN`（可选但建议）。

全部变量见 `.env.example`；`railway-variables.example.txt` 是最小集合。同一个 Bot Token 只能跑一个实例（否则 Telegram 轮询冲突）。

本地运行：`cp .env.example .env`，填好后 `set -a; source .env; set +a; python main.py`（Python 3.12+，无需安装依赖）。

## 测试

```bash
python tests/run_all.py
```

全部离线（见 `TESTING.md`）：观测解析、本地日切片与锁定、修正识别、模型、Predict 接口行为、机器人端到端提醒流程与命令。

## 限制与注意

- 本程序读的是 NOAA 页面背后的 Synoptic 接口和 NOAA 的 METAR 接口，不是页面本身；页面若有人工修订或显示差异，以页面和 Predict 的实际结算为准。
- Synoptic 免费 token 每月 5,000 次请求，默认每 10 分钟读一次；想更快请调 `NOAA_POLL_SECONDS`，并留意配额。演示 token 不保证可用。
- Weather Underground 使用其网页内嵌的公开 key，可能变化（`WU_API_KEY` 可覆盖）；它只做对照和备用。
- Predict 的 REST 接口若返回 401/403，需要在 Discord 申请 API key 填到 `PREDICT_API_KEY`。
- 不构成任何投资建议。
