# 测试

全部测试离线运行：`tests/offline.py` 会让任何真实 HTTP 请求立即失败，数据源一律用固定样本替代（Synoptic、aviationweather.gov、
Weather Underground、Open-Meteo、Predict.fun GraphQL/REST 的返回格式）。

```bash
python tests/run_all.py          # 全部套件
python tests/run_all.py bot      # 只跑 tests/test_bot.py
```

| 套件 | 覆盖 |
| --- | --- |
| `test_parse.py` | 档位标题解析（°C/°F、或以下/或以上、区间、中文）、slug、站点配置、Config 校验、时区、整数取整、日期参数、脱敏 |
| `test_sources.py` | 三个观测来源的解析、本地日切片与次日首条锁定、读数修正识别、刷新失败保留旧读数、持久化与重启恢复、来源优先级 |
| `test_model.py` | 模型概率（已观测下限、锁定、无预报、°F、σ 收缩）、净优势与手续费、Predict 辅助函数、订单簿方向、结算识别、Feed 对假接口的行为 |
| `test_bot.py` | 机器人端到端：新高/档位、优势、修正、来源不一致、数据异常与恢复、停更、锁定、结算、重启不重复、市场上线、规则站点自动识别、命令、`--check` |

Docker 构建时会先跑全部套件（见 `Dockerfile`），失败则构建失败，不会把坏版本部署上去。

真实数据源只能在线核对：

```bash
python main.py --diag    # 逐个请求 Synoptic / METAR / WU / Open-Meteo / Predict，打印结果；不需要 TG Token
python main.py --check   # 刷新一次并打印各市场状态卡（含盘口深度）；不发送 TG
```
