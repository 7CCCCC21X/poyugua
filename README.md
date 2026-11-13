# Polymarket 5 分钟涨跌 · 发布即预挂机器人

参考 [huice](https://github.com/7CCCCC21X/huice) 的「15 分钟市场发布即预挂」（`preopen.py` / `PREOPEN.md`）改写到 Polymarket：
每个 5 分钟涨跌市场一发布，就在**涨、跌两边各挂一张 post-only 限价买单**（默认 49¢ × 10 份），
排在队列前面；**开场前撤单**，订单自带 GTD 到期时间兜底。两边都成交 = 一对成本 98¢、到期必得 $1 的无风险组合；
只成交一边 = 49¢ 买到一个五五开的结果。

首批市场（`PREOPEN_MARKETS=all`，默认）：

| 币种 | 链接规律（`<开始时间的 Unix 秒>`，5 分钟一场） | 例子 |
| --- | --- | --- |
| BTC | `btc-updown-5m-<t>` | https://polymarket.com/zh/event/btc-updown-5m-1794536700 |
| ETH | `eth-updown-5m-<t>` | https://polymarket.com/zh/event/eth-updown-5m-1794536700 |
| SOL | `sol-updown-5m-<t>` | https://polymarket.com/zh/event/sol-updown-5m-1794537300 |
| XRP | `xrp-updown-5m-<t>` | https://polymarket.com/zh/event/xrp-updown-5m-1794536700 |
| DOGE | `doge-updown-5m-<t>` | https://polymarket.com/zh/event/doge-updown-5m-1794536700 |
| HYPE | `hype-updown-5m-<t>` | https://polymarket.com/zh/event/hype-updown-5m-1794536700 |
| BNB | `bnb-updown-5m-<t>` | https://polymarket.com/zh/event/bnb-updown-5m-1794536700 |

`t` 是这一场的开始时间（UTC 秒，300 的整数倍），例如 `1794536700` = 北京时间 11-13 10:25（美东 11-12 9:25PM），
结束于 `t + 300`。机器人不需要你逐场添加：它按规律算出接下来每一场的链接，发布了就挂。

## 实测的市场规律（2026-11-13）

- **发布时间**：每一场在开场前约 **23 小时 52.5 分**创建（`createdAt`，中位 85,948 秒），七个币在同一秒内；偶尔晚一两分钟。
- **创建 ≠ 能下单**：刚创建的市场 `active=false`、`acceptingOrders=null`，交易所不接单；约 **75～90 秒后才激活**
  （`active`、`acceptingOrders` 和 CLOB 的接单标志 `ao` 同时变成 true）。`acceptingOrdersTimestamp` 字段写的是创建后约 6 秒，
  是事后回填的，不能信。激活后 1 秒内盘口上就有人挂了 200 多份（BTC：激活后 1 秒 50¢ 230 份；10 秒后 49¢ 281 份；30 秒后 482 份），
  所以真正的比赛起点是「激活那一刻」。
- **两种查法差 88 秒**：`GET /markets/slug/<slug>`（按路径）在创建后 **0.5 秒**就能查到，字段齐全（conditionId、两个代币、开始结束时间）；
  而批量查询 `GET /markets?slug=...`（以及 `/events?slug=`）只返回已激活的市场，要等到激活后才出现。机器人用路径接口盯每个币的下一场，
  提前约 80 秒拿到代币、预填 SDK 的市场缓存（签名不再联网），创建后 55 秒起每 0.2 秒问一次 CLOB 是否接单，一开放七个币并行下单。
- **Gamma 有 5 分钟缓存**：`/markets` 的应答带 `cache-control: max-age=300`，CloudFront 会把「还没发布」的空结果缓存
  最多 5 分钟（实测 `x-cache: Hit, age: 264` 时市场已发布 4 分钟仍返回 `[]`）。机器人每次请求都带一个独一无二的参数绕开缓存。
- **结算**：Chainlink 60 秒 TWAP（开场价和结算价都是 TWAP），开场后约 80 秒出结果（`outcomePrices` 变成 `["1","0"]` 或 `["0","1"]`）。
  开场前几分钟盘口就会因为行情偏离五五开（实测开场前 5 分钟 BTC 涨被买到 55¢），越接近开场成交越可能是「知道方向的人」吃你的单，
  所以默认**开场前 60 秒撤单、30 秒到期**，比 huice 的 20/10 秒保守。
- **手续费**：这些市场只收吃单方手续费（`feeSchedule.takerOnly=true`，费率 0.07），post-only 挂单永远是挂单方，不付手续费，
  还可能分到挂单返佣（taker 手续费的 20%）。若某个市场改成挂单方也收费，机器人不挂它。
- **流动性奖励**：这些市场 `rewardsMinSize=50`、`rewardsMaxSpread=4.5`¢：每边 ≥50 份、离中间价 4.5¢ 以内的挂单可能计入奖励。
- **最小单位**：价位 0.01（价格接近 0/1 时变 0.001），最小 5 份。

## 和 huice（Predict.fun）版本的关键不同：挂单会占用资金

Polymarket 的挂买单**会占用 pUSD 余额**：`可下单量 = 余额 − Σ(挂单份数 − 已成交) × 价格`。
默认 49¢ × 10 份，一场两边占 $9.80；七个币一天 288 × 7 ≈ 2016 场，**全天候发布即挂要约 $19,800**。资金不够时：

- 机器人按「开场最近的先」分配资金，余额（或 `PREOPEN_MAX_OPEN_USD`）用完就不再新挂，等有场次撤单释放后再挂下一场；
- 想保住「发布即挂」的排队优势，就减少币种或份数，或把 `PREOPEN_AHEAD_MINUTES` 调小（比如 240 = 只挂 4 小时内开始的，
  但那样就不是发布那一刻挂的了，排在别人后面）。`python main.py probe` 会算出当前设置全挂上要多少钱。

## 规则

| 项目 | 行为 |
| --- | --- |
| 订单 | 两边各一张 **BUY 限价、post-only、GTD**：会吃单的直接被交易所拒绝，不会主动成交 |
| 价格与份数 | 第一档 `PREOPEN_PRICE_CENTS` × `PREOPEN_SHARES`；`PREOPEN_LEVELS=48x20,47x40` 在下面再挂更深的档，`PREOPEN_DEEP_AHEAD_MINUTES` 限定深档只在开场前多久才挂 |
| 范围 | 只挂 `PREOPEN_AHEAD_MINUTES`（默认 1440）分钟内开始的场次；最新发布的最先挂，其余按开场时间最近的先 |
| 新单截止 | 距开场不足 `PREOPEN_ENTRY_SECONDS`（默认 300）秒不再新挂 |
| 撤单 | 开场前 `PREOPEN_CANCEL_SECONDS`（默认 60）秒按订单号撤；没撤掉的每 2 秒再撤；有状态不明的单时按整个市场撤一遍 |
| 到期兜底 | 订单在开场前 `PREOPEN_EXPIRY_SECONDS`（默认 30）秒自动失效（GTD 的到期时间写成 `开场 − 到期秒数 + 60`，交易所提前 60 秒失效） |
| 单边异常 | 一边被拒、没发出或结果不明，另一边立刻撤掉；之后这一档重新报价（换新订单），不重签补单 |
| 重新报价 | 开场前被撤掉、没有成交的档位会重新报价：因自身原因失败的（单边被拒、挂单被别人撤掉）每档最多 `PREOPEN_MAX_ATTEMPTS` 次，间隔 60 秒；改设置、暂停、限流、市场还没开放、余额不足这类不算次数。有成交的、被交易所判定为无效订单的不再报 |
| 结果不明 | 下单请求超时等「不知道挂没挂上」的单：先用本地算出的订单哈希查（订单号 = EIP-712 订单哈希），再到挂单列表里按 代币/价格/份数 认领；到点撤单时按市场整体撤一遍；签名到期后还找不到的视为没挂上 |
| 账户快照 | 每 10 秒读一次全部挂单和余额：发现成交（通知）、发现挂单不见了（读最终记录）、算资金占用；快照过期就暂停新挂 |
| 限流 | 本地令牌桶（下单 30/秒、撤单 60/秒，低于 Standard 档位的 40/80）；交易所回 429、撮合引擎重启（425）、只许撤单时整体冷却，按服务器给的时间重试 |
| 重启、改设置 | 挂单留在簿上（到期自动失效），新进程按账本接管：和当前设置一致的原样保留（排队位置不丢），不一致的撤掉按新设置重报。`PREOPEN_CANCEL_ON_EXIT=on` 则退出时撤单 |
| 结算与领取 | 窗口结束约 1 分钟后读官方结果算盈亏；实盘且 `PREOPEN_AUTO_REDEEM=on` 时自动领取赢的一边（智能钱包需要 Relayer Key；EOA 钱包需要 POL 付 gas；失败的 10 分钟后再试，最多 6 次，也可以在网页上手动领取） |
| 暂停 | `/pause`：不再新挂，撤掉本程序的全部挂单（状态持久保存，重启后仍暂停）；`/resume` 恢复 |

## 部署（Railway）

1. 把本仓库连到 Railway（Deploy from GitHub repo），使用仓库里的 `Dockerfile` 和 `railway.json`，启动命令 `python -u main.py`。
   这是常驻进程：**不要配健康检查和 Cron**。构建时会跑全部离线测试，测试不过构建失败。
   **地区必须选 EU West（Amsterdam）**：Polymarket 对美国、新加坡、英国、法国、德国、澳大利亚等地的 IP 在 API 上也只许平仓、不许开新单
   （Railway 的 US West / US East / Southeast Asia 区都下不了单）；荷兰只限制网页、不限制 API。Polymarket 的服务器在 AWS 伦敦（eu-west-2），
   Amsterdam 到那里延迟最低。实盘启动时机器人会查一次 `polymarket.com/api/geoblock` 并在日志/Telegram 里报出所在国家，受限就告警。
2. 给服务挂一个 **Volume 到 `/data`**（账本 `/data/preopen.sqlite3` 必须在持久卷上，否则重启后不认识自己挂着的单）。只运行一个副本。
3. 先不填私钥、保持 `LIVE=off` 跑模拟盘，看日志或 Telegram `/status`：每场发布后几秒内是否「挂上」、开场前是否撤掉。
4. 准备实盘账户（建议单独开一个账户给机器人）：
   - `POLY_WALLET`：polymarket.com 右上角个人菜单里显示的钱包地址；
   - `POLY_PRIVATE_KEY`：这个账户的签名私钥（邮箱/Google 登录的账户可在网页设置里导出；MetaMask 等外部钱包就是那个钱包的私钥），
     或者一把已授权给这个 Deposit Wallet 的 **Session Key**（只能交易、不能提现，更安全；但可能不能自动领取，领取可在网页上做）；
   - 自动领取：在 设置 → API Keys → Relayer API Keys 建一个 Key，填 `POLY_RELAYER_API_KEY` 和 `POLY_RELAYER_API_KEY_ADDRESS`；
   - 账户里要有 **pUSD**，并在网页上至少交易过一次或完成交易授权（SDK 也会在缺授权时尝试补上）。
5. 先在本地或 Railway 上跑 `python main.py probe`（只读，不需要私钥）确认市场都找得到、资金需求，再设 `LIVE=on`。

## 本地运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # 按需修改
python main.py probe 1          # 只读探测：接下来 1 小时的场次、发布规律、盘口、资金需求
python -u main.py               # 运行（LIVE=off 时是模拟盘）
python main.py status           # 看本地账本的记录数和盈亏
python tests/run_all.py         # 离线测试（不联网、不需要私钥）
```

## Telegram（可选）

填 `TELEGRAM_BOT_TOKEN` 和 `TELEGRAM_CHAT_ID` 后，机器人会把启动、成交、结算、问题发到这个聊天，并接受命令：

- `/status`（或 `/preopen`）：挂着多少场多少张、资金占用和余额、已发布到哪一场、下一场预计几点发布、开放接单→挂上用时、今天的成交和盈亏、不新挂的原因、最近的问题
- `/check`：体检（快照多久前、冷却、队列、状态不明的单、订单号是否等于哈希、限流档位、学到的发布提前量）
- `/probe [小时]`：只读探测
- `/pause` / `/resume`

## 代码结构

| 文件 | 作用 |
| --- | --- |
| `main.py` | 入口：`run` / `probe` / `status` |
| `preopen/config.py` | 读环境变量并校验（价格不超过 50¢、GTD 的 3 分钟规则、撤单早于到期等） |
| `preopen/markets.py` | 链接规律、范围内的场次、Gamma 市场字段的校验（涨/跌按名字对应代币） |
| `preopen/gamma.py` | 公共读接口：Gamma 批量查链接（绕缓存）、CLOB 是否接单、盘口 |
| `preopen/exchange.py` | 交易所适配：实盘走官方统一 SDK `polymarket-client`（签名、15 张一批下单、按单号/按市场撤单、挂单列表、余额、领取），模拟盘在内存里 |
| `preopen/engine.py` | 控制器：发现、下单队列、到点撤单、核对、账户快照、结算与领取、接管 |
| `preopen/ledger.py` | SQLite 账本：先落盘再发送；两天前无成交的记录归档 |
| `preopen/telegram.py` / `preopen/probe.py` / `preopen/region.py` | Telegram 通知与命令、只读探测、实盘前的所在地区检查 |
| `tests/` | 离线测试（假 Gamma、假 SDK、可控时钟） |

依赖只有 `polymarket-client`（官方统一 SDK，固定 0.18.0；它还在 0.x 阶段、接口常变，所以所有调用都集中在 `preopen/exchange.py`）和 `httpx`。

## 风险提示

- post-only 只保证下单那一刻不吃单，挂着的单随时可能被成交；撤单不能撤回已经成交的部分。
- 只成交一边是方向性风险（49¢ 买一个五五开的结果）。开场前越近，成交越可能来自知道方向的人，撤单时间不要设得太晚。
- 交易所维护、限流、网络故障都可能让撤单晚到；GTD 到期是最后一道兜底。
- 请先用模拟盘和小资金验证。实盘的一切盈亏由使用者自己承担。
