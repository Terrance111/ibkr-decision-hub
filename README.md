<p align="right"><a href="#中文说明">中文文档 ↓</a></p>

# IBKR Decision Hub

**A pure local, privacy-first Streamlit dashboard for IBKR traders who want real diluted cost basis, liquidity awareness, and one-stop investment insight.**

All data stays on your machine. Built for serious wave traders who actively manage positions.

---

## Motivation

While actively investing on Interactive Brokers, I was consistently frustrated by one core problem:

**IBKR's official portfolio view does not reflect the true diluted cost basis after wave trading (selling high and buying back lower).**

Every swing trade should lower my average cost, but the platform kept showing the original number. This made it nearly impossible to assess real performance accurately.

I built this tool to solve that — and more:

- Pull **complete trade history** from IBKR via Flex Web Service
- Calculate **true cash-flow diluted cost basis**
- Clearly separate **Realized vs Unrealized P&L**
- Integrate **liquidity monitoring**, market data, and professional stock analysis
- Keep everything **100% local** for privacy and security

---

## Core Features

- **True Diluted Cost Basis** — Net cash-flow method that properly reflects wave trading
- **Strict P&L Separation** — Realized and Unrealized clearly distinguished
- **Dual Portfolio Views** — Current holdings (from Flex Open Positions) + Historical view (from trade CSV)
- **Liquidity Monitor** — CNN Fear & Greed + VIX + credit spread proxies
- **Daily Market Brief** — Yahoo Finance headlines + earnings calendar + key events
- **Professional Stock Analysis** — Analyze any ticker: valuation multiples, analyst consensus, institutional ownership, earnings momentum, enhanced technicals, and 6-month price chart
- **Full Trade History** — Filterable transaction log
- **Dark Professional UI** — Bloomberg-inspired dark theme throughout

---

## Project Layout

```
.
├── main.py                     # Streamlit entrypoint
├── config.py                   # Loads .env; creates cache directory
├── requirements.txt
├── .env.example                # Template for all secrets / settings
├── .gitignore
├── .streamlit/
│   └── config.toml             # Dark theme configuration
├── cache/                      # Runtime cache (trade_history.csv, snapshots, etc.)
└── src/
    ├── data/                   # ibkr_fetch.py · ibkr_account.py · flex_report.py
    ├── core/                   # trade_processor.py · market_data.py · stock_analysis.py
    ├── monitors/               # liquidity_monitor.py · daily_brief.py
    └── utils/
```

---

## Prerequisites

- Python 3.10+
- An IBKR account with **Flex Web Service** enabled
- Two Activity Flex Query templates created in IBKR Client Portal (details below)

---

## Step 1 — Enable Flex Web Service & Get Token

1. Log in to **IBKR Client Portal**
2. Go to **Reporting → Flex Queries → Flex Web Service**
3. Click **Generate Token** (valid until manually regenerated)
4. Copy the token — this is your `IBKR_FLEX_TOKEN`

> **Important:** The token's effective `toDate` is always `(token generation date − 1)`. Regenerate the token monthly to keep your data current.

---

## Step 2 — Create Trade History Query

In Client Portal: **Reporting → Flex Queries → Activity Flex Query → + (New)**

### General Configuration
| Setting | Value |
|---------|-------|
| Format | **CSV** |
| Period | **Last 365 Calendar Days** (or set a custom start date matching your account opening) |
| Date Format | `yyyyMMdd` |
| Time Format | `HHmmss` |

### Sections → Trades

Enable the **Trades** section. Select **Execution** level of detail. Check the following fields:

| Field Name in Portal | Notes |
|----------------------|-------|
| **Currency** | Required — filters STK vs FX vs Bond rows |
| **Asset Class** | Required — code filters on `STK` only |
| **Symbol** | Required |
| **Date/Time** | Required — exact execution timestamp |
| **Trade Date** | Required |
| **Quantity** | Required — shares per fill (negative for sells) |
| **Trade Price** | Required |
| **Trade Money** | Required — gross trade value |
| **Proceeds** | Required — net cash flow (used for diluted cost calc) |
| **Commission** | Required |
| **Basis in Open Price** | Optional — IBKR lot cost; not used in calculations |
| **Buy/Sell** | Required |
| **Level of Detail** | Required — ensures EXECUTION rows are identifiable |

Save the query and note its **Query ID** → this is your `IBKR_FLEX_QUERY_ID`.

---

## Step 3 — Create Open Positions Query

In Client Portal: **Reporting → Flex Queries → Activity Flex Query → + (New)**

### General Configuration
| Setting | Value |
|---------|-------|
| Format | **CSV** |
| Period | **Latest** (no date range — always returns current snapshot) |

### Sections → Open Positions

Enable the **Open Positions** section. Select **Summary** level of detail. Check:

| Field Name in Portal | Notes |
|----------------------|-------|
| **Asset Class** | Required |
| **Symbol** | Required |
| **Quantity** | Required — current shares held |
| **Mark Price** | Required — used for unrealized P&L |
| **Position Value** | Required |
| **Open Price** | Recommended — IBKR's own avg cost (reference) |
| **Cost Basis Price** | Recommended |
| **Cost Basis Money** | Recommended |
| **% of NAV** | Optional |
| **Unrealized P&L (FIFO)** | Recommended — IBKR's calc shown alongside ours |
| **Level of Detail** | Required |
| **Currency** | Required |

Save the query and note its **Query ID** → this is your `IBKR_FLEX_POSITIONS_QUERY_ID`.

> This should be a **dedicated** positions-only query. The app fetches it without `fd`/`td` date params to get the current live snapshot.

---

## Step 4 — App Setup

```bash
# 1. Clone the repository
git clone <repo-url>
cd ibkr-decision-hub

# 2. Create virtual environment and install dependencies
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. Create your .env from the template
cp .env.example .env
```

Edit `.env` with the values from the steps above:

```
IBKR_FLEX_TOKEN=your_token_here
IBKR_FLEX_QUERY_ID=your_trades_query_id
IBKR_FLEX_POSITIONS_QUERY_ID=your_positions_query_id
```

### Optional but Recommended

```
# Set your actual account opening date to avoid fetching thousands of empty segments
IBKR_TRADE_HISTORY_START_DATE=20240101

# Or set a lookback in days (default 730 if no start date is set)
# IBKR_TRADE_HISTORY_LOOKBACK_DAYS=3650
```

Full list of all options is documented in `.env.example`.

---

## Step 5 — Run

```bash
streamlit run main.py
```

Use the sidebar **"Refresh All Data"** button to pull fresh data from IBKR.

On first run, the app fetches all trade history from `IBKR_TRADE_HISTORY_START_DATE` to yesterday in multiple 364-day segments. Subsequent refreshes only fetch from the last cached date forward (incremental).

---

## Stock Analysis Tab

The **Stock Analysis** tab lets you analyze any publicly traded stock — not limited to your portfolio holdings.

**How to use:**
- Type any ticker directly in the input box (e.g. `NVDA`, `SPY`, `TSLA`)
- Or quick-pick from your portfolio symbols via the dropdown
- Click **Analyze** to fetch and display the full panel

**What's displayed:**

| Section | Data |
|---------|------|
| **Valuation Multiples** | Fwd P/E, EV/EBITDA, P/S, P/FCF, PEG, P/B |
| **Key Fundamentals** | Gross/Op/Net margin, ROE, Revenue growth YoY, Debt/Equity |
| **Analyst Consensus** | Overall rating badge, mean/high/low price targets, upside %, Buy/Hold/Sell distribution bar |
| **Technical Indicators** | RSI(14) with zone label, MACD direction, Bollinger Band position, 52-week range %, relative volume, price vs MA 20/50/200 |
| **Earnings Momentum** | Last 4 quarters EPS actual vs estimate + surprise %, next earnings date, EPS trend |
| **Institutional Ownership** | Top 8 institutional holders + % out |
| **Insider Activity** | Net insider direction over last 90 days (Net Buying / Selling / Mixed) |
| **Price Chart** | 6-month candlestick with MA20 / MA50 overlays |

All data is sourced from yfinance at analysis time — no extra API keys required.

---

## Known Data Limitations

### 1. Historical Fetch Window

The trade fetch starts from `IBKR_TRADE_HISTORY_START_DATE` (or `today − IBKR_TRADE_HISTORY_LOOKBACK_DAYS`, default **730 days**). Positions originally opened **before** that window will have their initial buy records missing. This shows up as:

- **Negative ledger shares** — sells exceed buys in the local CSV; the live snapshot is used as ground truth and such symbols are excluded from display
- **Underestimated diluted avg cost** — only partial buy history in window

**Fix:** Set `IBKR_TRADE_HISTORY_START_DATE=` to your account opening date and do a full refresh.

### 2. API Data Available Only Through Yesterday

IBKR Flex always lags by 1 day. The effective `toDate` is `(token generation date − 1)`. Requests with `td = today` return **ErrorCode 1003**. The app automatically backtracks `td` up to 7 days to find the latest valid date.

**Implication:** Trades executed today appear the following day.

### 3. Symbols Absent from Live Snapshot

If a symbol appears in trade history but not in the live Open Positions snapshot, the position is treated as fully closed and excluded from all portfolio views.

| Scenario | Explanation |
|----------|-------------|
| Sold after last refresh | Click **Refresh** to sync |
| Sold during a data gap period | Sell record not in local CSV due to API gap |
| Corporate action / delisting | Forced closure outside normal trade flow |
| Account transfer | Not a taxable sell; no trade record |

### 4. Wave-Traded Positions with Zero Diluted Cost

**Diluted Avg Cost** can reach `$0.00` when cumulative sell proceeds from prior round-trips exceed total buy costs — meaning remaining shares are held at zero net cost ("house money"). Unrealized P&L for such positions equals the full current market value. This is correct behavior, not an error.

---

## Security & Privacy

- Credentials live only in `.env`, which is gitignored
- No backend, no cloud sync — runs entirely on your machine
- Third-party data (Yahoo Finance, CNN API, RSS) is fetched from your machine only when you use those tabs

---

## Disclaimer

This tool is for **personal informational purposes only**. It is not investment, tax, or financial advice. All analysis data (valuation multiples, analyst targets, technicals) is sourced from public data via yfinance — always verify numbers with your broker statements and consult professionals before making decisions.

---

## License

MIT License

---
---

<a id="中文说明"></a>

<p align="right"><a href="#ibkr-decision-hub">English ↑</a></p>

# IBKR 决策中心

**纯本地运行、隐私优先的 Streamlit 投资仪表盘——专为需要真实摊薄成本、流动性感知和一站式投资洞察的 IBKR 交易者设计。**

所有数据保存在本地，不上传任何云服务。专为主动做波段交易的投资者打造。

---

## 项目动机

在 Interactive Brokers 上主动投资的过程中，我长期被一个核心问题困扰：

**IBKR 官方投资组合视图无法反映波段交易后的真实摊薄成本（高价卖出、低价买回）。**

每次做完波段，实际持仓成本应该降低，但平台始终显示原始买入价。这让我很难准确评估真实盈亏，也无法做出合理决策。

我开发这个工具就是为了解决这个问题，同时提供更多功能：

- 通过 Flex Web Service 拉取**完整交易历史**
- 计算**真实现金流摊薄成本**
- 清晰区分**已实现与未实现盈亏**
- 集成**流动性监控**、市场数据与专业股票分析
- 全部**本地运行**，保护隐私与安全

---

## 核心功能

- **真实摊薄成本** — 净现金流法，正确反映波段交易收益
- **盈亏严格分离** — 已实现与未实现盈亏清晰展示
- **双视图切换** — 当前持仓（来自 Flex 实时快照）+ 历史记录（来自本地 CSV）
- **流动性监控** — CNN 恐贪指数 + VIX + 信用利差代理
- **每日市场简报** — Yahoo Finance 新闻 + 财报日历 + 重要事件
- **专业股票分析** — 可分析任意 ticker：估值倍数、分析师共识、机构持仓、盈利动量、增强技术指标及 6 个月 K 线图
- **完整交易记录** — 可筛选的交易流水
- **专业暗色 UI** — Bloomberg 风格全局暗色主题

---

## 项目结构

```
.
├── main.py                     # Streamlit 入口
├── config.py                   # 加载 .env；创建 cache 目录
├── requirements.txt
├── .env.example                # 所有密钥和配置的模板
├── .gitignore
├── .streamlit/
│   └── config.toml             # 暗色主题配置
├── cache/                      # 运行时缓存（trade_history.csv、快照等）
└── src/
    ├── data/                   # ibkr_fetch.py · ibkr_account.py · flex_report.py
    ├── core/                   # trade_processor.py · market_data.py · stock_analysis.py
    ├── monitors/               # liquidity_monitor.py · daily_brief.py
    └── utils/
```

---

## 前置条件

- Python 3.10+
- 已开通 IBKR 账户并启用 **Flex Web Service**
- 在 IBKR Client Portal 创建两个 Activity Flex Query 模板（见下方步骤）

---

## 第一步 — 启用 Flex Web Service 并获取 Token

1. 登录 **IBKR Client Portal**
2. 进入 **报表 → Flex 查询 → Flex Web 服务**
3. 点击 **生成令牌**（手动重新生成前长期有效）
4. 复制 Token，填入 `IBKR_FLEX_TOKEN`

> **重要：** Token 的有效 `toDate` 始终是 `（Token 生成日期 − 1）`。建议每月重新生成一次 Token，确保能拉取到最新数据。

---

## 第二步 — 创建交易历史查询

在 Client Portal 中：**报表 → Flex 查询 → Activity Flex 查询 → +（新建）**

### 通用配置
| 设置项 | 值 |
|--------|-----|
| 格式 | **CSV** |
| 期间 | **过去 365 个日历天**（或设置与账户开户日匹配的自定义起始日期） |
| 日期格式 | `yyyyMMdd` |
| 时间格式 | `HHmmss` |

### 报表内容 → 交易（Trades）

启用 **交易（Trades）** 部分，明细级别选 **Execution（执行）**，勾选以下字段：

| Portal 中的字段名 | 说明 |
|-------------------|------|
| **Currency（货币）** | 必选 — 用于区分 STK / 外汇 / 债券行 |
| **Asset Class（资产类别）** | 必选 — 代码仅处理 `STK` 行 |
| **Symbol（代码）** | 必选 |
| **Date/Time（日期/时间）** | 必选 — 精确执行时间戳 |
| **Trade Date（交易日期）** | 必选 |
| **Quantity（数量）** | 必选 — 每笔成交股数（卖出为负数） |
| **Trade Price（成交价）** | 必选 |
| **Trade Money（成交金额）** | 必选 — 成交总价值 |
| **Proceeds（净收入）** | 必选 — 净现金流（摊薄成本计算核心） |
| **Commission（佣金）** | 必选 |
| **Basis in Open Price（开仓基础价）** | 可选 — IBKR 的批次成本，计算中不使用 |
| **Buy/Sell（买卖方向）** | 必选 |
| **Level of Detail（明细级别）** | 必选 — 确保可识别 EXECUTION 行 |

保存查询，记下 **查询 ID** → 填入 `IBKR_FLEX_QUERY_ID`。

---

## 第三步 — 创建持仓快照查询

在 Client Portal 中：**报表 → Flex 查询 → Activity Flex 查询 → +（新建）**

### 通用配置
| 设置项 | 值 |
|--------|-----|
| 格式 | **CSV** |
| 期间 | **最新（Latest）**（无日期范围 — 始终返回当前快照） |

### 报表内容 → 未平仓头寸（Open Positions）

启用 **未平仓头寸** 部分，明细级别选 **Summary（汇总）**，勾选以下字段：

| Portal 中的字段名 | 说明 |
|-------------------|------|
| **Asset Class（资产类别）** | 必选 |
| **Symbol（代码）** | 必选 |
| **Quantity（数量）** | 必选 — 当前持股数 |
| **Mark Price（市价）** | 必选 — 用于未实现盈亏计算 |
| **Position Value（持仓价值）** | 必选 |
| **Open Price（开仓价）** | 推荐 — IBKR 自身的成本价（参考用） |
| **Cost Basis Price（成本基础价）** | 推荐 |
| **Cost Basis Money（成本基础金额）** | 推荐 |
| **% of NAV（占净资产比例）** | 可选 |
| **Unrealized P&L (FIFO)（未实现盈亏）** | 推荐 — IBKR 计算值与本工具对照显示 |
| **Level of Detail（明细级别）** | 必选 |
| **Currency（货币）** | 必选 |

保存查询，记下 **查询 ID** → 填入 `IBKR_FLEX_POSITIONS_QUERY_ID`。

> 建议此查询为**专用的纯持仓查询**。App 拉取时不带 `fd`/`td` 日期参数，直接获取当前实时快照。

---

## 第四步 — 安装与配置

```bash
# 1. 克隆仓库
git clone <repo-url>
cd ibkr-decision-hub

# 2. 创建虚拟环境并安装依赖
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. 从模板创建 .env 文件
cp .env.example .env
```

编辑 `.env`，填入前面获取的信息：

```
IBKR_FLEX_TOKEN=你的_token
IBKR_FLEX_QUERY_ID=交易历史查询_ID
IBKR_FLEX_POSITIONS_QUERY_ID=持仓快照查询_ID
```

### 推荐额外配置

```
# 设置账户实际开户日期，避免抓取大量空数据段
IBKR_TRADE_HISTORY_START_DATE=20240101

# 或者用回溯天数（未设置 START_DATE 时默认 730 天）
# IBKR_TRADE_HISTORY_LOOKBACK_DAYS=3650
```

所有配置项详见 `.env.example`。

---

## 第五步 — 运行

```bash
streamlit run main.py
```

点击左侧边栏的 **"Refresh All Data"** 按钮从 IBKR 拉取最新数据。

---

## 股票分析功能

**Stock Analysis** 标签页可分析任意上市股票，不受持仓限制。

**使用方法：**
- 在输入框直接输入任意 ticker（如 `NVDA`、`SPY`、`TSLA`）
- 或从下拉菜单快速选择你的持仓标的
- 点击 **Analyze** 获取并展示完整分析面板

**展示内容：**

| 模块 | 数据内容 |
|------|---------|
| **估值倍数** | 远期市盈率、EV/EBITDA、市销率、市现率、PEG、市净率 |
| **核心基本面** | 毛利率/营业利润率/净利率、ROE、营收同比增长、债务/权益比 |
| **分析师共识** | 综合评级徽章、平均/最高/最低目标价、上涨空间、买入/持有/卖出分布条形图 |
| **技术指标** | RSI(14) 及超买超卖标注、MACD 方向、布林带位置、52 周区间位置、相对成交量、价格与 MA20/50/200 偏离% |
| **盈利动量** | 近 4 季度 EPS 实际 vs 预期 + 超预期幅度、下次财报日期、EPS 趋势 |
| **机构持仓** | 前 8 大机构股东 + 持仓占比 |
| **内部人士动向** | 近 90 天净买入/卖出/混合 |
| **价格走势图** | 6 个月 K 线图叠加 MA20/MA50 |

所有数据在点击分析时通过 yfinance 实时获取，无需额外 API Key。

首次运行时，App 会从 `IBKR_TRADE_HISTORY_START_DATE` 到昨天，分多个 364 天的时间段拉取全量历史。后续刷新仅增量拉取最新日期之后的数据。

---

## 已知数据局限性

### 1. 历史抓取窗口

交易历史从 `IBKR_TRADE_HISTORY_START_DATE`（或 `今天 − IBKR_TRADE_HISTORY_LOOKBACK_DAYS`，默认 **730 天**）开始抓取。在该窗口之前开仓的持仓，其初始买入记录将缺失，表现为：

- **账本持仓为负** — 本地 CSV 中卖出数量多于买入数量；以实时持仓快照为准，此类标的不显示在面板中
- **摊薄成本偏低** — 仅包含窗口内的部分买入历史

**解决方法：** 在 `.env` 中设置 `IBKR_TRADE_HISTORY_START_DATE=` 为账户开户日期，然后做一次全量刷新。

### 2. API 数据只到昨天

IBKR Flex 始终滞后 1 天。有效 `toDate` = `（Token 生成日期 − 1）`。请求 `td = 今天` 会返回 **ErrorCode 1003**。App 会自动将 `td` 回退最多 7 天以找到最新有效日期。

**影响：** 今天的交易次日刷新后才会出现。

### 3. 实时快照中不存在的标的

若某标的出现在交易历史中，但不在实时持仓快照里，则视为已清仓，从所有视图中排除。

| 情形 | 说明 |
|------|------|
| 上次刷新后已卖出 | 点击 **Refresh** 同步即可 |
| 在数据空白期内卖出 | 卖出记录因 API 空白期不在本地 CSV 中 |
| 公司行动 / 退市 | 非正常交易流程的强制平仓 |
| 账户划转 | 非应税卖出事件，无交易记录 |

### 4. 波段交易导致摊薄成本为零

当历次波段卖出的累计收益超过全部买入成本时，**摊薄成本**会降至 `$0.00`——即剩余持仓以零净成本持有（"躺赚"状态）。此时未实现盈亏等于当前市值。这是正常计算结果，不是错误。

---

## 安全与隐私

- 所有凭证仅存于 `.env`，已在 `.gitignore` 中排除，不会上传代码库
- 无后端服务，无云同步 — 完全在本地运行
- 第三方数据（Yahoo Finance、CNN API、RSS）仅在使用对应功能时从本地机器请求

---

## 免责声明

本工具仅供**个人信息参考使用**，不构成投资、税务或财务建议。所有分析数据（估值倍数、分析师目标价、技术指标）均通过 yfinance 获取自公开来源——在做出任何决策前，请以券商官方报表为准，并咨询专业人士。

---

## 许可证

MIT License
