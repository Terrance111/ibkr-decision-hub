<p align="right"><a href="#更新日志">中文 ↓</a></p>

# Changelog

All notable changes to this project are documented here.  
Format: `[version] YYYY-MM-DD — summary`

---

## [1.4] 2026-05-17 — Margin indicators, portfolio charts, IBKR-free mode, short interest

**Added:**
- **Liquidity Monitor**: `get_margin_indicators()` in `liquidity_monitor.py` — fetches FINRA monthly NYSE margin debt via HTTP with joblib cache fallback; adds a "Margin & Leverage" section to the Liquidity tab showing debt level (in $B), MoM change %, and Expanding / Contracting / Stable trend signal
- **Stock Analysis**: `get_short_interest()` in `stock_analysis.py` — reads Short % of Float, Days to Cover, Shares Short, and data date from yfinance; displayed as a dedicated row between Institutional Ownership and the price chart
- **Portfolio Charts**: new `src/core/portfolio_charts.py` module with five chart functions (holdings pie, unrealized P&L bar, cost vs MV grouped bar, monthly trade activity, cumulative realized P&L); all functions accept pre-computed DataFrames only — no calculation logic inside; accessible via the collapsible "Chart Analysis" expander at the bottom of the Portfolio tab
- **IBKR-Free Mode**: IBKR data loading is now wrapped in try/except; `ibkr_available` flag controls which tabs require credentials; Portfolio and Trade History tabs show a friendly setup prompt when unconfigured; Liquidity, Market Brief, and Stock Analysis remain fully functional without any IBKR credentials

---

## [1.3] 2026-05-15 — Stock Analysis: open symbol input

**Problem:** The Stock Analysis tab only allowed selecting symbols already present in the local trade history CSV, making it impossible to analyze stocks not currently held.

**Change:**
- Replaced the portfolio-limited `selectbox` with a **free-text input** — any publicly traded ticker (e.g. `SPY`, `NVDA`, `TSLA`) can now be analyzed regardless of trading history
- Added a secondary **quick-pick dropdown** that still allows fast selection from held positions, with the text input taking precedence

---

## [1.2] 2026-05-14 — README bilingual update

- Added a dedicated **Stock Analysis** section in both English and Chinese README, documenting all 8 analysis modules, usage instructions, and data sources
- Removed outdated DCF references from the Disclaimer sections
- Updated Core Features descriptions to reflect current functionality

---

## [1.1] 2026-05-14 — Stock Analysis overhaul + dark professional UI

**Removed:**
- `src/core/dcf_valuation.py` — DCF valuation models (Two-Stage, Conservative, Perpetual / Gordon Growth) removed. DCF is considered outdated for modern equity analysis and produced unreliable results for FCF-negative or high-growth companies.

**Added:**
- `src/core/stock_analysis.py` — new professional analysis engine powered by yfinance (no new dependencies):
  - **Valuation Multiples**: Fwd P/E, EV/EBITDA, P/S, P/FCF, PEG, P/B
  - **Key Fundamentals**: margins (gross / op / net), ROE, revenue growth YoY, debt/equity
  - **Analyst Consensus**: overall rating badge, mean/high/low price targets, upside %, Buy/Hold/Sell distribution bar chart
  - **Technical Indicators**: RSI(14) with zone label, MACD direction, Bollinger Band position, 52-week range %, relative volume, price vs MA20/50/200
  - **Earnings Momentum**: last 4Q EPS actual vs estimate + surprise %, next earnings date, EPS trend
  - **Institutional Ownership**: top 8 holders + % out
  - **Insider Activity**: net direction over last 90 days
  - **6-month Candlestick Chart**: price with MA20/MA50 overlays
- `.streamlit/config.toml` — global dark theme (`#0a0e1a` background, `#00c8a0` accent)
- Custom CSS injected in `main.py`: styled metric cards, teal/red P&L coloring, dark Plotly charts throughout all tabs

---

## [1.0] 2026-05-14 — Initial release

Core local IBKR portfolio tracker with:
- Segmented IBKR Flex Web Service trade history fetch (auto-incremental, `td`-backtracking)
- True cash-flow diluted average cost basis (wave trading aware)
- Realized vs Unrealized P&L separation
- Current Holdings + Historical Holdings dual views
- Liquidity Monitor: CNN Fear & Greed, VIX, 10Y-2Y spread, HY OAS
- Daily Market Brief: Yahoo Finance headlines + earnings calendar
- Full filterable trade history log
- Bilingual README (English + Chinese)

---
---

<a id="更新日志"></a>

<p align="right"><a href="#changelog">English ↑</a></p>

# 更新日志

本项目所有重要变更均记录于此。  
格式：`[版本号] YYYY-MM-DD — 摘要`

---

## [1.4] 2026-05-17 — 保证金指标、持仓图表、无 IBKR 模式、空头兴趣

**新增：**
- **流动性监控**：`liquidity_monitor.py` 新增 `get_margin_indicators()` — 通过 HTTP 拉取 FINRA 月度纽交所保证金债务数据，含 joblib 缓存 fallback；Liquidity 标签页新增"Margin & Leverage"区块，展示债务规模（亿美元）、环比变化 % 及 Expanding / Contracting / Stable 趋势信号
- **股票分析**：`stock_analysis.py` 新增 `get_short_interest()` — 从 yfinance 读取空头占比、Days to Cover、空头股数及数据日期；在机构持仓与价格图之间新增专属展示行
- **投资组合图表**：新建 `src/core/portfolio_charts.py`，包含 5 个图表函数（持仓占比饼图、未实现盈亏柱状图、成本 vs 市值对比、月度交易活跃度、已实现盈亏柱状图）；所有函数仅接收已算好的 DataFrame，与成本计算逻辑完全隔离；通过 Portfolio 标签页底部可折叠的"Chart Analysis"区块访问
- **无 IBKR 降级模式**：IBKR 数据加载已包裹在 try/except 中；`ibkr_available` 标志控制各标签页的凭据依赖；未配置时 Portfolio 和 Trade History 标签页显示友好的设置引导；Liquidity、Market Brief、Stock Analysis 三个标签页无需任何 IBKR 凭据即可正常使用

---

## [1.3] 2026-05-15 — 股票分析：开放任意 Symbol 输入

**问题：** 股票分析标签页原仅限从本地交易历史 CSV 中的 Symbol 选择，无法分析未持有的股票。

**变更：**
- 将受持仓限制的下拉选择器替换为**自由文本输入框**——任意上市股票（如 `SPY`、`NVDA`、`TSLA`）均可直接输入分析，与是否有交易记录无关
- 保留**持仓快速选择下拉**，方便快速切换已持有标的；文本框输入优先

---

## [1.2] 2026-05-14 — README 双语更新

- 在中英文 README 中新增独立的**股票分析**章节，详细说明 8 个分析模块、使用方法及数据来源
- 删除免责声明中过时的 DCF 相关表述
- 更新核心功能描述，与当前实际功能保持一致

---

## [1.1] 2026-05-14 — 股票分析重构 + 专业暗色 UI

**移除：**
- `src/core/dcf_valuation.py` — 删除 DCF 估值模型（两阶段、保守版、永续增长 / Gordon 模型）。DCF 对于自由现金流为负或高成长型公司结果不可靠，且已逐渐脱离现代专业投资者的主流分析框架。

**新增：**
- `src/core/stock_analysis.py` — 基于 yfinance 的专业分析引擎（无新增依赖）：
  - **估值倍数**：远期市盈率、EV/EBITDA、市销率、市现率、PEG、市净率
  - **核心基本面**：毛利率/营业利润率/净利率、ROE、营收同比增长率、债务/权益比
  - **分析师共识**：综合评级徽章、平均/最高/最低目标价、上涨空间、买入/持有/卖出分布条形图
  - **技术指标**：RSI(14) 含超买/超卖标注、MACD 方向、布林带位置、52 周区间位置、相对成交量、价格与 MA20/50/200 偏离%
  - **盈利动量**：近 4 季度 EPS 实际 vs 预期 + 超预期幅度、下次财报日期、EPS 趋势
  - **机构持仓**：前 8 大机构股东 + 持仓占比
  - **内部人士动向**：近 90 天净买入/卖出/混合
  - **6 个月 K 线图**：叠加 MA20/MA50
- `.streamlit/config.toml` — 全局暗色主题（背景 `#0a0e1a`，强调色 `#00c8a0`）
- `main.py` 注入自定义 CSS：卡片化 metric 组件、盈亏绿/红配色、所有标签页 Plotly 图表统一暗色风格

---

## [1.0] 2026-05-14 — 初始版本发布

本地 IBKR 投资组合追踪器，包含以下核心功能：
- IBKR Flex Web Service 分段交易历史抓取（自动增量，`td` 自动回退）
- 真实现金流摊薄成本（支持波段交易）
- 已实现与未实现盈亏严格分离
- 当前持仓 + 历史持仓双视图
- 流动性监控：CNN 恐贪指数、VIX、10Y-2Y 利差、高收益债利差
- 每日市场简报：Yahoo Finance 新闻 + 财报日历
- 完整可筛选交易记录
- 中英双语 README
