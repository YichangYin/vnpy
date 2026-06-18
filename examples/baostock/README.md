# Baostock + vn.py 量化交易系统

基于 Baostock 免费金融数据的 vn.py 量化交易系统，包含数据采集、多因子选股、信号生成和策略执行。

## 目录结构

```
examples/baostock/
├── README.md                          # 本文档
├── __init__.py                        # Python 包初始化
│
├── baostock_database.py               # 数据库核心模块（16张表 ORM + vn.py 接口）
│
├── baostock_collector.py              # 基础数据采集器（股票列表 + 行业分类）
├── baostock_bar_collector.py          # K线数据采集器（5分钟/日/周/月线）
├── baostock_financial_collector.py    # 财务数据采集器（6步财务数据）
│
├── multi_factor_strategy.py           # 多因子选股策略（排名百分比法）
├── technical_analysis.py              # 技术分析模块（MA/MACD/RSI/成交量）
├── signal_generator.py                # 信号生成器（收盘后运行）
├── vnpy_baostock_strategy.py          # vn.py CTA 策略（读取信号+风控执行）
├── baostock_review_gui.py             # GUI 复盘终端（K线图+多因子选股+信号查看）
│
├── scheduler.py                       # APScheduler 定时调度器
├── data_logger.py                     # 数据日志记录工具
├── test_api.py                        # API 连接测试脚本
│
├── data/                              # 运行数据目录
│   ├── bar_collect_progress.json      # K线采集进度（断点续采）
│   ├── collect_progress.json          # 基础数据采集进度
│   ├── financial_progress.json        # 财务数据采集进度
│   └── api_request_counter.json       # API 请求计数器（全局共享）
│
└── log/                               # 日志目录
    ├── api_call.log                   # API 调用日志（全局共享）
    ├── baostock_bar_collector.log     # K线采集器日志
    ├── baostock_collector.log         # 基础数据采集器日志
    ├── baostock_financial_collector.log # 财务数据采集器日志
    ├── db_commit.log                  # 数据库入库日志
    └── signal_generator.log           # 信号生成器日志
```

## 系统架构

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                              数据采集层                                      │
│                                                                             │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────┐  │
│  │ baostock_        │  │ baostock_        │  │ baostock_financial_      │  │
│  │ collector        │  │ bar_collector    │  │ collector                │  │
│  │                  │  │                  │  │                          │  │
│  │ 股票列表         │  │ 5分钟线          │  │ 利润表                   │  │
│  │ 行业分类         │  │ 日线             │  │ 成长性数据               │  │
│  │                  │  │ 周线             │  │ 资产负债表               │  │
│  │                  │  │ 月线             │  │ 现金流量表               │  │
│  │                  │  │                  │  │ 分红数据                 │  │
│  │                  │  │                  │  │ 业绩快报                 │  │
│  └────────┬─────────┘  └────────┬─────────┘  └────────────┬─────────────┘  │
└───────────┼─────────────────────┼─────────────────────────┼────────────────┘
            │                     │                         │
            ▼                     ▼                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              数据库层 (PostgreSQL)                           │
│                                                                             │
│  ┌─────────────┐ ┌───────────────┐ ┌──────────────┐ ┌────────────────────┐  │
│  │ baostock_   │ │ baostock_     │ │ baostock_    │ │ baostock_          │  │
│  │ stock_list  │ │ bar_data      │ │ profit       │ │ strategy_signals   │  │
│  │ basic       │ │ bar_weekly    │ │ growth       │ │ api_call_log       │  │
│  │ industry    │ │ bar_monthly   │ │ balance      │ │ daily_basic        │  │
│  │             │ │ bar_5min      │ │ cash_flow    │ │                    │  │
│  │             │ │               │ │ dividend     │ │                    │  │
│  │             │ │               │ │ performance  │ │                    │  │
│  └─────────────┘ └───────────────┘ └──────────────┘ └────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
            │                                             ▲
            ▼                                             │
┌─────────────────────────────────────────────────────────────────────────────┐
│                              分析层                                          │
│                                                                             │
│  ┌──────────────────┐                                    ┌────────────────┐  │
│  │ signal_generator │                                    │ multi_factor_  │  │
│  │                  │                                    │ strategy       │  │
│  │ 1. 读取每日指标  │──▶ 初筛 ──▶ 多因子选股 ──┐         │                │  │
│  │ 2. 技术面过滤    │                           │         │ 排名百分比法   │  │
│  │ 3. 写入信号表    │◀── 组合优化 ◀── 技术分析 ──┘         │ 综合评分       │  │
│  └────────┬─────────┘                                    └────────────────┘  │
└───────────┼─────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              执行层 (vn.py)                                  │
│                                                                             │
│  ┌──────────────────────────────┐                                           │
│  │ vnpy_baostock_strategy.py    │                                           │
│  │                              │                                           │
│  │ • 9:30 加载当日信号           │                                           │
│  │ • 有信号 → 开仓              │                                           │
│  │ • 无信号 → 平仓              │                                           │
│  │ • 止损 5% / 止盈 10%         │                                           │
│  │ • 单票最大仓位 5%            │                                           │
│  └──────────────┬───────────────┘                                           │
│                 │                                                           │
│                 ▼                                                           │
│  ┌──────────────────────────────┐                                           │
│  │ vn.py Gateway → 券商/交易所   │                                           │
│  └──────────────────────────────┘                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 文件说明

### 数据采集器

| 文件 | 职责 | 数据表 | 运行方式 |
|------|------|--------|----------|
| `baostock_collector.py` | 采集股票列表和行业分类 | stock_list, basic, industry | `python baostock_collector.py` |
| `baostock_bar_collector.py` | 采集K线数据（5min/日/周/月） | bar_data, bar_weekly, bar_monthly, bar_5min | `python baostock_bar_collector.py` |
| `baostock_financial_collector.py` | 采集财务数据（6步） | profit, growth, balance, cash_flow, dividend, performance | `python baostock_financial_collector.py` |

### 分析与策略

| 文件 | 职责 | 依赖数据表 | 运行方式 |
|------|------|------------|----------|
| `baostock_review_gui.py` | GUI 复盘终端（K线图+多因子选股+信号查看） | daily_basic, bar_data, strategy_signals | `python baostock_review_gui.py` |
| `signal_generator.py` | 每日收盘后生成交易信号 | daily_basic, bar_data, strategy_signals | `python signal_generator.py` |
| `multi_factor_strategy.py` | 多因子选股（排名百分比法） | daily_basic | 被 signal_generator/GUI 调用 |
| `technical_analysis.py` | 技术分析信号生成 | bar_data | 被 signal_generator/GUI 调用 |
| `vnpy_baostock_strategy.py` | vn.py CTA 策略执行 | strategy_signals | vn.py 框架加载 |

### 辅助工具

| 文件 | 职责 |
|------|------|
| `baostock_database.py` | 数据库核心：16张表 ORM 定义 + vn.py Database 接口 + 数据读写方法 |
| `scheduler.py` | APScheduler 定时调度器，可配置每日定时采集和信号生成 |
| `data_logger.py` | 数据日志记录工具 |
| `test_api.py` | Baostock API 连接测试脚本 |

## 数据库表结构（16张）

### K线行情表（4张）

| 表名 | 主键 | 字段 | 数据来源 |
|------|------|------|----------|
| `baostock_bar_data` | symbol_exchange_interval_datetime | symbol, exchange, datetime, interval, open, high, low, close, volume, turnover, open_interest, gateway_name, updated_at | `bs.query_history_k_data_plus(frequency='d')` |
| `baostock_bar_weekly` | 同上 | 同上 | `bs.query_history_k_data_plus(frequency='w')` |
| `baostock_bar_monthly` | 同上 | 同上 | `bs.query_history_k_data_plus(frequency='m')` |
| `baostock_bar_5min` | 同上 | 同上 | `bs.query_history_k_data_plus(frequency='5')` |

### 每日指标表

| 表名 | 主键 | 字段 | 数据来源 |
|------|------|------|----------|
| `baostock_daily_basic` | code_date | code, date, close, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST, turn, volume, amount, updated_at | `bs.query_history_k_data_plus()` 估值字段 |

### 基础信息表

| 表名 | 主键 | 字段 | 数据来源 |
|------|------|------|----------|
| `baostock_stock_list` | code | code, code_name, industry, industryClassification, updated_at | `bs.query_stock_basic()` |
| `baostock_basic` | baostock_code | baostock_code, security_code, security_name, exchange, board, status, market, is_hs, list_date, delist_date, industry, province, city, website, sec_company, underlying_code, update_time | `bs.query_stock_basic()` |
| `baostock_stock_industry` | code_date | code, code_name, industry, industryClassification, date, created_at | `bs.query_stock_industry()` |

### 财务数据表（6张）

| 表名 | 主键 | 核心字段 | Baostock 接口 |
|------|------|----------|---------------|
| `baostock_profit` | code_statDate | roeAvg, npMargin, gpMargin, netProfit, epsTTM, MBRevenue, totalShare, liqaShare | `bs.query_profit_data()` |
| `baostock_growth` | code_statDate | YOYEquity, YOYAsset, YOYNI, YOYEPSBasic, YOYPNI | `bs.query_growth_data()` |
| `baostock_balance` | code_statDate | currentRatio, quickRatio, cashRatio, YOYLiability, liabilityToAsset, assetToEquity | `bs.query_balance_data()` |
| `baostock_cash_flow` | code_statDate | CAToAsset, NCAToAsset, tangibleAssetToAsset, ebitToInterest, CFOToOR, CFOToNP, CFOToGr | `bs.query_cash_flow_data()` |
| `baostock_dividend` | id | dividPlanDate, dividRegistDate, dividOperateDate, dividPayDate, dividCashPsBeforeTax, dividCashPsAfterTax, dividStocksPs, dividCashStock | `bs.query_dividend_data()` |
| `baostock_performance` | id | performanceExpPubDate, performanceExpStatDate, performanceExpressROEWa, performanceExpressEPS, totalShare, totalAssets, totalLiab, totalEquity, BPS, netProfitYOY, netProfit | `bs.query_performance_express_report()` |

### 信号与日志表

| 表名 | 字段 | 说明 |
|------|------|------|
| `baostock_strategy_signals` | id, signal_date, symbol, exchange, direction, score, reason, target_weight, created_at | 策略交易信号，供 vnpy 策略读取 |
| `baostock_api_call_log` | interface_name, call_date, call_count, updated_at | API 调用统计 |

## 运行指南

### 1. 安装依赖

```bash
pip install baostock apscheduler sqlalchemy pandas numpy PySide6 pyqtgraph
```

### 2. 配置数据库

确保 PostgreSQL 已运行，创建数据库：

```bash
createdb baostock_vnpy
```

可选配置环境变量：

```bash
export BAOSTOCK_DB_URL="postgresql://postgres:postgres@localhost:5432/baostock_vnpy"
```

### 3. 数据采集

```bash
# 3.1 采集基础数据（股票列表 + 行业分类）
python baostock_collector.py

# 3.2 采集 K 线数据（5分钟/日/周/月）
python baostock_bar_collector.py

# 3.3 采集财务数据（利润/成长/资产/现金流/分红/业绩）
python baostock_financial_collector.py
```

### 4. 生成交易信号

```bash
# 每日收盘后运行（建议 16:00 后）
python signal_generator.py
```

### 5. 启动 vn.py 策略

在 vn.py 中配置 `BaostockSignalStrategy` 策略。

## 采集器参数

### baostock_collector.py

```bash
python baostock_collector.py              # 正常采集
python baostock_collector.py --reset      # 重置进度从头采集
python baostock_collector.py --reset-counter  # 仅重置计数器
python baostock_collector.py --step 1     # 只采集步骤1（股票列表）
python baostock_collector.py --step 2     # 只采集步骤2（行业分类）
```

### baostock_bar_collector.py

```bash
python baostock_bar_collector.py          # 正常采集（5min/日/周/月）
python baostock_bar_collector.py --reset  # 重置进度从头采集
python baostock_bar_collector.py --step daily   # 只采集日线
python baostock_bar_collector.py --step weekly  # 只采集周线
python baostock_bar_collector.py --step monthly # 只采集月线
```

### baostock_financial_collector.py

```bash
python baostock_financial_collector.py    # 正常采集
python baostock_financial_collector.py --reset  # 重置进度从头采集
python baostock_financial_collector.py --step 1 # 只采集利润表
python baostock_financial_collector.py --step 2 # 只采集成长性数据
```

## 核心特性

### 断点续采

每个采集器都支持断点续采，按股票代码记录进度：

```json
{
  "d": {
    "last_code": "sh.600000",       // 最后查询的股票代码
    "progress": "100/4920",         // 当前进度
    "period_start": "2026-01-01",   // 本轮采集起始日期
    "period_end": "2026-06-06",     // 本轮采集结束日期
    "is_period_done": false,        // 本轮是否完成
    "period_key": "2026-06-06",     // 周期标识
    "last_run": "2026-06-06 10:00"  // 最后运行时间
  }
}
```

### 周期管理（K线采集器）

| 频率 | 周期标识 | 跳过逻辑 |
|------|----------|----------|
| 5分钟线 | 当天日期 | 每天重新采集 |
| 日线 | 当天日期 | 每天重新采集 |
| 周线 | 最近周五日期 | 同一周内已完成则跳过 |
| 月线 | 最近月末日期 | 同一月内已完成则跳过 |

### API 限流

三个采集器共享同一个计数器，每日上限 45000 次：

```json
{
  "date": "2026-06-06",
  "success": 5000,
  "retry": 10,
  "timeout": 2,
  "conn_reset": 0,
  "fail": 5,
  "total": 5017
}
```

### 重连机制

- 超时 30 秒自动重连
- 连接断开自动重连（不先 logout，避免破坏 socket 状态）
- 最多重试 5 次，全部失败则退出程序

### 分批入库

| 数据类型 | 批次大小 | 说明 |
|----------|----------|------|
| 5分钟线 | 1000 条 | 数据量大，大批次减少入库频率 |
| 日线 | 1000 条 | 同上 |
| 周线 | 100 条 | 数据量中等 |
| 月线 | 10 条 | 数据量小，小批次确保及时入库 |
| 财务数据 | 10 条 | 确保进度及时记录 |

## 信号生成流程

```
数据库 baostock_daily_basic 表
    ↓ (get_latest_daily_basic)
DataFrame: code, close, peTTM, pbMRQ, amount, isST, volume, turn
    ↓ (filter_candidates)
过滤：
  - 排除科创板(68)、北交所(4/8/bj)
  - 排除停牌(volume=0)
  - 价格范围 3~200 元
  - 排除 ST 股票
    ↓
初筛后候选股票池
    ↓ (MultiFactorStrategy.select_stocks)
排名百分比法计算综合评分：
  - 估值因子(30%): PE-TTM 排名百分比（越低越好）
  - 动量因子(25%): 收盘价排名百分比（越高越好）
  - 流动性因子(25%): 成交额排名百分比（越高越好）
  - 质量因子(20%): 1/PE 排名百分比（越高越好，PE<0 质量为0）
    ↓
返回 Top 30 股票
    ↓ (technical_filter)
技术面过滤（读取 bar_data 表日线数据）：
  - MA 金叉/死叉（5日/20日均线）
  - MACD 多头/空头（12/26/9）
  - RSI 超买(>70)/超卖(<30)
  - 成交量突破（>20日均量×1.5）
    ↓
仅保留：有看多信号且无看空信号的股票
    ↓ (组合优化)
计算目标权重：min(5%, 1/信号数量)
    ↓ (save_signals)
写入 baostock_strategy_signals 表
    ↓
清理 7 天前的过期信号
```

## GUI 复盘终端

```bash
# 启动 GUI 终端
python baostock_review_gui.py
```

### 界面布局

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│  [菜单栏] 系统 | 数据 | 帮助                                                │
├──────────┬──────────────────────────────────────────────────────────────────┤
│          │  行情信息栏                                                        │
│  股票列表 │  贵州茅台  600519  |  1856.32  +12.50  +0.68%                    │
│  搜索框   │  开盘:1843.82  最高:1862.15  最低:1840.20  昨收:1843.82          │
│  ┌────┐  │  成交量:12,345  成交额:2,283,456,789                              │
│  │6000│  ├──────────────────────────────────────────────────────────────────┤
│  │0000│  │  [日线] [周线] [月线]        │  多因子选股          │  信号查看    │
│  │3007│  │                            │  ┌────────────────┐  │  ┌────────┐  │
│  │0027│  │                            │  │ ▶运行 Top N:30 │  │  │🔄加载  │  │
│  │... │  │      K 线 图                │  ├────────────────┤  │  │日期:[] │  │
│  │    │  │                            │  │代码|名称|PE|评分│  │  ├────────┤  │
│  │    │  │  ┌──┐  ┌──┐  ┌──┐         │  │600519|茅台|35|  │  │  │代码|方 │  │
│  │    │  │  │██│  │  │  │  │         │  │300750|宁德|28|  │  │  │600519|L │  │
│  │    │  │  └──┘  └──┘  └──┘         │  │... |... |..|  │  │  │...  |...│  │
│  │    │  │   MA5   MA10   MA20       │  └────────────────┘  │  └────────┘  │
│  └────┘  │                            │                      │              │
├──────────┴──────────────────────────────────────────────────────────────────┤
│  状态栏: 600519 日线 - 250 条数据                                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 功能模块

| 模块 | 功能 | 操作 |
|------|------|------|
| **股票列表** | 从数据库动态加载有日线数据的股票，支持搜索过滤 | 双击切换股票 |
| **行情信息栏** | 显示当前选中股票的最新行情（价格、涨跌、OHLCV） | 点击K线自动更新 |
| **K线图** | 日线/周线/月线切换，MA5/MA10/MA20均线，十字光标，悬停详情 | 点击周期按钮切换 |
| **多因子选股** | 排名百分比法（PE-TTM 30%、收盘价 25%、成交额 25%、1/PE 20%）+ 技术面过滤 | 点击"运行选股" |
| **信号查看** | 查看 signal_generator 生成的每日交易信号 | 选择日期后加载 |

### 多因子选股说明

```
数据库 baostock_daily_basic 表
    ↓
初筛：排除科创板、北交所、停牌、ST、价格<3或>200
    ↓
排名百分比法计算综合评分：
  - 估值因子(30%): PE-TTM 排名（越低越好）
  - 动量因子(25%): 收盘价排名（越高越好）
  - 流动性因子(25%): 成交额排名（越高越好）
  - 质量因子(20%): 1/PE 排名（越高越好）
    ↓
返回 Top N（默认30）
    ↓
技术面过滤（读取日线数据，保留有看多信号且无看空信号的股票）：
  - MA_GOLDEN_CROSS: 5日均线上穿20日均线
  - MACD_BULLISH: MACD金叉
  - RSI_OVERSOLD: RSI < 30
  - VOLUME_BREAKOUT: 成交量 > 20日均量×1.5
    ↓
结果表格显示（绿色高亮 = 有看多信号）
```

### 快捷键

| 操作 | 方式 |
|------|------|
| 切换股票 | 左侧列表双击 |
| 切换周期 | 点击日线/周线/月线按钮 |
| 运行选股 | 点击"▶ 运行选股" |
| 加载信号 | 选择日期后点击"🔄 加载信号" |
| 刷新列表 | 菜单 → 数据 → 刷新股票列表 |

## 与 Tushare 版本的区别

| 维度 | Tushare 版 | Baostock 版 |
|------|-----------|------------|
| Token | 需要注册获取 | 免费，无需 token |
| 公司详细信息 | `pro.stock_company()` | 不可用（用行业分类替代） |
| 主营业务构成 | `pro.fina_mainbz()` | 不可用（无此接口） |
| 财务审计意见 | `pro.fina_audit()` | 不可用（用业绩快报替代） |
| 成长性数据 | 从利润表计算 | `bs.query_growth_data()` 直接提供 |
| 频率限制 | 有（积分制） | 无 |

## 定时调度

使用 `scheduler.py` 可配置定时任务：

```python
# scheduler.py 配置示例
# 每日 16:30 生成信号
scheduler.add_job(generate_signals, 'cron', hour=16, minute=30)

# 每周六 02:00 采集数据
scheduler.add_job(collect_data, 'cron', day_of_week='sat', hour=2)
```

启动调度器：

```bash
python scheduler.py
```
