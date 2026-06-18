"""
自定义 vn.py 数据库接口 — 将 Baostock 数据桥接到 vn.py 数据库接口

架构说明:
- 作为 vn.py 的自定义 Database Backend
- 支持保存和加载 BarData/TickData
- 数据源: Baostock 采集后写入 PostgreSQL
- 回测时 vn.py 自动调用 load_bar_data()

16 张业务表:
    baostock_basic            — 股票基础信息
    baostock_stock_industry   — 行业分类
    baostock_profit           — 利润表
    baostock_growth           — 成长性数据
    baostock_balance          — 资产负债表
    baostock_cash_flow        — 现金流量表
    baostock_dividend         — 分红数据
    baostock_performance      — 业绩快报
    baostock_daily_basic      — 每日指标(PE/PB/换手率)
    baostock_bar_data         — 日线行情
    baostock_bar_weekly       — 周线行情

使用方式:
    在 vnpy/trader/setting.py 中配置:
        "database.name": "baostock"
        "database.module": "examples.baostock.baostock_database"
"""
import os

from vnpy.trader.database import BaseDatabase, BarOverview, TickOverview
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, Text, select, func
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import Table
import logging
import time

# SQLAlchemy 基类
Base = declarative_base()

# 数据库操作日志（同时输出到文件和控制台，方便调试）
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
os.makedirs(LOG_DIR, exist_ok=True)


def _rotate_old_logs():
    """将历史日志文件移动到日期子目录中（全局只执行一次）

    使用 min(mtime, ctime) 判断文件日期：
    - mtime：文件最后修改时间
    - ctime：文件创建时间（Windows）/ inode 变更时间（Linux）
    这样可以处理日志被打开后 mtime 被更新为"今天"的情况
    """
    global _LOGS_ROTATED
    if _LOGS_ROTATED:
        return
    _LOGS_ROTATED = True
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        log_files = [f for f in os.listdir(LOG_DIR) if f.endswith(".log")]
        if not log_files:
            return

        # 按创建日期或修改日期（取较早者）分组
        date_groups = {}
        for filename in log_files:
            filepath = os.path.join(LOG_DIR, filename)
            mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
            ctime = datetime.fromtimestamp(os.path.getctime(filepath))
            # 使用较早的时间作为文件的真实日期
            file_date = min(mtime, ctime).strftime("%Y-%m-%d")
            if file_date == today:
                continue  # 今天的文件不移
            date_groups.setdefault(file_date, []).append(filename)

        for date_str, files in date_groups.items():
            dest_dir = os.path.join(LOG_DIR, date_str)
            os.makedirs(dest_dir, exist_ok=True)
            for filename in files:
                src = os.path.join(LOG_DIR, filename)
                dest = os.path.join(dest_dir, filename)
                if os.path.exists(dest):
                    base, ext = os.path.splitext(filename)
                    i = 1
                    while os.path.exists(dest):
                        dest = os.path.join(dest_dir, f"{base}_{i}{ext}")
                        i += 1
                os.rename(src, dest)
    except Exception:
        pass  # 归档失败不影响主流程


_LOGS_ROTATED = False
_rotate_old_logs()

db_logger = logging.getLogger("baostock_db")
db_logger.setLevel(logging.DEBUG)
# 避免重复添加 handler
if not db_logger.handlers:
    _db_handler = logging.FileHandler(
        os.path.join(LOG_DIR, "db_commit.log"),
        encoding="utf-8"
    )
    db_logger.addHandler(_db_handler)
    # 同时输出到控制台
    db_logger.addHandler(logging.StreamHandler())


# ============================================================
# 行情数据表
# ============================================================

class BarRecord(Base):
    """Bar 行情数据表 — vnpy BarData 存储"""
    __tablename__ = "baostock_bar_data"
    __table_args__ = {'comment': '日线行情数据(OHLCV)，供 vnpy 回测和实盘使用'}
    id = Column(String(50), primary_key=True, comment='主键: symbol_exchange_interval_datetime')
    symbol = Column(String(10), nullable=False, comment='证券代码，如 600000')
    exchange = Column(String(10), nullable=False, comment='交易所: sse(上交所)/szse(深交所)')
    datetime = Column(DateTime, nullable=False, comment='行情时间')
    interval = Column(String(10), nullable=False, comment='周期: daily/1m/5m/15m/30m/60m')
    open = Column(Float, nullable=False, comment='开盘价')
    high = Column(Float, nullable=False, comment='最高价')
    low = Column(Float, nullable=False, comment='最低价')
    close = Column(Float, nullable=False, comment='收盘价')
    volume = Column(Float, nullable=False, comment='成交量(股)')
    turnover = Column(Float, nullable=False, comment='成交额(元)')
    open_interest = Column(Float, default=0, comment='持仓量(期货用，股票为0)')
    gateway_name = Column(String(20), default="BAOSTOCK", comment='数据来源')
    updated_at = Column(DateTime, comment='数据更新时间')


# ============================================================
# 周线行情表 (新增)
# ============================================================

class WeeklyBarRecord(Base):
    """周线行情 — bs.query_history_k_data_plus(frequency='w')"""
    __tablename__ = "baostock_bar_weekly"
    __table_args__ = {'comment': '周线行情数据(OHLCV)，供 vnpy 回测使用'}
    id = Column(String(50), primary_key=True, comment='主键: symbol_exchange_interval_datetime')
    symbol = Column(String(10), nullable=False, comment='证券代码，如 600000')
    exchange = Column(String(10), nullable=False, comment='交易所: sse/szse')
    datetime = Column(DateTime, nullable=False, comment='行情时间(周一)')
    interval = Column(String(10), nullable=False, comment='周期: weekly')
    open = Column(Float, nullable=False, comment='开盘价')
    high = Column(Float, nullable=False, comment='最高价')
    low = Column(Float, nullable=False, comment='最低价')
    close = Column(Float, nullable=False, comment='收盘价')
    volume = Column(Float, nullable=False, comment='成交量(股)')
    turnover = Column(Float, nullable=False, comment='成交额(元)')
    open_interest = Column(Float, default=0, comment='持仓量')
    gateway_name = Column(String(20), default="BAOSTOCK", comment='数据来源')
    updated_at = Column(DateTime, comment='数据更新时间')


# ============================================================
# 月线行情表 (新增)
# ============================================================

class MonthlyBarRecord(Base):
    """月线行情 — bs.query_history_k_data_plus(frequency='m')"""
    __tablename__ = "baostock_bar_monthly"
    __table_args__ = {'comment': '月线行情数据(OHLCV)，供 vnpy 回测使用'}
    id = Column(String(50), primary_key=True, comment='主键: symbol_exchange_interval_datetime')
    symbol = Column(String(10), nullable=False, comment='证券代码，如 600000')
    exchange = Column(String(10), nullable=False, comment='交易所: sse/szse')
    datetime = Column(DateTime, nullable=False, comment='行情时间(月初)')
    interval = Column(String(10), nullable=False, comment='周期: monthly')
    open = Column(Float, nullable=False, comment='开盘价')
    high = Column(Float, nullable=False, comment='最高价')
    low = Column(Float, nullable=False, comment='最低价')
    close = Column(Float, nullable=False, comment='收盘价')
    volume = Column(Float, nullable=False, comment='成交量(股)')
    turnover = Column(Float, nullable=False, comment='成交额(元)')
    open_interest = Column(Float, default=0, comment='持仓量')
    gateway_name = Column(String(20), default="BAOSTOCK", comment='数据来源')
    updated_at = Column(DateTime, comment='数据更新时间')


# ============================================================
# 5分钟线行情表 (新增)
# ============================================================

class Bar5MinRecord(Base):
    """5分钟线行情 — bs.query_history_k_data_plus(frequency='5', adjustflag='2')"""
    __tablename__ = "baostock_bar_5min"
    __table_args__ = {'comment': '5分钟线行情数据(OHLCV)，供 vnpy 回测使用'}
    id = Column(String(50), primary_key=True, comment='主键: symbol_exchange_interval_datetime')
    symbol = Column(String(10), nullable=False, comment='证券代码，如 600000')
    exchange = Column(String(10), nullable=False, comment='交易所: sse/szse')
    datetime = Column(DateTime, nullable=False, comment='行情时间')
    interval = Column(String(10), nullable=False, comment='周期: 5m')
    open = Column(Float, nullable=False, comment='开盘价')
    high = Column(Float, nullable=False, comment='最高价')
    low = Column(Float, nullable=False, comment='最低价')
    close = Column(Float, nullable=False, comment='收盘价')
    volume = Column(Float, nullable=False, comment='成交量(股)')
    turnover = Column(Float, nullable=False, comment='成交额(元)')
    open_interest = Column(Float, default=0, comment='持仓量')
    gateway_name = Column(String(20), default="BAOSTOCK", comment='数据来源')
    updated_at = Column(DateTime, comment='数据更新时间')


# ============================================================
# 信号表
# ============================================================

class SignalRecord(Base):
    """策略信号表 — 外部分析脚本生成，vnpy策略读取执行"""
    __tablename__ = "baostock_strategy_signals"
    __table_args__ = {'comment': '多因子分析生成的交易信号，vnpy策略盘中读取执行'}
    id = Column(String(50), primary_key=True, comment='主键: signal_date_symbol_exchange')
    signal_date = Column(String(10), nullable=False, comment='信号日期 YYYY-MM-DD')
    symbol = Column(String(10), nullable=False, comment='证券代码')
    exchange = Column(String(10), nullable=False, comment='交易所: SSE/SZSE')
    direction = Column(String(10), nullable=False, comment='交易方向: LONG(做多)/SHORT(做空)')
    score = Column(Float, nullable=False, comment='综合评分(0~1)')
    reason = Column(Text, nullable=True, comment='信号原因，如 MA_GOLDEN_CROSS|MACD_BULLISH')
    target_weight = Column(Float, nullable=False, comment='目标仓位权重(如 0.05 表示5%)')
    updated_at = Column(DateTime, comment='数据更新时间')


# ============================================================
# API 调用统计表
# ============================================================

class ApiCallLog(Base):
    """API 调用次数统计表 — 按日按接口统计"""
    __tablename__ = "baostock_api_call_log"
    __table_args__ = {'comment': '每日各接口调用次数统计，用于监控用量'}
    interface_name = Column(String(100), primary_key=True, comment='接口名称')
    call_date = Column(String(10), primary_key=True, comment='调用日期 YYYY-MM-DD')
    call_count = Column(Integer, default=0, comment='当日调用次数')
    updated_at = Column(DateTime, comment='最后更新时间')


# ============================================================
# 步骤1: 股票列表(轻量版)
# ============================================================

class StockListRecord(Base):
    """股票列表 — bs.query_stock_basic() 轻量版"""
    __tablename__ = "baostock_stock_list"
    __table_args__ = {'comment': '股票列表(轻量版)，仅含代码/名称/行业，用于快速筛选'}
    code = Column(String(15), primary_key=True, comment='baostock代码，如 sh.600000')
    code_name = Column(String(50), comment='证券名称')
    industry = Column(String(50), comment='行业分类')
    industryClassification = Column(String(50), comment='行业细分')
    updated_at = Column(DateTime, comment='最后更新时间')


# ============================================================
# 股票基础信息表 — 完整版
# ============================================================

class BasicInfoRecord(Base):
    """股票基础信息 — 完整版(17字段)"""
    __tablename__ = "baostock_basic"
    __table_args__ = {'comment': '股票基础信息完整版，含代码/名称/交易所/板块/上市日期/省份/官网等'}
    baostock_code = Column(String(15), primary_key=True, comment='baostock代码，如 sh.600000')
    security_code = Column(String(15), nullable=False, comment='证券代码，如 600000')
    security_name = Column(String(50), nullable=False, comment='证券名称，如 浦发银行')
    exchange = Column(String(10), nullable=False, comment='交易所: sse(上交所)/szse(深交所)')
    board = Column(String(10), nullable=False, comment='板块: 10(主板)/20(中小板)/30(创业板)/40(科创板)/50(北交所)')
    status = Column(String(10), nullable=False, comment='状态: 1(上市)/2(退市)/3(暂停)')
    market = Column(String(10), nullable=False, comment='市场类型: 1(沪深A股)/2(港股通沪)/3(港股通深)')
    is_hs = Column(String(10), comment='是否沪深港通标的: 1(是)/空(否)')
    list_date = Column(String(10), nullable=False, comment='上市日期 YYYYMMDD')
    delist_date = Column(String(10), comment='退市日期 YYYYMMDD，空表示未退市')
    industry = Column(String(50), comment='所属行业')
    province = Column(String(30), comment='注册省份')
    city = Column(String(30), comment='注册城市')
    website = Column(String(200), comment='公司官网')
    sec_company = Column(String(100), comment='所属证券公司')
    underlying_code = Column(String(15), comment='关联代码(如ETF跟踪的指数代码)')
    updated_at = Column('update_time', DateTime, nullable=False, comment='最后更新时间')


# ============================================================
# 步骤1.5: 每日指标(PE/PB/市值/换手率)
# ============================================================

class DailyBasicRecord(Base):
    """每日指标 — bs.query_history_k_data_plus() 估值字段"""
    __tablename__ = "baostock_daily_basic"
    __table_args__ = {'comment': '每日估值指标(PE/PB/PS/PCF/换手率)，用于多因子选股'}
    id = Column(String(50), primary_key=True, comment='主键: code_date')
    code = Column(String(15), nullable=False, comment='baostock代码')
    date = Column(String(10), nullable=False, comment='交易日期 YYYY-MM-DD')
    close = Column(Float, comment='收盘价')
    peTTM = Column(Float, comment='市盈率(滚动TTM)')
    pbMRQ = Column(Float, comment='市净率(最近季度)')
    psTTM = Column(Float, comment='市销率(滚动TTM)')
    pcfNcfTTM = Column(Float, comment='市现率(滚动TTM)')
    isST = Column(String(5), comment='是否ST: 1(是)/空(否)')
    turn = Column(Float, comment='换手率(%)')
    volume = Column(Float, comment='成交量(股)')
    amount = Column(Float, comment='成交额(元)')
    created_at = Column(DateTime, comment='数据更新时间')


# ============================================================
# 步骤2: 行业分类
# ============================================================

class StockIndustryRecord(Base):
    """行业分类 — bs.query_stock_industry()"""
    __tablename__ = "baostock_stock_industry"
    __table_args__ = {'comment': '股票行业分类数据，按申万/中信行业体系分类'}
    id = Column(String(50), primary_key=True, comment='主键: code_date')
    code = Column(String(15), nullable=False, comment='baostock代码')
    code_name = Column(String(50), comment='证券名称')
    industry = Column(String(50), comment='所属行业大类')
    industryClassification = Column(String(50), comment='行业细分')
    date = Column(String(10), comment='行业分类日期')
    updated_at = Column(DateTime, comment='数据创建时间')


# ============================================================
# 步骤3: 利润表
# ============================================================

class ProfitRecord(Base):
    """利润表 — bs.query_profit_data()"""
    __tablename__ = "baostock_profit"
    __table_args__ = {'comment': '盈利能力指标表，含ROE/净利率/毛利率/净利润/EPS等'}
    id = Column(String(50), primary_key=True, comment='主键: code_statDate')
    code = Column(String(15), nullable=False, comment='baostock代码')
    statDate = Column(String(10), nullable=False, comment='统计日期 YYYY-MM-DD')
    roeAvg = Column(Float, comment='平均净资产收益率(%)')
    npMargin = Column(Float, comment='销售净利率(%)')
    gpMargin = Column(Float, comment='销售毛利率(%)')
    netProfit = Column(Float, comment='净利润(元)')
    epsTTM = Column(Float, comment='每股收益(元，滚动TTM)')
    MBRevenue = Column(Float, comment='主营营业收入(元)')
    totalShare = Column(Float, comment='总股本(万股)')
    liqaShare = Column(Float, comment='流通股本(万股)')
    updated_at = Column(DateTime, comment='数据更新时间')


# ============================================================
# 步骤4: 成长性数据
# ============================================================

class GrowthRecord(Base):
    """成长性数据 — bs.query_growth_data()"""
    __tablename__ = "baostock_growth"
    __table_args__ = {'comment': '成长能力指标表，含净资产/资产/净利润/EPS同比增长率'}
    id = Column(String(50), primary_key=True, comment='主键: code_statDate')
    code = Column(String(15), nullable=False, comment='baostock代码')
    statDate = Column(String(10), nullable=False, comment='统计日期 YYYY-MM-DD')
    YOYEquity = Column(Float, comment='净资产同比增长率(%)')
    YOYAsset = Column(Float, comment='总资产同比增长率(%)')
    YOYNI = Column(Float, comment='净利润同比增长率(%)')
    YOYEPSBasic = Column(Float, comment='基本每股收益同比增长率(%)')
    YOYPNI = Column(Float, comment='归属母公司净利润同比增长率(%)')
    updated_at = Column(DateTime, comment='数据更新时间')


# ============================================================
# 步骤5: 资产负债表
# ============================================================

class BalanceRecord(Base):
    """资产负债表 — bs.query_balance_data()"""
    __tablename__ = "baostock_balance"
    __table_args__ = {'comment': '偿债能力指标表，含流动比率/速动比率/现金比率/负债率等'}
    id = Column(String(50), primary_key=True, comment='主键: code_statDate')
    code = Column(String(15), nullable=False, comment='baostock代码')
    statDate = Column(String(10), nullable=False, comment='统计日期 YYYY-MM-DD')
    currentRatio = Column(Float, comment='流动比率=流动资产/流动负债')
    quickRatio = Column(Float, comment='速动比率=(流动资产-存货)/流动负债')
    cashRatio = Column(Float, comment='现金比率=货币资金/流动负债')
    YOYLiability = Column(Float, comment='负债总额同比增长率(%)')
    liabilityToAsset = Column(Float, comment='资产负债率=总负债/总资产')
    assetToEquity = Column(Float, comment='权益乘数=总资产/净资产')
    updated_at = Column(DateTime, comment='数据更新时间')


# ============================================================
# 步骤6: 现金流量表
# ============================================================

class CashFlowRecord(Base):
    """现金流量表 — bs.query_cash_flow_data()"""
    __tablename__ = "baostock_cash_flow"
    __table_args__ = {'comment': '现金流能力指标表，含经营现金流/投资现金流/筹资现金流比率'}
    id = Column(String(50), primary_key=True, comment='主键: code_statDate')
    code = Column(String(15), nullable=False, comment='baostock代码')
    statDate = Column(String(10), nullable=False, comment='统计日期 YYYY-MM-DD')
    CAToAsset = Column(Float, comment='流动资产/总资产')
    NCAToAsset = Column(Float, comment='非流动资产/总资产')
    tangibleAssetToAsset = Column(Float, comment='有形资产/总资产')
    ebitToInterest = Column(Float, comment='已获利息倍数=EBIT/利息费用')
    CFOToOR = Column(Float, comment='经营现金流/营业收入')
    CFOToNP = Column(Float, comment='经营现金流/净利润')
    CFOToGr = Column(Float, comment='经营现金流/营业收入增长率')
    updated_at = Column(DateTime, comment='数据更新时间')


# ============================================================
# 步骤7: 分红数据
# ============================================================

class DividendRecord(Base):
    """分红数据 — bs.query_dividend_data()"""
    __tablename__ = "baostock_dividend"
    __table_args__ = {'comment': '分红送股数据表，含现金分红/股票分红/送转比例等'}
    id = Column(String(50), primary_key=True, comment='主键: code_dividPlanDate_dividPayDate')
    code = Column(String(15), nullable=False, comment='baostock代码')
    dividPlanDate = Column(String(10), comment='分红方案公布日期')
    dividRegistDate = Column(String(10), comment='股权登记日期')
    dividOperateDate = Column(String(10), comment='除权除息日期')
    dividPayDate = Column(String(10), comment='红利发放日期')
    dividStockMarketDate = Column(String(10), comment='送转股上市日期')
    dividCashPsBeforeTax = Column(Float, comment='每股税前现金分红(元)')
    dividCashPsAfterTax = Column(Float, comment='每股税后现金分红(元)')
    dividStocksPs = Column(Float, comment='每股送转股数量(股)')
    dividCashStock = Column(Float, comment='每股现金替代金额')
    dividReserveToStockPs = Column(Float, comment='每股公积金转增股本(股)')
    updated_at = Column(DateTime, comment='数据更新时间')


# ============================================================
# 步骤8: 业绩快报
# ============================================================

class PerformanceRecord(Base):
    """业绩快报 — bs.query_performance_express_report()"""
    __tablename__ = "baostock_performance"
    __table_args__ = {'comment': '业绩快报数据，含ROE/EPS/总资产/总负债/净利润及同比增速'}
    id = Column(String(50), primary_key=True, comment='主键: code_performanceExpStatDate')
    code = Column(String(15), nullable=False, comment='baostock代码')
    performanceExpPubDate = Column(String(20), comment='业绩快报预计发布日期')
    performanceExpStatDate = Column(String(10), comment='业绩快报统计日期')
    performanceExpressROEWa = Column(Float, comment='加权平均净资产收益率(%)')
    performanceExpressEPS = Column(Float, comment='每股收益(元)')
    totalShare = Column(Float, comment='总股本(万股)')
    totalAssets = Column(Float, comment='总资产(元)')
    totalLiab = Column(Float, comment='总负债(元)')
    totalEquity = Column(Float, comment='净资产(元)')
    BPS = Column(Float, comment='每股净资产(元)')
    netProfitYOY = Column(Float, comment='净利润同比增长率(%)')
    netProfit = Column(Float, comment='净利润(元)')
    performanceExpressPubDate = Column(String(20), comment='业绩快报实际发布日期')
    updated_at = Column(DateTime, comment='数据更新时间')


# ============================================================
# 数据库接口实现
# ============================================================

class BaostockDatabase(BaseDatabase):
    """将 Baostock 数据桥接到 vn.py 数据库接口"""

    def __init__(self, db_url: str = "postgresql://postgres:postgres@localhost:5432/baostock_vnpy"):
        self.engine = create_engine(db_url, echo=False)
        Base.metadata.create_all(self.engine)

    # ========== Bar 数据操作 ==========

    def save_bar_data(self, bars: list[BarData], stream: bool = False, batch_size: int = 10000) -> bool:
        if not bars:
            return True

        records = []
        now = datetime.now()
        for bar in bars:
            record_id = f"{bar.symbol}_{bar.exchange.value}_{bar.interval.value}_{bar.datetime.strftime('%Y%m%d%H%M')}"
            records.append(BarRecord(
                id=record_id,
                symbol=bar.symbol,
                exchange=bar.exchange.value,
                datetime=bar.datetime,
                interval=bar.interval.value,
                open=bar.open_price,
                high=bar.high_price,
                low=bar.low_price,
                close=bar.close_price,
                volume=bar.volume,
                turnover=bar.turnover,
                gateway_name=bar.gateway_name,
                updated_at=now,
            ))

        for i in range(0, len(records), batch_size):
            chunk = records[i:i + batch_size]
            t0 = time.time()
            try:
                with Session(self.engine) as session:
                    for record in chunk:
                        session.merge(record)
                    session.commit()
                elapsed = time.time() - t0
                msg = f"事务提交 | baostock_bar_data | 入库 {len(chunk):>5} 条 | 耗时 {elapsed:.3f}s"
                db_logger.info(msg)
                for h in db_logger.handlers:
                    h.flush()
            except Exception as e:
                db_logger.error(f"数据库写入失败: {e}")
                raise
        return True

    def load_bar_data(
        self, symbol: str, exchange, interval,
        start: datetime, end: datetime,
    ) -> list[BarData]:
        """加载K线数据

        Args:
            symbol: 股票代码（如 600000）
            exchange: 交易所枚举（Exchange.SSE / Exchange.SZSE）或字符串
            interval: 周期（Interval 枚举或字符串：'d', 'w', 'monthly', '5m'）
            start: 开始时间
            end: 结束时间
        """
        # 处理 interval：支持 Interval 枚举和字符串
        if hasattr(interval, 'value'):
            interval_val = interval.value
        else:
            interval_val = interval

        # 处理 exchange：支持 Exchange 枚举和字符串
        if hasattr(exchange, 'value'):
            exchange_val = exchange.value
        else:
            exchange_val = exchange

        # 根据 interval 选择正确的表（'1m' 和 '5m' 都映射到 5分钟表）
        record_map = {
            '5m': Bar5MinRecord,
            '1m': Bar5MinRecord,  # 历史数据用 '1m'
            'd': BarRecord,
            'w': WeeklyBarRecord,
            'monthly': MonthlyBarRecord,
        }
        record_class = record_map.get(interval_val, BarRecord)

        with Session(self.engine) as session:
            stmt = (
                select(record_class)
                .where(
                    record_class.symbol == symbol,
                    record_class.exchange == exchange_val,
                    record_class.datetime >= start,
                    record_class.datetime <= end,
                )
                .order_by(record_class.datetime)
            )
            rows = session.execute(stmt).scalars().all()

        return [
            BarData(
                symbol=r.symbol, exchange=Exchange(r.exchange), datetime=r.datetime,
                interval=r.interval,
                open_price=r.open, high_price=r.high,
                low_price=r.low, close_price=r.close, volume=r.volume,
                turnover=r.turnover, gateway_name=r.gateway_name,
            ) for r in rows
        ]

    def delete_bar_data(self, symbol: str, exchange, interval) -> int:
        with Session(self.engine) as session:
            exchange_val = exchange.value if hasattr(exchange, 'value') else exchange
            interval_val = interval.value if hasattr(interval, 'value') else interval
            stmt = select(BarRecord).where(
                BarRecord.symbol == symbol, BarRecord.exchange == exchange.value,
                BarRecord.interval == interval.value,
            )
            rows = session.execute(stmt).scalars().all()
            for r in rows:
                session.delete(r)
            session.commit()
        return len(rows)

    # ========== 信号表操作 ==========

    def save_signals(self, signals: list[dict]) -> int:
        now = datetime.now()
        with Session(self.engine) as session:
            for sig in signals:
                record_id = f"{sig['signal_date']}_{sig['symbol']}_{sig['exchange']}"
                record = SignalRecord(
                    id=record_id, signal_date=sig["signal_date"], symbol=sig["symbol"],
                    exchange=sig["exchange"], direction=sig["direction"],
                    score=sig["score"], reason=sig.get("reason", ""),
                    target_weight=sig["target_weight"], updated_at=now,
                )
                session.merge(record)
            session.commit()
        return len(signals)

    def load_signals(self, symbol: str, exchange: str, date: str) -> dict | None:
        record_id = f"{date}_{symbol}_{exchange}"
        with Session(self.engine) as session:
            row = session.execute(select(SignalRecord).where(SignalRecord.id == record_id)).scalar()
        if row:
            return {
                "signal_date": row.signal_date, "symbol": row.symbol,
                "exchange": row.exchange, "direction": row.direction,
                "score": row.score, "reason": row.reason, "target_weight": row.target_weight,
            }
        return None

    def load_all_signals(self, date: str) -> list[dict]:
        with Session(self.engine) as session:
            rows = session.execute(
                select(SignalRecord).where(SignalRecord.signal_date == date)
            ).scalars().all()
        return [
            {"signal_date": r.signal_date, "symbol": r.symbol, "exchange": r.exchange,
             "direction": r.direction, "score": r.score, "reason": r.reason,
             "target_weight": r.target_weight} for r in rows
        ]

    def delete_expired_signals(self, before_date: str) -> int:
        with Session(self.engine) as session:
            rows = session.execute(
                select(SignalRecord).where(SignalRecord.signal_date < before_date)
            ).scalars().all()
            for r in rows:
                session.delete(r)
            session.commit()
        return len(rows)

    # ========== Tick 数据 ==========

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        return True

    def load_tick_data(self, symbol: str, exchange: Exchange, start: datetime, end: datetime) -> list[TickData]:
        return []

    def delete_tick_data(self, symbol: str, exchange: Exchange) -> int:
        return 0

    # ========== 步骤1: 股票列表 ==========

    def save_stock_list(self, records: list[dict]) -> int:
        if not records:
            return 0
        db_logger.info(f"save_stock_list 被调用, records={len(records)} 条")
        t0 = time.time()
        with Session(self.engine) as session:
            for rec in records:
                record = StockListRecord(
                    code=rec["code"], code_name=rec.get("code_name", ""),
                    industry=rec.get("industry", ""),
                    industryClassification=rec.get("industryClassification", ""),
                    updated_at=datetime.now(),
                )
                session.merge(record)
            session.commit()
        elapsed = time.time() - t0
        db_logger.info(f"事务提交 | baostock_stock_list | 入库 {len(records):>5} 条 | 耗时 {elapsed:.3f}s")
        for h in db_logger.handlers:
            h.flush()
        return len(records)

    def get_stock_list(self, limit: int = 0) -> list[dict]:
        with Session(self.engine) as session:
            stmt = select(StockListRecord).order_by(StockListRecord.code)
            if limit > 0:
                stmt = stmt.limit(limit)
            rows = session.execute(stmt).scalars().all()
        return [{"code": r.code, "code_name": r.code_name, "industry": r.industry,
                 "industryClassification": r.industryClassification} for r in rows]

    def get_stock_count(self) -> int:
        with Session(self.engine) as session:
            return session.execute(select(func.count()).select_from(StockListRecord)).scalar()

    # ========== 股票基础信息(完整版) ==========

    def save_basic_info(self, records: list[dict]) -> int:
        """保存股票基础信息（完整版）"""
        if not records:
            return 0
        db_logger.info(f"save_basic_info 被调用, records={len(records)} 条")
        t0 = time.time()
        with Session(self.engine) as session:
            for rec in records:
                record = BasicInfoRecord(
                    baostock_code=rec["baostock_code"],
                    security_code=rec["security_code"],
                    security_name=rec["security_name"],
                    exchange=rec["exchange"],
                    board=rec["board"],
                    status=rec["status"],
                    market=rec["market"],
                    is_hs=rec.get("is_hs"),
                    list_date=rec["list_date"],
                    delist_date=rec.get("delist_date"),
                    industry=rec.get("industry"),
                    province=rec.get("province"),
                    city=rec.get("city"),
                    website=rec.get("website"),
                    sec_company=rec.get("sec_company"),
                    underlying_code=rec.get("underlying_code"),
                    updated_at=datetime.now(),
                )
                session.merge(record)
            session.commit()
        elapsed = time.time() - t0
        db_logger.info(f"事务提交 | baostock_basic | 入库 {len(records):>5} 条 | 耗时 {elapsed:.3f}s")
        for h in db_logger.handlers:
            h.flush()
        return len(records)

    def get_basic_info(self, baostock_code: str = None, exchange: str = None,
                       board: str = None, industry: str = None) -> list[dict]:
        """获取股票基础信息（支持条件查询）"""
        with Session(self.engine) as session:
            stmt = select(BasicInfoRecord).where(BasicInfoRecord.status == "1")
            if baostock_code:
                stmt = stmt.where(BasicInfoRecord.baostock_code == baostock_code)
            if exchange:
                stmt = stmt.where(BasicInfoRecord.exchange == exchange)
            if board:
                stmt = stmt.where(BasicInfoRecord.board == board)
            if industry:
                stmt = stmt.where(BasicInfoRecord.industry == industry)
            rows = session.execute(stmt).scalars().all()
        return [
            {
                "baostock_code": r.baostock_code,
                "security_code": r.security_code,
                "security_name": r.security_name,
                "exchange": r.exchange,
                "board": r.board,
                "status": r.status,
                "market": r.market,
                "is_hs": r.is_hs,
                "list_date": r.list_date,
                "delist_date": r.delist_date,
                "industry": r.industry,
                "province": r.province,
                "city": r.city,
                "website": r.website,
                "sec_company": r.sec_company,
                "underlying_code": r.underlying_code,
            }
            for r in rows
        ]

    def get_basic_count(self) -> int:
        """获取股票基础信息总数"""
        with Session(self.engine) as session:
            return len(session.execute(select(BasicInfoRecord)).scalars().all())

    # ========== 步骤1.5: 每日指标 ==========

    def save_daily_basic(self, records: list[dict]) -> int:
        with Session(self.engine) as session:
            for rec in records:
                record_id = f"{rec['code']}_{rec['date']}"
                record = DailyBasicRecord(
                    id=record_id, code=rec["code"], date=rec["date"],
                    close=rec.get("close"), peTTM=rec.get("peTTM"),
                    pbMRQ=rec.get("pbMRQ"), psTTM=rec.get("psTTM"),
                    pcfNcfTTM=rec.get("pcfNcfTTM"), isST=rec.get("isST"),
                    turn=rec.get("turn"), volume=rec.get("volume"),
                    amount=rec.get("amount"), updated_at=datetime.now(),
                )
                session.merge(record)
            session.commit()
        return len(records)

    def get_daily_basic(self, code: str, date: str = None) -> list[dict]:
        with Session(self.engine) as session:
            stmt = select(DailyBasicRecord).where(DailyBasicRecord.code == code)
            if date:
                stmt = stmt.where(DailyBasicRecord.date == date)
            rows = session.execute(stmt).scalars().all()
        return [
            {"code": r.code, "date": r.date, "close": r.close,
             "peTTM": r.peTTM, "pbMRQ": r.pbMRQ, "psTTM": r.psTTM,
             "pcfNcfTTM": r.pcfNcfTTM, "isST": r.isST,
             "turn": r.turn, "volume": r.volume, "amount": r.amount} for r in rows
        ]

    def get_latest_daily_basic(self) -> list[dict]:
        """获取最新一日的全市场指标（用于多因子选股）"""
        with Session(self.engine) as session:
            # 先找出最新日期
            latest = session.execute(
                select(DailyBasicRecord.date).order_by(DailyBasicRecord.date.desc()).limit(1)
            ).scalar()
            if not latest:
                return []
            # 再取该日全部数据
            rows = session.execute(
                select(DailyBasicRecord).where(DailyBasicRecord.date == latest)
            ).scalars().all()
        return [
            {"code": r.code, "date": r.date, "close": r.close,
             "peTTM": r.peTTM, "pbMRQ": r.pbMRQ, "psTTM": r.psTTM,
             "pcfNcfTTM": r.pcfNcfTTM, "isST": r.isST,
             "turn": r.turn, "volume": r.volume, "amount": r.amount} for r in rows
        ]

    # ========== 步骤2: 行业分类 ==========

    def save_stock_industry(self, records: list[dict]) -> int:
        if not records:
            return 0
        db_logger.info(f"save_stock_industry 被调用, records={len(records)} 条")
        t0 = time.time()
        with Session(self.engine) as session:
            for rec in records:
                record_id = f"{rec['code']}_{rec.get('date', '')}"
                record = StockIndustryRecord(
                    id=record_id, code=rec["code"],
                    code_name=rec.get("code_name", ""),
                    industry=rec.get("industry", ""),
                    industryClassification=rec.get("industryClassification", ""),
                    date=rec.get("date", ""),
                    updated_at=datetime.now(),
                )
                session.merge(record)
            session.commit()
        elapsed = time.time() - t0
        db_logger.info(f"事务提交 | baostock_stock_industry | 入库 {len(records):>5} 条 | 耗时 {elapsed:.3f}s")
        for h in db_logger.handlers:
            h.flush()
        return len(records)

    def get_stock_industry(self, code: str = None) -> list[dict]:
        with Session(self.engine) as session:
            stmt = select(StockIndustryRecord)
            if code:
                stmt = stmt.where(StockIndustryRecord.code == code)
            rows = session.execute(stmt).scalars().all()
        return [{"code": r.code, "code_name": r.code_name, "industry": r.industry,
                 "industryClassification": r.industryClassification, "date": r.date} for r in rows]

    # ========== 步骤3: 利润表 ==========

    def save_profit(self, records: list[dict]) -> int:
        if not records:
            return 0
        db_logger.info(f"save_profit 被调用, records={len(records)} 条")
        t0 = time.time()
        with Session(self.engine) as session:
            for rec in records:
                record_id = f"{rec['code']}_{rec['statDate']}"
                record = ProfitRecord(
                    id=record_id, code=rec["code"], statDate=rec["statDate"],
                    roeAvg=rec.get("roeAvg"), npMargin=rec.get("npMargin"),
                    gpMargin=rec.get("gpMargin"), netProfit=rec.get("netProfit"),
                    epsTTM=rec.get("epsTTM"), MBRevenue=rec.get("MBRevenue"),
                    totalShare=rec.get("totalShare"), liqaShare=rec.get("liqaShare"),
                    updated_at=datetime.now(),
                )
                session.merge(record)
            session.commit()
        elapsed = time.time() - t0
        db_logger.info(f"事务提交 | baostock_profit | 入库 {len(records):>5} 条 | 耗时 {elapsed:.3f}s")
        for h in db_logger.handlers:
            h.flush()
        return len(records)

    def get_profit(self, code: str, statDate: str = None) -> list[dict]:
        with Session(self.engine) as session:
            stmt = select(ProfitRecord).where(ProfitRecord.code == code)
            if statDate:
                stmt = stmt.where(ProfitRecord.statDate == statDate)
            rows = session.execute(stmt).scalars().all()
        return [
            {"code": r.code, "statDate": r.statDate, "roeAvg": r.roeAvg,
             "npMargin": r.npMargin, "gpMargin": r.gpMargin, "netProfit": r.netProfit,
             "epsTTM": r.epsTTM, "liqaShare": r.liqaShare} for r in rows
        ]

    # ========== 步骤4: 成长性数据 ==========

    def save_growth(self, records: list[dict]) -> int:
        if not records:
            return 0
        db_logger.info(f"save_growth 被调用, records={len(records)} 条")
        t0 = time.time()
        with Session(self.engine) as session:
            for rec in records:
                record_id = f"{rec['code']}_{rec['statDate']}"
                record = GrowthRecord(
                    id=record_id, code=rec["code"], statDate=rec["statDate"],
                    YOYEquity=rec.get("YOYEquity"), YOYAsset=rec.get("YOYAsset"),
                    YOYNI=rec.get("YOYNI"), YOYEPSBasic=rec.get("YOYEPSBasic"),
                    YOYPNI=rec.get("YOYPNI"), updated_at=datetime.now(),
                )
                session.merge(record)
            session.commit()
        elapsed = time.time() - t0
        db_logger.info(f"事务提交 | baostock_growth | 入库 {len(records):>5} 条 | 耗时 {elapsed:.3f}s")
        for h in db_logger.handlers:
            h.flush()
        return len(records)

    def get_growth(self, code: str, statDate: str = None) -> list[dict]:
        with Session(self.engine) as session:
            stmt = select(GrowthRecord).where(GrowthRecord.code == code)
            if statDate:
                stmt = stmt.where(GrowthRecord.statDate == statDate)
            rows = session.execute(stmt).scalars().all()
        return [
            {"code": r.code, "statDate": r.statDate, "YOYEquity": r.YOYEquity,
             "YOYAsset": r.YOYAsset, "YOYNI": r.YOYNI, "YOYEPSBasic": r.YOYEPSBasic,
             "YOYPNI": r.YOYPNI} for r in rows
        ]

    # ========== 步骤5: 资产负债表 ==========

    def save_balance(self, records: list[dict]) -> int:
        if not records:
            return 0
        db_logger.info(f"save_balance 被调用, records={len(records)} 条")
        t0 = time.time()
        with Session(self.engine) as session:
            for rec in records:
                record_id = f"{rec['code']}_{rec['statDate']}"
                record = BalanceRecord(
                    id=record_id, code=rec["code"], statDate=rec["statDate"],
                    currentRatio=rec.get("currentRatio"), quickRatio=rec.get("quickRatio"),
                    cashRatio=rec.get("cashRatio"), YOYLiability=rec.get("YOYLiability"),
                    liabilityToAsset=rec.get("liabilityToAsset"), assetToEquity=rec.get("assetToEquity"),
                    updated_at=datetime.now(),
                )
                session.merge(record)
            session.commit()
        elapsed = time.time() - t0
        db_logger.info(f"事务提交 | baostock_balance | 入库 {len(records):>5} 条 | 耗时 {elapsed:.3f}s")
        for h in db_logger.handlers:
            h.flush()
        return len(records)

    def get_balance(self, code: str, statDate: str = None) -> list[dict]:
        with Session(self.engine) as session:
            stmt = select(BalanceRecord).where(BalanceRecord.code == code)
            if statDate:
                stmt = stmt.where(BalanceRecord.statDate == statDate)
            rows = session.execute(stmt).scalars().all()
        return [
            {"code": r.code, "statDate": r.statDate,
             "currentRatio": r.currentRatio, "quickRatio": r.quickRatio,
             "cashRatio": r.cashRatio, "YOYLiability": r.YOYLiability,
             "liabilityToAsset": r.liabilityToAsset, "assetToEquity": r.assetToEquity} for r in rows
        ]

    # ========== 步骤6: 现金流量表 ==========

    def save_cash_flow(self, records: list[dict]) -> int:
        if not records:
            return 0
        db_logger.info(f"save_cash_flow 被调用, records={len(records)} 条")
        t0 = time.time()
        with Session(self.engine) as session:
            for rec in records:
                record_id = f"{rec['code']}_{rec['statDate']}"
                record = CashFlowRecord(
                    id=record_id, code=rec["code"], statDate=rec["statDate"],
                    CAToAsset=rec.get("CAToAsset"), NCAToAsset=rec.get("NCAToAsset"),
                    tangibleAssetToAsset=rec.get("tangibleAssetToAsset"),
                    ebitToInterest=rec.get("ebitToInterest"),
                    CFOToOR=rec.get("CFOToOR"), CFOToNP=rec.get("CFOToNP"),
                    CFOToGr=rec.get("CFOToGr"), updated_at=datetime.now(),
                )
                session.merge(record)
            session.commit()
        elapsed = time.time() - t0
        db_logger.info(f"事务提交 | baostock_cash_flow | 入库 {len(records):>5} 条 | 耗时 {elapsed:.3f}s")
        for h in db_logger.handlers:
            h.flush()
        return len(records)

    def get_cash_flow(self, code: str, statDate: str = None) -> list[dict]:
        with Session(self.engine) as session:
            stmt = select(CashFlowRecord).where(CashFlowRecord.code == code)
            if statDate:
                stmt = stmt.where(CashFlowRecord.statDate == statDate)
            rows = session.execute(stmt).scalars().all()
        return [
            {"code": r.code, "statDate": r.statDate,
             "CAToAsset": r.CAToAsset, "NCAToAsset": r.NCAToAsset,
             "tangibleAssetToAsset": r.tangibleAssetToAsset, "ebitToInterest": r.ebitToInterest,
             "CFOToOR": r.CFOToOR, "CFOToNP": r.CFOToNP, "CFOToGr": r.CFOToGr} for r in rows
        ]

    # ========== 步骤7: 分红数据 ==========

    def save_dividend(self, records: list[dict]) -> int:
        if not records:
            return 0
        db_logger.info(f"save_dividend 被调用, records={len(records)} 条")
        t0 = time.time()
        with Session(self.engine) as session:
            for rec in records:
                record_id = f"{rec['code']}_{rec.get('dividPlanDate','')}_{rec.get('dividPayDate','')}"
                record = DividendRecord(
                    id=record_id, code=rec["code"],
                    dividPlanDate=rec.get("dividPlanDate", ""),
                    dividRegistDate=rec.get("dividRegistDate", ""),
                    dividOperateDate=rec.get("dividOperateDate", ""),
                    dividPayDate=rec.get("dividPayDate", ""),
                    dividStockMarketDate=rec.get("dividStockMarketDate", ""),
                    dividCashPsBeforeTax=rec.get("dividCashPsBeforeTax"),
                    dividCashPsAfterTax=rec.get("dividCashPsAfterTax"),
                    dividStocksPs=rec.get("dividStocksPs"),
                    dividCashStock=rec.get("dividCashStock"),
                    dividReserveToStockPs=rec.get("dividReserveToStockPs"),
                    updated_at=datetime.now(),
                )
                session.merge(record)
            session.commit()
        elapsed = time.time() - t0
        db_logger.info(f"事务提交 | baostock_dividend | 入库 {len(records):>5} 条 | 耗时 {elapsed:.3f}s")
        for h in db_logger.handlers:
            h.flush()
        return len(records)

    def get_dividend(self, code: str) -> list[dict]:
        with Session(self.engine) as session:
            rows = session.execute(
                select(DividendRecord).where(DividendRecord.code == code)
            ).scalars().all()
        return [
            {"code": r.code, "dividPlanDate": r.dividPlanDate,
             "dividPayDate": r.dividPayDate, "dividCashPsBeforeTax": r.dividCashPsBeforeTax,
             "dividCashPsAfterTax": r.dividCashPsAfterTax, "dividStocksPs": r.dividStocksPs,
             "dividCashStock": r.dividCashStock} for r in rows
        ]

    # ========== 步骤8: 业绩快报 ==========

    def save_performance(self, records: list[dict]) -> int:
        if not records:
            return 0
        db_logger.info(f"save_performance 被调用, records={len(records)} 条")
        t0 = time.time()
        with Session(self.engine) as session:
            for rec in records:
                record_id = f"{rec['code']}_{rec.get('performanceExpStatDate', '')}"
                record = PerformanceRecord(
                    id=record_id, code=rec["code"],
                    performanceExpPubDate=rec.get("performanceExpPubDate", ""),
                    performanceExpStatDate=rec.get("performanceExpStatDate", ""),
                    performanceExpressROEWa=rec.get("performanceExpressROEWa"),
                    performanceExpressEPS=rec.get("performanceExpressEPS"),
                    totalShare=rec.get("totalShare"), totalAssets=rec.get("totalAssets"),
                    totalLiab=rec.get("totalLiab"), totalEquity=rec.get("totalEquity"),
                    BPS=rec.get("BPS"), netProfitYOY=rec.get("netProfitYOY"),
                    netProfit=rec.get("netProfit"),
                    performanceExpressPubDate=rec.get("performanceExpressPubDate", ""),
                    updated_at=datetime.now(),
                )
                session.merge(record)
            session.commit()
        elapsed = time.time() - t0
        db_logger.info(f"事务提交 | baostock_performance | 入库 {len(records):>5} 条 | 耗时 {elapsed:.3f}s")
        for h in db_logger.handlers:
            h.flush()
        return len(records)

    def get_performance(self, code: str) -> list[dict]:
        with Session(self.engine) as session:
            rows = session.execute(
                select(PerformanceRecord).where(PerformanceRecord.code == code)
            ).scalars().all()
        return [
            {"code": r.code, "statDate": r.performanceExpStatDate,
             "ROEWa": r.performanceExpressROEWa, "EPS": r.performanceExpressEPS,
             "netProfit": r.netProfit, "netProfitYOY": r.netProfitYOY,
             "totalAssets": r.totalAssets, "totalLiab": r.totalLiab,
             "BPS": r.BPS} for r in rows
        ]

    # ========== API 调用统计 ==========

    def increment_call_count(self, interface_name: str, count: int = 1) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        with Session(self.engine) as session:
            row = session.execute(select(ApiCallLog).where(
                ApiCallLog.interface_name == interface_name,
                ApiCallLog.call_date == today,
            )).scalar()
            if row:
                row.call_count += count
                row.updated_at = datetime.now()
            else:
                session.add(ApiCallLog(
                    interface_name=interface_name, call_date=today,
                    call_count=count, updated_at=datetime.now(),
                ))
            session.commit()

    def get_call_stats(self, date: str = None) -> list[dict]:
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")
        with Session(self.engine) as session:
            rows = session.execute(
                select(ApiCallLog).where(ApiCallLog.call_date == date)
                .order_by(ApiCallLog.call_count.desc())
            ).scalars().all()
        return [{"interface_name": r.interface_name, "call_count": r.call_count,
                 "updated_at": r.updated_at} for r in rows]

    def get_total_calls(self, date: str = None) -> int:
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")
        with Session(self.engine) as session:
            counts = session.execute(
                select(ApiCallLog.call_count).where(ApiCallLog.call_date == date)
            ).scalars().all()
        return sum(counts)

    def get_bar_overview(self) -> list[BarOverview]:
        from sqlalchemy import func
        overviews = []
        # Map table to interval string
        table_map = [
            (Bar5MinRecord, '5m'),
            (BarRecord, 'd'),
            (WeeklyBarRecord, 'w'),
            (MonthlyBarRecord, 'monthly'),
        ]
        # Map interval string to Interval enum (use closest match)
        interval_map = {
            '5m': Interval.MINUTE,  # 5分钟也用MINUTE
            '1m': Interval.MINUTE,
            'd': Interval.DAILY,
            'w': Interval.WEEKLY,
            'monthly': Interval.WEEKLY,  # 月线用WEEKLY代理
        }
        with Session(self.engine) as session:
            for record_class, interval_str in table_map:
                rows = session.execute(
                    select(
                        record_class.symbol,
                        record_class.exchange,
                        func.count().label("count"),
                        func.min(record_class.datetime).label("start"),
                        func.max(record_class.datetime).label("end"),
                    )
                    .group_by(record_class.symbol, record_class.exchange)
                ).all()
                for symbol, exchange, count, start, end in rows:
                    overviews.append(BarOverview(
                        symbol=symbol, exchange=Exchange(exchange),
                        interval=interval_map.get(interval_str, Interval.DAILY),
                        count=count, start=start, end=end,
                    ))
        return overviews

    def get_tick_overview(self) -> list[TickOverview]:
        return []

    # ========== 周线数据 ==========

    def save_bar_weekly(self, bars: list[BarData], batch_size: int = 10000) -> bool:
        if not bars:
            return True
        now = datetime.now()
        records = []
        for bar in bars:
            record_id = f"{bar.symbol}_{bar.exchange.value}_{bar.interval.value}_{bar.datetime.strftime('%Y%m%d%H%M')}"
            records.append(WeeklyBarRecord(
                id=record_id, symbol=bar.symbol, exchange=bar.exchange.value,
                datetime=bar.datetime, interval=bar.interval.value,
                open=bar.open_price, high=bar.high_price, low=bar.low_price,
                close=bar.close_price, volume=bar.volume, turnover=bar.turnover,
                gateway_name=bar.gateway_name, updated_at=now,
            ))
        for i in range(0, len(records), batch_size):
            chunk = records[i:i + batch_size]
            t0 = time.time()
            try:
                with Session(self.engine) as session:
                    for record in chunk:
                        session.merge(record)
                    session.commit()
                elapsed = time.time() - t0
                msg = f"事务提交 | baostock_bar_weekly | 入库 {len(chunk):>5} 条 | 耗时 {elapsed:.3f}s"
                db_logger.info(msg)
                for h in db_logger.handlers:
                    h.flush()
            except Exception as e:
                db_logger.error(f"数据库写入失败: {e}")
                raise
        return True

    # ========== 月线数据 ==========

    def save_bar_monthly(self, bars: list[BarData], batch_size: int = 10000) -> bool:
        if not bars:
            return True
        now = datetime.now()
        records = []
        for bar in bars:
            interval_val = bar.interval.value if hasattr(bar.interval, 'value') else bar.interval
            record_id = f"{bar.symbol}_{bar.exchange.value}_{interval_val}_{bar.datetime.strftime('%Y%m%d%H%M')}"
            records.append(MonthlyBarRecord(
                id=record_id, symbol=bar.symbol, exchange=bar.exchange.value,
                datetime=bar.datetime, interval=interval_val,
                open=bar.open_price, high=bar.high_price, low=bar.low_price,
                close=bar.close_price, volume=bar.volume, turnover=bar.turnover,
                gateway_name=bar.gateway_name, updated_at=now,
            ))
        for i in range(0, len(records), batch_size):
            chunk = records[i:i + batch_size]
            t0 = time.time()
            try:
                with Session(self.engine) as session:
                    for record in chunk:
                        session.merge(record)
                    session.commit()
                elapsed = time.time() - t0
                msg = f"事务提交 | baostock_bar_monthly | 入库 {len(chunk):>5} 条 | 耗时 {elapsed:.3f}s"
                db_logger.info(msg)
                for h in db_logger.handlers:
                    h.flush()
            except Exception as e:
                db_logger.error(f"数据库写入失败: {e}")
                raise
        return True

    # ========== 5分钟线数据 ==========

    def save_bar_5min(self, bars: list[BarData], batch_size: int = 10000) -> bool:
        db_logger.debug(f"save_bar_5min 被调用, bars={len(bars)} 条")
        if not bars:
            db_logger.warning("save_bar_5min: bars 为空，跳过")
            return True
        now = datetime.now()
        records = []
        for bar in bars:
            record_id = f"{bar.symbol}_{bar.exchange.value}_{bar.interval.value}_{bar.datetime.strftime('%Y%m%d%H%M')}_{bar.volume}"
            records.append(Bar5MinRecord(
                id=record_id, symbol=bar.symbol, exchange=bar.exchange.value,
                datetime=bar.datetime, interval=bar.interval.value,
                open=bar.open_price, high=bar.high_price, low=bar.low_price,
                close=bar.close_price, volume=bar.volume, turnover=bar.turnover,
                gateway_name=bar.gateway_name, updated_at=now,
            ))
        for i in range(0, len(records), batch_size):
            chunk = records[i:i + batch_size]
            t0 = time.time()
            try:
                with Session(self.engine) as session:
                    for record in chunk:
                        session.merge(record)
                    session.commit()
                elapsed = time.time() - t0
                msg = f"事务提交 | baostock_bar_5min | 入库 {len(chunk):>5} 条 | 耗时 {elapsed:.3f}s"
                db_logger.debug(msg)
                # 强制写入磁盘（双重保险：logger handler + 直接文件追加）
                for h in db_logger.handlers:
                    h.flush()
            except Exception as e:
                db_logger.error(f"数据库写入失败: {e}")
                raise
        return True
