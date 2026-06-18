"""
vn.py CTA 策略 — 读取外部 Baostock 分析信号并执行

职责:
- 不做重度计算（避免阻塞事件循环）
- 每日开盘读取 signal_generator 生成的信号
- 根据信号执行开仓/平仓
- 内置止损止盈风控

架构关系:
    signal_generator.py (外部进程) → 写入 DB → 本策略读取 → 下单执行
"""

from vnpy_ctastrategy import CtaTemplate, BarGenerator, ArrayManager
from vnpy.trader.object import BarData, TickData, OrderData, TradeData
from vnpy.trader.constant import Exchange, Interval
from datetime import datetime, time


class BaostockSignalStrategy(CtaTemplate):
    """读取外部 Baostock 分析信号 + vn.py 执行的完整策略"""

    author = "Baostock-Integration"

    # 策略参数
    max_position_per_stock = 0.05  # 单票最大仓位5%
    stop_loss_pct = 0.05  # 止损5%
    take_profit_pct = 0.10  # 止盈10%

    parameters = ["max_position_per_stock", "stop_loss_pct", "take_profit_pct"]
    variables = ["entry_price", "current_signal", "signal_loaded"]

    def __init__(self, cta_engine, strategy_name: str, vt_symbol: str, setting: dict):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()

        # 数据库连接（延迟初始化，避免策略加载时数据库不可用）
        self.db = None

        # 状态变量
        self.entry_price = 0.0
        self.current_signal = None
        self.signal_loaded = False
        self.last_signal_date = ""

    def on_init(self):
        self.write_log("策略初始化，加载数据库连接...")
        try:
            import os
            from baostock_database import BaostockDatabase
            db_url = os.environ.get(
                "BAOSTOCK_DB_URL",
                "postgresql://postgres:postgres@localhost:5432/baostock_vnpy"
            )
            self.db = BaostockDatabase(db_url)
            self.write_log("数据库连接成功")
        except Exception as e:
            self.write_log(f"数据库连接失败: {e}")
            self.db = None

        self.load_signal()
        self.load_bar(10)
        self.write_log(f"当前信号: {self.current_signal}")

    def on_start(self):
        self.write_log("策略启动")
        self.put_event()

    def on_stop(self):
        self.write_log("策略停止")
        self.put_event()

    def on_tick(self, tick: TickData):
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        # 每天 9:30 重新加载当日信号
        if bar.datetime.time() == time(9, 30):
            self.load_signal()

        self.am.update_bar(bar)
        if not self.am.inited:
            return

        # ========== 有持仓时的风控逻辑 ==========
        if self.pos > 0:
            pnl_pct = (bar.close_price - self.entry_price) / self.entry_price

            if pnl_pct <= -self.stop_loss_pct:
                self.sell(bar.close_price * 0.99, abs(self.pos))
                self.write_log(f"止损触发: {pnl_pct:.2%}")
                self.entry_price = 0.0
                return

            if pnl_pct >= self.take_profit_pct:
                self.sell(bar.close_price * 0.99, abs(self.pos))
                self.write_log(f"止盈触发: {pnl_pct:.2%}")
                self.entry_price = 0.0
                return

            if self.current_signal is None:
                self.sell(bar.close_price * 0.99, abs(self.pos))
                self.write_log("信号消失，平仓")
                self.entry_price = 0.0
                return

        # ========== 无持仓时的开仓逻辑 ==========
        elif self.pos == 0 and self.current_signal is not None:
            if self.current_signal.get("direction") == "LONG":
                target_weight = self.current_signal.get("target_weight", 0.05)
                target_weight = min(target_weight, self.max_position_per_stock)

                capital = getattr(self.cta_engine, "capital", 1000000)
                buy_size = int(capital * target_weight / bar.close_price / 100) * 100

                if buy_size >= 100:
                    self.buy(bar.close_price * 1.01, buy_size)
                    self.entry_price = bar.close_price
                    self.write_log(
                        f"开仓: {buy_size}股 @ {bar.close_price:.2f}, 权重{target_weight:.1%}"
                    )

        self.put_event()

    def on_trade(self, trade: TradeData):
        if trade.direction.value == "多":
            self.write_log(f"买入成交: {trade.volume}股 @ {trade.price:.2f}")
        else:
            self.write_log(f"卖出成交: {trade.volume}股 @ {trade.price:.2f}")
        self.put_event()

    def on_order(self, order: OrderData):
        pass

    def load_signal(self):
        """从数据库读取今日信号"""
        if not self.db:
            self.write_log("数据库未连接，跳过信号加载")
            return

        today = datetime.now().strftime("%Y-%m-%d")

        if today == self.last_signal_date:
            return

        symbol = self.vt_symbol.split(".")[0]
        exchange = self.vt_symbol.split(".")[1] if "." in self.vt_symbol else "SSE"

        try:
            signal = self.db.load_signals(symbol, exchange, today)
            if signal:
                self.current_signal = signal
                self.signal_loaded = True
                self.last_signal_date = today
                self.write_log(
                    f"读取到信号: 评分={signal['score']:.2f}, "
                    f"权重={signal['target_weight']:.1%}, 原因={signal['reason']}"
                )
            else:
                self.current_signal = None
                self.signal_loaded = False
                self.last_signal_date = today
                self.write_log(f"今日无信号: {symbol}")
        except Exception as e:
            self.write_log(f"信号读取失败: {e}")
            self.current_signal = None
            self.signal_loaded = False
            self.last_signal_date = today
