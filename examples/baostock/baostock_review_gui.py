"""
Baostock 多因子选股与复盘终端

集成:
- 股票列表（从数据库动态加载）
- 多因子选股（PE/PB/成交额/质量排名）
- 股票筛选器（价格/成交量/PE/PB/行业过滤）
- K线图（日/周/月线）
- 信号查看（信号生成器结果）
- 技术指标（MA/MACD/RSI）

使用方式:
    python baostock_review_gui.py
"""
import sys
import os
import threading
import subprocess
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PySide6 import QtWidgets, QtGui, QtCore

from vnpy.trader.database import get_database
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData
from vnpy.chart.base import NORMAL_FONT

# 导入 baostock 模块
from baostock_database import BaostockDatabase
from multi_factor_strategy import MultiFactorStrategy
from technical_analysis import TechnicalAnalysis

# 必须在使用前导入 pyqtgraph，避免 QSS 冲突
import pyqtgraph as pg

# ========== 颜色方案 (A股: 红涨绿跌) ==========
# 用于图表、UI 元素的颜色定义
# A股市场惯例: 红色表示上涨，绿色表示下跌
COLOR_UP = QtGui.QColor(220, 20, 60)       # 涨 - 红色 (A股惯例)
COLOR_DOWN = QtGui.QColor(0, 128, 0)       # 跌 - 绿色 (A股惯例)
COLOR_FLAT = QtGui.QColor(128, 128, 128)  # 平 - 灰色
COLOR_BG = "#FAFAFA"                       # 页面背景色 (浅灰)
COLOR_SIDEBAR = "#F5F5F5"                  # 侧边栏背景色
COLOR_BORDER = "#E0E0E0"                   # 边框颜色
COLOR_ACCENT = "#1890FF"                   # 主题强调色 (蓝色，用于选中、按钮等)
COLOR_TEXT = "#333333"                     # 主要文字颜色
COLOR_TEXT_LIGHT = "#999999"               # 次要文字颜色 (灰色)
COLOR_HEADER_BG = "#EBEBEB"                # 表头/标签页背景色
COLOR_ROW_ALT = "#F9F9F9"                  # 表格交替行背景色
COLOR_HOVER = "#E8F4FF"                    # 鼠标悬停背景色 (浅蓝)
COLOR_SELECTED = "#E6F7FF"                 # 选中项背景色 (更浅蓝)

# ========== 浅色主题 QSS ==========
# 定义全局样式表，统一 UI 组件的视觉风格
# 使用 f-string 嵌入颜色变量，保持主题一致性
# QSS (Qt Style Sheets) 语法类似 CSS，用于自定义 Qt 控件外观
LIGHT_THEME = f"""
QPushButton {{
    background-color: #FFFFFF;
    border: 1px solid {COLOR_BORDER};
    border-radius: 4px;
    padding: 6px 16px;
    min-width: 60px;
}}
QPushButton:hover {{
    background-color: {COLOR_HOVER};
    border-color: {COLOR_ACCENT};
}}
QPushButton:pressed {{
    background-color: #D0E8FF;
}}
QPushButton#Primary {{
    background-color: {COLOR_ACCENT};
    color: white;
    border-color: {COLOR_ACCENT};
}}
QTableWidget {{
    background-color: #FFFFFF;
    border: 1px solid {COLOR_BORDER};
    gridline-color: {COLOR_BORDER};
    selection-background-color: {COLOR_SELECTED};
}}
QTableWidget::item {{
    padding: 4px 8px;
}}
QHeaderView::section {{
    background-color: {COLOR_HEADER_BG};
    color: {COLOR_TEXT};
    padding: 6px;
    border: none;
    border-right: 1px solid {COLOR_BORDER};
    border-bottom: 1px solid {COLOR_BORDER};
    font-weight: bold;
    font-size: 12px;
}}
QTreeWidget {{
    background-color: #FFFFFF;
    border: 1px solid {COLOR_BORDER};
}}
QTreeWidget::item {{
    padding: 4px 8px;
    border-bottom: 1px solid {COLOR_BORDER};
}}
QTreeWidget::item:hover {{
    background-color: {COLOR_HOVER};
}}
QTreeWidget::item:selected {{
    background-color: {COLOR_SELECTED};
}}
QSplitter::handle {{
    background-color: {COLOR_BORDER};
}}
QTabWidget::pane {{
    border: 1px solid {COLOR_BORDER};
    background-color: #FFFFFF;
}}
QTabBar::tab {{
    background-color: {COLOR_HEADER_BG};
    padding: 8px 16px;
    margin-right: 2px;
    border: 1px solid {COLOR_BORDER};
    border-bottom: none;
    border-radius: 4px 4px 0 0;
}}
QTabBar::tab:selected {{
    background-color: #FFFFFF;
    border-bottom: 2px solid {COLOR_ACCENT};
}}
QLineEdit {{
    background-color: #FFFFFF;
    border: 1px solid {COLOR_BORDER};
    border-radius: 4px;
    padding: 6px 10px;
}}
QComboBox {{
    background-color: #FFFFFF;
    border: 1px solid {COLOR_BORDER};
    border-radius: 4px;
    padding: 6px 10px;
}}
QLabel {{ color: {COLOR_TEXT}; }}
QListWidget {{
    background-color: #FFFFFF;
    border: none;
    font-size: 12px;
}}
QListWidget::item {{
    padding: 6px 8px;
    border-bottom: 1px solid #F0F0F0;
}}
QListWidget::item:hover {{ background-color: {COLOR_HOVER}; }}
QListWidget::item:selected {{
    background-color: {COLOR_SELECTED};
    color: {COLOR_ACCENT};
}}
"""


# ==================== UI 组件定义 ====================

class StockListWidget(QtWidgets.QWidget):
    """左侧股票列表 + 搜索框

    功能:
    - 从数据库加载所有有日线数据的股票
    - 支持按代码或名称搜索过滤
    - 点击股票发射 stock_selected 信号，通知主窗口更新图表

    信号:
        stock_selected(str, Exchange): 用户点击股票时发射，参数为 (symbol, exchange)

    内部数据:
        self._data: list[(symbol, name, exchange), ...] — 全部股票数据，用于搜索过滤
    """

    # 自定义信号: 当用户选择股票时发射 (symbol, exchange)
    stock_selected = QtCore.Signal(str, Exchange)

    def __init__(self, db: BaostockDatabase, parent=None):
        """初始化股票列表组件

        Args:
            db: 数据库实例，用于查询股票数据
            parent: 父窗口组件
        """
        super().__init__(parent)
        self.db = db
        self._data = []  # 存储所有股票数据: [(symbol, name, exchange), ...]
        self.init_ui()
        self.load_stocks_from_db()

    def load_stocks_from_db(self):
        """从数据库加载所有有日线数据的股票

        流程:
        1. 调用 db.get_bar_overview() 查询所有 interval=DAILY 的股票概览
        2. 调用 db.get_stock_list() 查询股票基本信息获取名称映射
        3. 将代码和名称组合，按股票代码字母序排序
        4. 调用 _populate_list() 填充到 QListWidget

        注意:
            - baostock 代码格式为 "sh.600000" 或 "sz.000001"
            - 需要提取 "." 后面的纯数字部分作为 symbol
        """
        try:
            # 获取日线行情概览，筛选出有日线数据的股票
            # get_bar_overview() 返回数据库中所有已采集的K线概览记录
            overview = self.db.get_bar_overview()
            daily_stocks = {}
            for o in overview:
                if o.interval == Interval.DAILY:
                    daily_stocks[o.symbol] = o.exchange

            # 获取股票名称映射表 {symbol: name}
            # baostock 代码格式: "sh.600000" → 提取 "600000" 作为 key
            stock_list = self.db.get_stock_list()
            name_map = {}
            for s in stock_list:
                code = s["code"]
                # 处理 baostock 格式代码 (如 sh.600000 -> 600000)
                if "." in code:
                    symbol = code.split(".")[1]
                else:
                    symbol = code
                name_map[symbol] = s.get("code_name", "")

            # 构建股票列表数据并排序 (按代码字母序)
            self._data = sorted(
                [(symbol, name_map.get(symbol, ""), exchange)
                 for symbol, exchange in daily_stocks.items()],
                key=lambda x: x[0]  # 按股票代码排序
            )

            self._populate_list(self._data)
        except Exception as e:
            import traceback
            traceback.print_exc()

    def init_ui(self):
        """初始化 UI 布局

        布局结构 (垂直):
        ┌─────────────────────┐
        │  [标题栏 "股票列表"]  │
        ├─────────────────────┤
        │  [搜索输入框]        │  ← textChanged → _filter_stocks()
        ├─────────────────────┤
        │                     │
        │  [股票列表]          │  ← itemClicked → _on_select() → 发射信号
        │  (QListWidget)      │
        │                     │
        └─────────────────────┘
        """
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # 标题栏 — 固定在顶部，带底部分隔线
        title = QtWidgets.QLabel(" 股票列表")
        title.setStyleSheet(
            "QLabel { font-size: 15px; font-weight: bold; color: #333333; "
            "background-color: #FFFFFF; border-bottom: 1px solid #E0E0E0; padding: 12px 8px; }"
        )
        layout.addWidget(title)

        # 搜索输入框 — 支持实时模糊搜索
        self.search_edit = QtWidgets.QLineEdit()
        self.search_edit.setPlaceholderText("  输入代码或名称搜索...")
        self.search_edit.setTextMargins(4, 0, 0, 0)
        self.search_edit.setStyleSheet(
            "QLineEdit { background-color: #FFFFFF; border: 1px solid #E0E0E0; "
            "border-radius: 4px; padding: 8px 8px; margin: 8px; font-size: 13px; }"
            "QLineEdit:focus { border-color: #1890FF; }"
        )
        self.search_edit.textChanged.connect(self._filter_stocks)  # 输入时实时过滤
        layout.addWidget(self.search_edit)

        # 股票列表组件 — 支持交替行颜色和点击选择
        self.list_widget = QtWidgets.QListWidget()
        self.list_widget.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.list_widget.setAlternatingRowColors(True)  # 交替行颜色，便于区分
        self.list_widget.itemClicked.connect(self._on_select)  # 点击发射信号
        layout.addWidget(self.list_widget)

    def _populate_list(self, stocks):
        """填充股票列表到 QListWidget

        每项显示格式: "600000  浦发银行" 或仅代码（无名称时）
        将 (code, exchange) 元组存入 UserRole，供点击时获取

        Args:
            stocks: 股票数据列表 [(symbol, name, exchange), ...]
        """
        self.list_widget.clear()
        for code, name, ex in stocks:
            text = f"{code}  {name}" if name else code
            item = QtWidgets.QListWidgetItem(text)
            # 将股票代码和交易所存储在 UserRole 中，供点击时获取
            # Qt.ItemDataRole.UserRole 是自定义数据存储的标准角色
            item.setData(QtCore.Qt.ItemDataRole.UserRole, (code, ex))
            self.list_widget.addItem(item)

    def _filter_stocks(self, text):
        """根据输入文本过滤股票列表

        过滤规则:
        - 空搜索 → 显示全部股票
        - 模糊匹配 → 代码或名称包含关键词即匹配 (不区分大小写)

        Args:
            text: 搜索关键词 (代码或名称)
        """
        text = text.strip().lower()
        if not text:
            # 空搜索时显示全部股票
            self._populate_list(self._data)
            return
        # 模糊匹配代码或名称
        filtered = [
            (code, name, ex)
            for code, name, ex in self._data
            if text in code.lower() or text in name.lower()
        ]
        self._populate_list(filtered)

    def _on_select(self, item):
        """处理股票列表点击事件

        从 QListWidgetItem 的 UserRole 中提取 (symbol, exchange) 并发射信号

        Args:
            item: 被点击的 QListWidgetItem
        """
        data = item.data(QtCore.Qt.ItemDataRole.UserRole)
        if data:
            print(f"[DEBUG] 股票列表点击: symbol={data[0]}, exchange={data[1]}")
            # 发射信号，通知主窗口更新图表
            self.stock_selected.emit(data[0], data[1])


class QuoteBar(QtWidgets.QWidget):
    """顶部行情信息栏

    显示当前选中股票的实时行情数据:
    - 股票名称和代码
    - 最新价和涨跌幅/涨跌额
    - 开盘/最高/最低/昨收/成交量/成交额

    布局结构 (水平):
    [股票信息] | [价格+涨跌幅] | [开盘/最高/最低] | [昨收/成交量/成交额]

    颜色逻辑:
    - 价格上涨 → 红色 (COLOR_UP)
    - 价格下跌 → 绿色 (COLOR_DOWN)
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._name_map = {}  # 股票代码 -> 股票名称 映射表
        self.init_ui()

    def set_name_map(self, name_map: dict):
        """设置股票名称映射表

        Args:
            name_map: {symbol: name} 字典，用于将代码转换为可读名称
        """
        self._name_map = name_map

    def init_ui(self):
        """初始化行情栏布局

        布局结构 (水平):
        ┌─────────┬──┬───────────┬──┬──────────────────────┐
        │ 名称     │  │ 最新价     │  │ 开盘  最高  最低    │
        │ 代码     │  │ 涨跌幅     │  │ 昨收  成交量 成交额 │
        └─────────┴──┴───────────┴──┴──────────────────────┘
        """
        layout = QtWidgets.QHBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)
        layout.setSpacing(12)

        # === 左侧: 股票名称和代码 ===
        info_layout = QtWidgets.QVBoxLayout()
        info_layout.setSpacing(2)

        self.name_label = QtWidgets.QLabel("请选择股票")
        self.name_label.setObjectName("Title")
        self.name_label.setMinimumWidth(100)
        self.code_label = QtWidgets.QLabel("")
        self.code_label.setObjectName("SubTitle")

        info_layout.addWidget(self.name_label)
        info_layout.addWidget(self.code_label)
        layout.addLayout(info_layout)

        # 分隔线 (垂直)
        sep = QtWidgets.QFrame()
        sep.setFrameShape(QtWidgets.QFrame.Shape.VLine)
        sep.setStyleSheet("color: #E0E0E0;")
        layout.addWidget(sep)

        # === 中间: 价格和涨跌幅 ===
        price_layout = QtWidgets.QVBoxLayout()
        price_layout.setSpacing(2)
        self.price_label = QtWidgets.QLabel("--")
        self.price_label.setObjectName("Price")
        self.price_label.setMinimumWidth(90)
        self.change_label = QtWidgets.QLabel("--")
        self.change_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        self.change_label.setMinimumWidth(90)

        price_layout.addWidget(self.price_label)
        price_layout.addWidget(self.change_label)
        layout.addLayout(price_layout)

        # 分隔线 (垂直)
        sep2 = QtWidgets.QFrame()
        sep2.setFrameShape(QtWidgets.QFrame.Shape.VLine)
        sep2.setStyleSheet("color: #E0E0E0;")
        layout.addWidget(sep2)

        # === 右侧: 详细数据网格 (3列 x 2行) ===
        grid = QtWidgets.QGridLayout()
        grid.setSpacing(4)
        self.info_labels = {}
        # 6个指标: 开盘、最高、最低、昨收、成交量、成交额
        # 布局为 2行 x 3列 (每列: 标签 + 值)
        for i, label in enumerate(["开盘", "最高", "最低", "昨收", "成交量", "成交额"]):
            title = QtWidgets.QLabel(label)
            title.setStyleSheet("color: #999999; font-size: 11px;")
            value = QtWidgets.QLabel("--")
            value.setStyleSheet("color: #333333; font-size: 12px;")
            value.setMinimumWidth(80)
            self.info_labels[label] = value
            # 计算网格位置: row = i//3, col = (i%3)*2 和 (i%3)*2+1
            grid.addWidget(title, i // 3, (i % 3) * 2)
            grid.addWidget(value, i // 3, (i % 3) * 2 + 1)
        layout.addLayout(grid)
        layout.addStretch()  # 右侧弹性空间

    def update_quote(self, bar: BarData, pre_close: float = 0):
        """更新行情显示数据

        根据 BarData 和昨收价计算涨跌幅，并更新所有标签

        Args:
            bar: BarData 对象，包含 OHLCV 数据 (open_price, high_price, low_price, close_price, volume, turnover)
            pre_close: 昨收价，用于计算涨跌幅 (若为 0 则用 bar.open_price 替代)
        """
        # 显示股票名称
        name = self._name_map.get(bar.symbol, bar.symbol)
        self.name_label.setText(name)
        self.code_label.setText(bar.symbol)

        # 计算涨跌幅和涨跌额
        pc = pre_close if pre_close else bar.open_price
        change_pct = (bar.close_price - pc) / pc * 100 if pc else 0
        change_amt = bar.close_price - pc if pc else 0

        # 更新价格显示
        self.price_label.setText(f"{bar.close_price:.2f}")
        if change_pct >= 0:
            # 上涨: 红色
            self.price_label.setStyleSheet(
                f"font-size: 24px; font-weight: bold; color: {COLOR_UP.name()};"
            )
            self.change_label.setStyleSheet(
                f"font-size: 16px; font-weight: bold; color: {COLOR_UP.name()};"
            )
            self.change_label.setText(f"+{change_amt:.2f}  +{change_pct:.2f}%")
        else:
            # 下跌: 绿色
            self.price_label.setStyleSheet(
                f"font-size: 24px; font-weight: bold; color: {COLOR_DOWN.name()};"
            )
            self.change_label.setStyleSheet(
                f"font-size: 16px; font-weight: bold; color: {COLOR_DOWN.name()};"
            )
            self.change_label.setText(f"{change_amt:.2f}  {change_pct:.2f}%")

        # 更新详细数据
        self.info_labels["开盘"].setText(f"{bar.open_price:.2f}")
        self.info_labels["最高"].setText(f"{bar.high_price:.2f}")
        self.info_labels["最低"].setText(f"{bar.low_price:.2f}")
        self.info_labels["昨收"].setText(f"{pc:.2f}")
        self.info_labels["成交量"].setText(f"{bar.volume:,.0f}")
        self.info_labels["成交额"].setText(f"{bar.turnover:,.0f}")


class KLineWidget(QtWidgets.QWidget):
    """K线图区域

    使用 pyqtgraph 绘制蜡烛图，支持:
    - 日/周/月线切换 (顶部工具栏按钮)
    - 鼠标悬浮显示 OHLC 详情 (十字线 + 浮动提示框)
    - MA5/MA10/MA20 均线 (金色/蓝色/棕色)
    - A股配色 (红涨绿跌)

    绘图原理:
    - 阳线 (收盘 >= 开盘): 白色填充 + 红色边框
    - 阴线 (收盘 < 开盘): 绿色填充 + 绿色边框
    - 影线: 从最高价到最低价的竖直线

    信号:
        period_changed(str): 周期切换时发射，参数为 "DAILY"/"WEEKLY"/"MONTHLY"
    """

    # 图表颜色定义
    COLOR_UP = "#DC143C"    # 阳线颜色 (深红)
    COLOR_DOWN = "#008000"  # 阴线颜色 (深绿)
    COLOR_AXIS = "#999999"  # 坐标轴颜色

    # 自定义信号
    period_changed = QtCore.Signal(str)      # 周期切换时发射
    candle_clicked = QtCore.Signal(object)   # 点击蜡烛时发射，参数为 BarData 对象

    def __init__(self, parent=None):
        """初始化 K 线图组件"""
        super().__init__(parent)
        self._bars: list[BarData] = []       # 当前显示的K线数据
        self._current_interval = "DAILY"     # 当前周期类型
        self._plot_item = None               # pyqtgraph PlotItem 实例
        self._vline = None                   # 十字线: 垂直线
        self._hline = None                   # 十字线: 水平线
        self._tooltip = None                 # 鼠标悬浮提示框 (QLabel)
        self._items = []                     # 图表上的所有图形元素 (用于清空时移除)
        self.setMinimumSize(400, 300)
        self.init_ui()

    def init_ui(self):
        """初始化 K 线图 UI

        布局结构 (垂直):
        ┌─────────────────────────────┐
        │ [日线] [周线] [月线]  (工具栏)│
        ├─────────────────────────────┤
        │                             │
        │  pyqtgraph 图表区域          │
        │  (蜡烛图 + MA均线 + 十字线)  │
        │                             │
        └─────────────────────────────┘

        组件说明:
        - 工具栏: 3个互斥按钮，通过 QButtonGroup 管理
        - pyqtgraph: GraphicsLayoutWidget + PlotItem
        - 十字线: 2条 InfiniteLine (水平+垂直)
        - 提示框: 浮动 QLabel，跟随鼠标位置显示OHLC数据
        """
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # === 顶部工具栏 ===
        toolbar = QtWidgets.QWidget()
        toolbar.setFixedHeight(32)
        toolbar.setStyleSheet("background-color: #FFFFFF; border-bottom: 1px solid #E0E0E0;")
        tb_layout = QtWidgets.QHBoxLayout(toolbar)
        tb_layout.setContentsMargins(8, 2, 8, 2)
        tb_layout.setSpacing(4)

        # 周期按钮组 (互斥)
        self.period_group = QtWidgets.QButtonGroup(self)
        self.period_group.setExclusive(True)

        self.btn_daily = QtWidgets.QPushButton("日线")
        self.btn_weekly = QtWidgets.QPushButton("周线")
        self.btn_monthly = QtWidgets.QPushButton("月线")
        period_map = {
            self.btn_daily: "DAILY",
            self.btn_weekly: "WEEKLY",
            self.btn_monthly: "MONTHLY",
        }
        for btn, period_str in period_map.items():
            btn.setFixedHeight(26)
            # 按钮样式: 默认灰色，悬停浅蓝，选中蓝色
            btn.setStyleSheet(
                "QPushButton { background-color: #F5F5F5; border: 1px solid #E0E0E0; "
                "border-radius: 3px; font-size: 12px; padding: 0 12px; }"
                "QPushButton:hover { background-color: #E8F4FF; border-color: #1890FF; }"
                "QPushButton:checked { background-color: #1890FF; color: white; border-color: #1890FF; }"
            )
            btn.setCheckable(True)
            btn.setChecked(period_str == "DAILY")  # 默认选中日线
            btn.setProperty("interval", period_str)  # 存储周期类型用于识别
            self.period_group.addButton(btn)
            tb_layout.addWidget(btn)

        self.period_group.buttonClicked.connect(self._on_period_changed)
        tb_layout.addStretch()
        layout.addWidget(toolbar)

        # === pyqtgraph 图表区域 ===
        self.gfx = pg.GraphicsLayoutWidget()
        self.gfx.setBackground("#FFFFFF")
        layout.addWidget(self.gfx, 1)

        self._plot_item = self.gfx.addPlot(row=0, col=0)
        self._plot_item.setMouseEnabled(x=True, y=False)  # 允许水平拖拽，禁止垂直拖拽
        self._plot_item.showGrid(x=True, y=True, alpha=0.15)  # 显示网格线

        # 坐标轴样式
        axis_pen = pg.mkPen(self.COLOR_AXIS, width=1)
        self._plot_item.getAxis("left").setPen(axis_pen)
        self._plot_item.getAxis("bottom").setPen(axis_pen)
        self._plot_item.getAxis("left").setTextPen(pg.mkPen(self.COLOR_AXIS))
        self._plot_item.getAxis("bottom").setTextPen(pg.mkPen(self.COLOR_AXIS))

        # 十字线: 垂直线 (黄色虚线)
        self._vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen("#FFB800", width=1, style=QtCore.Qt.PenStyle.DashLine))
        self._vline.hide()
        self._plot_item.addItem(self._vline)

        # 十字线: 水平线 (黄色虚线)
        self._hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen("#FFB800", width=1, style=QtCore.Qt.PenStyle.DashLine))
        self._hline.hide()
        self._plot_item.addItem(self._hline)

        # 鼠标悬浮提示框
        self._tooltip = QtWidgets.QLabel(self)
        self._tooltip.setStyleSheet(
            "QLabel { background: rgba(255,255,255,0.95); border: 1px solid #E0E0E0; "
            "border-radius: 3px; padding: 2px 6px; font-size: 11px; color: #333; }"
        )
        self._tooltip.hide()

    def _on_period_changed(self, btn):
        """处理周期按钮点击事件

        Args:
            btn: 被点击的 QPushButton，其 property("interval") 存储周期类型
        """
        period_str = btn.property("interval")
        if period_str and period_str != self._current_interval:
            self._current_interval = period_str
            self.period_changed.emit(period_str)

    def _clear_plot(self):
        """清空图表上的所有图形元素

        遍历 self._items 列表，逐个从 PlotItem 中移除。
        使用 try/except 防止已移除的元素报错。
        """
        for item in self._items:
            try:
                self._plot_item.removeItem(item)
            except Exception:
                pass
        self._items = []

    def _add_item(self, item):
        """添加图形元素到图表，并记录到 _items 列表

        Args:
            item: pyqtgraph 图形元素 (PlotCurveItem, BarGraphItem 等)
        """
        self._plot_item.addItem(item)
        self._items.append(item)

    def update_data(self, bars: list[BarData], interval: str = "DAILY"):
        """更新 K 线图数据

        绘图步骤:
        1. 清空图表
        2. 绘制每根蜡烛的影线 (最高价到最低价的竖直线)
        3. 绘制阳线实体 (白色填充+红色边框)
        4. 绘制阴线实体 (绿色填充+绿色边框)
        5. 绘制 MA5/MA10/MA20 均线
        6. 设置坐标轴范围和刻度
        7. 注册鼠标悬浮事件 (十字线 + 提示框)

        Args:
            bars: K线数据列表，按时间顺序排列
            interval: 周期类型 "DAILY"/"WEEKLY"/"MONTHLY"
        """
        if not bars:
            return

        self._bars = bars
        self._clear_plot()

        n = len(bars)
        x = list(range(n))  # X轴坐标: 0, 1, 2, ..., n-1
        opens = [b.open_price for b in bars]
        highs = [b.high_price for b in bars]
        lows = [b.low_price for b in bars]
        closes = [b.close_price for b in bars]
        dates = [b.datetime for b in bars]

        # Y轴范围: 最低价以下2% ~ 最高价以上2%
        min_p = min(lows) * 0.98
        max_p = max(highs) * 1.02

        w = 0.5  # 蜡烛宽度

        # 分别收集阳线和阴线的数据
        up_x, up_y0, up_h = [], [], []     # 阳线: X坐标, 实体底部, 实体高度
        down_x, down_y0, down_h = [], [], []  # 阴线: X坐标, 实体底部, 实体高度

        # === 绘制影线和收集实体数据 ===
        for i in range(n):
            is_up = closes[i] >= opens[i]
            body_top = max(opens[i], closes[i])    # 实体顶部
            body_bottom = min(opens[i], closes[i])  # 实体底部
            # 实体高度至少为价格的 0.1%，避免高度为 0 时不显示
            body_h_val = max(body_top - body_bottom, (body_top + body_bottom) * 0.001)

            color = self.COLOR_UP if is_up else self.COLOR_DOWN
            # 绘制影线 (从最低价到最高价的竖直线)
            wick = pg.PlotCurveItem(
                x=[x[i], x[i]], y=[lows[i], highs[i]],
                pen=pg.mkPen(color, width=1),
            )
            self._add_item(wick)

            # 分类收集实体数据
            if is_up:
                up_x.append(x[i])
                up_y0.append(body_bottom)
                up_h.append(body_h_val)
            else:
                down_x.append(x[i])
                down_y0.append(body_bottom)
                down_h.append(body_h_val)

        # === 绘制阳线实体 (白色填充 + 红色边框) ===
        up_pen = QtGui.QPen(QtGui.QColor(self.COLOR_UP), 0.8)
        up_pen.setCosmetic(True)  # 边框宽度不随缩放变化
        down_pen = QtGui.QPen(QtGui.QColor(self.COLOR_DOWN), 0.8)
        down_pen.setCosmetic(True)
        if up_x:
            bar_up = pg.BarGraphItem(x=up_x, y0=up_y0, height=up_h, width=w,
                                     pen=up_pen, brush=pg.mkBrush("#FFFFFF"))
            self._add_item(bar_up)

        # === 绘制阴线实体 (绿色填充 + 绿色边框) ===
        if down_x:
            bar_down = pg.BarGraphItem(x=down_x, y0=down_y0, height=down_h, width=w,
                                       pen=down_pen, brush=pg.mkBrush(self.COLOR_DOWN))
            self._add_item(bar_down)

        # === 绘制 MA 均线 ===
        # MA5: 金色, MA10: 宝蓝色, MA20: 赭石色
        for period, color, name in [(5, "#FFB800", "MA5"), (10, "#4169E1", "MA10"), (20, "#A0522D", "MA20")]:
            if n >= period:
                # 计算移动平均: 第 i 个 MA 值为 closes[i-period+1 : i+1] 的平均值
                ma = [sum(closes[i - period + 1:i + 1]) / period if i >= period - 1 else float('nan') for i in range(n)]
                curve = pg.PlotCurveItem(x=x, y=ma, pen=pg.mkPen(color, width=1.2), name=name)
                self._add_item(curve)

        # === 设置坐标轴范围 ===
        self._plot_item.setYRange(min_p, max_p, padding=0)
        self._plot_item.setXRange(-0.5, n - 0.5, padding=0)

        # === 设置 X 轴时间刻度 ===
        if interval == "MONTHLY":
            # 月线: 显示月份
            x_ticks = [(i, f"{dates[i].month}月") for i in range(n)]
        elif interval == "WEEKLY":
            # 周线: 每隔约 n/8 根K线显示一个日期
            step = max(1, n // 8)
            x_ticks = [(i, dates[i].strftime("%m-%d")) for i in range(0, n, step)]
        else:
            # 日线: 每个月的第一天显示月份
            seen_months = set()
            x_ticks = []
            for i in range(n):
                m = (dates[i].year, dates[i].month)
                if m not in seen_months:
                    seen_months.add(m)
                    x_ticks.append((i, f"{dates[i].month}月"))
        self._plot_item.getAxis("bottom").setTicks([x_ticks])

        # 隐藏十字线和提示框 (等待鼠标事件触发)
        self._vline.hide()
        self._hline.hide()
        self._tooltip.hide()

        # === 鼠标悬浮事件: 显示十字线和OHLC提示 ===
        def mouse_moved(evt):
            """鼠标移动回调函数

            根据鼠标位置在图表上显示十字线，并在浮动框中显示对应K线的详细信息

            Args:
                evt: 鼠标在场景中的坐标位置
            """
            pos = self._plot_item.vb.mapSceneToView(evt)
            ix = int(round(pos.x()))
            if 0 <= ix < n:
                # 显示十字线
                self._plot_item.vb.setCursor(QtCore.Qt.CursorShape.CrossCursor)
                self._vline.setPos(ix)
                self._vline.show()
                self._hline.setPos(pos.y())
                self._hline.show()

                # 构建提示文字
                b = bars[ix]
                info = (
                    f"  {b.datetime.strftime('%Y-%m-%d')}  "
                    f"开:{b.open_price:.2f}  高:{b.high_price:.2f}  "
                    f"低:{b.low_price:.2f}  收:{b.close_price:.2f}  "
                    f"量:{b.volume:,.0f}"
                )
                self._tooltip.setText(info)
                self._tooltip.adjustSize()
                tw, th = self._tooltip.width(), self._tooltip.height()

                # 计算提示框位置: 跟随鼠标，但避免超出边界
                vb = self._plot_item.vb
                p = vb.mapViewToScene(QtCore.QPointF(float(ix), b.high_price))
                px, py = p.x(), p.y()
                plot_pos = self._plot_item.pos()
                px += plot_pos.x()
                py += plot_pos.y()
                w, h = self.width(), self.height()

                # 水平位置: 优先居中，靠近右边界时左移
                if px + tw > w - 5:
                    tx = px - tw - 5
                elif px - tw < 5:
                    tx = 5
                else:
                    tx = px - tw // 2

                # 垂直位置: 在上方显示，如果在上半区域则在下方显示
                if py > h / 2:
                    ty = py - th - 8
                else:
                    ty = py + 8

                self._tooltip.move(tx, ty)
                self._tooltip.show()
            else:
                # 鼠标超出范围，隐藏十字线
                self._plot_item.vb.unsetCursor()
                self._vline.hide()
                self._hline.hide()
                self._tooltip.hide()

        self.gfx.scene().sigMouseMoved.connect(mouse_moved)

        # === 鼠标点击事件: 点击蜡烛时发射信号，通知分时图切换 ===
        def on_mouse_clicked(event):
            """鼠标点击回调，获取被点击的蜡烛并发射信号"""
            if event.button() != QtCore.Qt.MouseButton.LeftButton:
                return
            pos = self._plot_item.vb.mapSceneToView(event.scenePos())
            ix = int(round(pos.x()))
            if 0 <= ix < n:
                clicked_bar = bars[ix]
                print(f"[DEBUG] K线图点击蜡烛: ix={ix}, date={clicked_bar.datetime.strftime('%Y-%m-%d')}")
                self.candle_clicked.emit(clicked_bar)

        self.gfx.scene().sigMouseClicked.connect(on_mouse_clicked)


class TimeShareWidget(QtWidgets.QWidget):
    """分时图

    显示单个交易日内的价格走势，基于5分钟数据绘制。

    功能:
    - 价格线 (涨红跌绿)
    - 均价线 (黄色虚线，成交量加权平均)
    - 昨收参考线 (灰色虚线)
    - 鼠标悬浮显示时间和价格

    布局结构:
    ┌─────────────────────┐
    │  [标题 "分时图"]     │
    ├─────────────────────┤
    │                     │
    │  pyqtgraph 图表      │
    │  (价格线 + 均价线)   │
    │                     │
    └─────────────────────┘

    颜色:
    COLOR_UP = "#DC143C"  (阳线深红)
    COLOR_DOWN = "#008000" (阴线深绿)
    COLOR_AXIS = "#999999" (坐标轴灰色)
    """

    COLOR_UP = "#DC143C"
    COLOR_DOWN = "#008000"
    COLOR_AXIS = "#999999"

    def __init__(self, parent=None):
        super().__init__(parent)
        self._items = []            # 图表上的所有图形元素
        self._bars = []             # 当前分时数据
        self._pre_close = 0.0       # 昨收价
        self._price_changes = []    # 每根K线相对于昨收的涨跌额
        self._n = 0                 # K线数量
        self.init_ui()

    def init_ui(self):
        """初始化分时图 UI

        与 KLineWidget 类似，但功能更简化:
        - 无工具栏
        - 禁止鼠标拖拽 (setMouseEnabled(False, False))
        - 隐藏X轴 (hideAxis("bottom"))，使用 TextItem 替代
        """
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # 标题栏
        title = QtWidgets.QLabel(" 分时图")
        title.setStyleSheet(
            "QLabel { font-size: 13px; font-weight: bold; color: #333333; "
            "background-color: #FFFFFF; border-bottom: 1px solid #E0E0E0; padding: 6px 8px; }"
        )
        layout.addWidget(title)

        # pyqtgraph 图表区域
        self.gfx = pg.GraphicsLayoutWidget()
        self.gfx.setBackground("#FFFFFF")
        layout.addWidget(self.gfx, 1)

        self._plot_item = self.gfx.addPlot(row=0, col=0)
        self._plot_item.setMouseEnabled(x=False, y=False)  # 禁止拖拽
        self._plot_item.showGrid(x=False, y=True, alpha=0.15)  # 仅显示水平网格

        # 坐标轴样式
        axis_pen = pg.mkPen(self.COLOR_AXIS, width=1)
        self._plot_item.getAxis("left").setPen(axis_pen)
        self._plot_item.getAxis("left").setTextPen(pg.mkPen(self.COLOR_AXIS))
        self._plot_item.hideAxis("bottom")  # 隐藏X轴，使用自定义时间标签

        # 十字线
        self._vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen("#AAAAAA", width=0.8, style=QtCore.Qt.PenStyle.DashLine))
        self._hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen("#AAAAAA", width=0.8, style=QtCore.Qt.PenStyle.DashLine))
        self._plot_item.addItem(self._vline)
        self._plot_item.addItem(self._hline)
        self._vline.hide()
        self._hline.hide()

        # 鼠标悬浮提示框
        self._tooltip = QtWidgets.QLabel(self)
        self._tooltip.setStyleSheet(
            "QLabel { background: rgba(255,255,255,0.95); border: 1px solid #E0E0E0; "
            "border-radius: 3px; padding: 2px 6px; font-size: 11px; color: #333; }"
        )
        self._tooltip.hide()

        self._setup_mouse_handler()

    def _setup_mouse_handler(self):
        """注册鼠标悬浮事件

        监听 scene().sigMouseMoved 信号，显示十字线和价格信息。
        与 KLineWidget 的逻辑类似，但显示格式为时间和涨跌额。
        """
        def on_moved(evt):
            """鼠标移动回调"""
            if not self._bars:
                return
            pos = self._plot_item.vb.mapSceneToView(evt)
            ix = int(round(pos.x()))
            if 0 <= ix < self._n:
                self.gfx.setCursor(QtCore.Qt.CursorShape.CrossCursor)
                self._vline.setPos(ix)
                self._vline.show()
                self._hline.setPos(self._bars[ix].close_price)
                self._hline.show()
                b = self._bars[ix]
                info = (
                    f"  {b.datetime.strftime('%H:%M')}  "
                    f"价格:{b.close_price:.2f}  "
                    f"涨跌:{self._price_changes[ix]:+.2f}"
                )
                self._tooltip.setText(info)
                self._tooltip.adjustSize()
                tw, th = self._tooltip.width(), self._tooltip.height()
                vb = self._plot_item.vb
                p = vb.mapViewToScene(QtCore.QPointF(float(ix), b.close_price))
                px, py = p.x(), p.y()
                plot_pos = self._plot_item.pos()
                px += plot_pos.x()
                py += plot_pos.y()
                w, h = self.width(), self.height()
                if px + tw > w - 5:
                    tx = px - tw - 5
                elif px - tw < 5:
                    tx = 5
                else:
                    tx = px - tw // 2
                if py > h / 2:
                    ty = py - th - 8
                else:
                    ty = py + 8
                self._tooltip.move(tx, ty)
                self._tooltip.show()
            else:
                self.gfx.unsetCursor()
                self._vline.hide()
                self._hline.hide()
                self._tooltip.hide()
        self.gfx.scene().sigMouseMoved.connect(on_moved)

    def _clear_plot(self):
        """清空图表上的所有图形元素"""
        for item in self._items:
            try:
                self._plot_item.removeItem(item)
            except Exception:
                pass
        self._items = []

    def _add_item(self, item):
        """添加图形元素到图表"""
        self._plot_item.addItem(item)
        self._items.append(item)

    def update_data(self, bars: list[BarData], pre_close: float):
        """更新分时图数据

        绘图步骤:
        1. 清空图表
        2. 绘制价格线 (涨红跌绿)
        3. 绘制均价线 (黄色虚线，成交量加权平均价 VWAP)
        4. 绘制昨收参考线 (灰色虚线)
        5. 设置 Y 轴范围 (以昨收为中心，上下对称)
        6. 添加 X 轴时间刻度 (9:30, 10:30, 11:00, 13:00, 14:00, 15:00)
        7. 添加 Y 轴刻度 (最高价、最低价、昨收、开盘价)

        Args:
            bars: 分时数据列表 (通常是单日的5分钟K线)
            pre_close: 前一日收盘价，用于计算涨跌和确定Y轴中心
        """
        if not bars:
            self._clear_plot()
            return

        self._clear_plot()
        self._bars = bars
        self._pre_close = pre_close
        self._n = len(bars)
        closes = [b.close_price for b in bars]
        x = list(range(self._n))
        self._price_changes = [c - pre_close for c in closes]

        # === 计算 Y 轴范围 ===
        data_high = max(b.high_price for b in bars)
        data_low = min(b.low_price for b in bars)
        bound_top = max(pre_close, data_high)
        bound_bot = min(pre_close, data_low)
        margin = (bound_top - bound_bot) * 0.05 if bound_top > bound_bot else 1.0
        y_min = bound_bot - margin
        y_max = bound_top + margin

        # === 绘制价格线 ===
        line_color = self.COLOR_UP if closes[-1] >= pre_close else self.COLOR_DOWN
        line = pg.PlotCurveItem(x=x, y=closes, pen=pg.mkPen(line_color, width=1.5))
        self._add_item(line)

        # === 绘制均价线 (VWAP) ===
        if any(b.volume > 0 for b in bars):
            cum_vol, cum_vp = 0, 0
            avg_prices = []
            for b in bars:
                cum_vol += b.volume
                cum_vp += b.close_price * b.volume
                avg_prices.append(cum_vp / cum_vol if cum_vol > 0 else pre_close)
            avg_line = pg.PlotCurveItem(x=x, y=avg_prices, pen=pg.mkPen("#FFB800", width=1, style=QtCore.Qt.PenStyle.DashLine))
            self._add_item(avg_line)

        # === 绘制昨收参考线 ===
        zero_line = pg.InfiniteLine(angle=0, pos=pre_close, pen=pg.mkPen("#CCCCCC", width=0.5, style=QtCore.Qt.PenStyle.DashLine))
        self._add_item(zero_line)

        # === 设置坐标轴范围 ===
        # X轴: -1 到 n，给价格线左右各留 1 单位的边距
        self._plot_item.setYRange(y_min, y_max, padding=0)
        self._plot_item.setXRange(-1, self._n)

        # === X轴时间标签 ===
        ticks = self._make_time_ticks(bars)
        y_tick = y_min + (y_max - y_min) * 0.01  # Y轴底部 1% 处
        for ix, label in ticks:
            ti = pg.TextItem(label, color=self.COLOR_AXIS, anchor=(0.5, 1))
            ti.setFont(pg.Qt.QtGui.QFont("Arial", 8))
            ti.setPos(float(ix), y_tick)
            self._plot_item.addItem(ti)
            self._items.append(ti)
            # 时间刻度对应的垂直网格线
            grid_line = pg.InfiniteLine(angle=90, pos=ix, pen=pg.mkPen("#E0E0E0", width=0.5, style=QtCore.Qt.PenStyle.DashLine))
            self._add_item(grid_line)

        # === Y轴刻度 ===
        self._plot_item.getAxis("left").setTicks([self._make_y_ticks(bars, pre_close)])

        # 重置状态
        self._vline.hide()
        self._hline.hide()
        self._tooltip.hide()
        self.gfx.unsetCursor()

    def leaveEvent(self, evt):
        """鼠标离开组件时隐藏十字线和提示框"""
        self.gfx.unsetCursor()
        self._vline.hide()
        self._hline.hide()
        self._tooltip.hide()
        super().leaveEvent(evt)

    def _make_time_ticks(self, bars: list[BarData]):
        """生成X轴时间刻度

        在标准交易时间点 (9:30, 10:30, 11:00, 13:00, 14:00, 15:00) 附近
        寻找最接近的K线位置，生成时间标签。

        Args:
            bars: 分时数据列表

        Returns:
            list[(index, label)]: 时间刻度列表，每个元素为 (X轴索引, 时间文字)
        """
        import datetime as dt
        # A股标准交易时间点
        time_points = [(9, 30), (10, 30), (11, 0), (13, 0), (14, 0), (15, 0)]
        if not bars:
            return []
        base_date = bars[0].datetime.date()
        tz = bars[0].datetime.tzinfo
        tick_labels = []
        for h, m in time_points:
            target = dt.datetime.combine(base_date, dt.time(hour=h, minute=m), tzinfo=tz)
            # 寻找最接近目标时间点的K线索引
            best_ix = None
            best_dist = float('inf')
            for i, b in enumerate(bars):
                diff = abs((b.datetime - target).total_seconds())
                if diff < best_dist:
                    best_dist = diff
                    best_ix = i
            # 时间差距在 10 分钟以内才显示标签
            if best_ix is not None and best_dist < 600:
                tick_labels.append((best_ix, f"{h:02d}:{m:02d}"))
        return tick_labels

    def _make_y_ticks(self, bars: list[BarData], pre_close: float):
        """生成Y轴刻度

        候选刻度: 最高价、最低价、昨收价、开盘价
        按优先级排序，并过滤掉过于密集的刻度

        Args:
            bars: 分时数据列表
            pre_close: 昨收价

        Returns:
            list[(value, label)]: Y轴刻度列表
        """
        high = max(b.high_price for b in bars)
        low = min(b.low_price for b in bars)
        # 候选刻度及其优先级 (数字越小优先级越高)
        candidates = [
            (round(high, 2), 1),       # 最高价
            (round(low, 2), 1),        # 最低价
            (round(pre_close, 2), 2),  # 昨收价
            (round(bars[0].open_price, 2), 3),  # 开盘价
        ]
        candidates.sort(key=lambda x: (x[1], x[0]))

        # 最小间距: Y轴范围的 8%，避免刻度过于密集
        y_range = (max(high, pre_close) - min(low, pre_close)) * 1.05
        min_spacing = y_range * 0.08

        # 选择刻度: 按优先级，间距小于最小间距的跳过
        selected = []
        for v, prio in candidates:
            if all(abs(v - s) >= min_spacing for s in selected):
                selected.append(v)
        return [(v, f"{v:.2f}") for v in sorted(set(selected))]


class FactorTableWidget(QtWidgets.QWidget):
    """多因子选股结果表格

    功能:
    - 运行多因子选股策略 (PE/PB/成交额/质量排名)
    - 显示选股结果 (代码/名称/收盘价/PE/PB/成交额/评分/技术信号)
    - 技术面过滤: 对初选结果进行 MA/MACD/RSI/成交量 检查
    - 双击股票发射 stock_selected 信号

    选股流程:
    1. 从数据库获取最新每日指标数据
    2. 初筛: 排除科创板/北交所、零成交量、价格异常
    3. 多因子排名: 调用 MultiFactorStrategy.select_stocks()
    4. 技术面过滤: 检查最近120天的技术指标，只保留有看多信号的
    5. 填充表格: 有看多信号的行标绿色背景

    信号:
        stock_selected(str): 双击股票时发射，参数为 symbol
    """

    stock_selected = QtCore.Signal(str)  # symbol

    def __init__(self, db: BaostockDatabase, parent=None):
        super().__init__(parent)
        self.db = db
        self._results = []  # 选股结果列表
        self.init_ui()

    def init_ui(self):
        """初始化多因子选股页面

        布局结构:
        ┌─────────────────────────────────────────────────┐
        │ [▶ 运行选股]  Top N: [30]   (控制面板, 固定高度) │
        ├─────────────────────────────────────────────────┤
        │                                                 │
        │  结果表格 (QTableWidget, 8列)                   │
        │  代码 | 名称 | 收盘价 | PE | PB | 成交额 | 评分 │
        │                                                 │
        └─────────────────────────────────────────────────┘
        """
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)

        # 控制面板
        ctrl = QtWidgets.QWidget()
        ctrl.setFixedHeight(40)
        ctrl_layout = QtWidgets.QHBoxLayout(ctrl)
        ctrl_layout.setContentsMargins(4, 2, 4, 2)

        self.btn_run = QtWidgets.QPushButton("▶ 运行选股")
        self.btn_run.setObjectName("Primary")
        self.btn_run.clicked.connect(self._run_selection)
        ctrl_layout.addWidget(self.btn_run)

        # Top N 选择器
        self.top_n_spin = QtWidgets.QSpinBox()
        self.top_n_spin.setRange(10, 100)
        self.top_n_spin.setValue(30)
        self.top_n_spin.setFixedWidth(60)
        ctrl_layout.addWidget(QtWidgets.QLabel(" Top N:"))
        ctrl_layout.addWidget(self.top_n_spin)

        ctrl_layout.addStretch()
        layout.addWidget(ctrl)

        # 结果表格 (8列)
        self.table = QtWidgets.QTableWidget()
        self.table.setColumnCount(8)
        self.table.setHorizontalHeaderLabels([
            "代码", "名称", "收盘价", "PE(TTM)", "PB", "成交额", "综合评分", "技术信号"
        ])
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)  # 只读
        self.table.itemDoubleClicked.connect(self._on_select)  # 双击发射信号
        layout.addWidget(self.table)

    def _run_selection(self):
        """运行多因子选股

        完整选股流程:
        1. 获取每日指标数据 (db.get_latest_daily_basic())
        2. 初筛过滤:
           - 排除科创板 (.68 开头)
           - 排除北交所 (sh.8x / sh.4x 开头)
           - 排除零成交量
           - 价格过滤 (3 ~ 200 元)
        3. 多因子排名 (MultiFactorStrategy.select_stocks())
        4. 技术面过滤:
           - 加载最近 365 天的日线数据
           - 需要至少 60 天数据
           - 生成技术信号 (MA/MACD/RSI/成交量)
           - 只保留有看多信号且无看空信号的
        5. 填充表格
        """
        try:
            top_n = self.top_n_spin.value()
            self.btn_run.setEnabled(False)
            self.btn_run.setText("计算中...")
            QtWidgets.QApplication.processEvents()  # 刷新UI，显示"计算中..."

            # 获取每日指标数据
            records = self.db.get_latest_daily_basic()
            if not records:
                QtWidgets.QMessageBox.warning(self, "提示", "数据库无每日指标数据，请先运行数据采集")
                return

            df = pd.DataFrame(records)

            # 初筛: 排除科创板/北交所
            df = df[~df["code"].str.contains(r"\.68")]
            df = df[~df["code"].str.contains(r"^sh\.[84]")]
            # 排除零成交量
            if "volume" in df.columns:
                df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
                df = df[df["volume"] > 0]
            # 价格过滤 (3 ~ 200 元)
            if "close" in df.columns:
                df["close"] = pd.to_numeric(df["close"], errors="coerce")
                df = df[(df["close"] >= 3) & (df["close"] <= 200)]

            # 多因子选股: PE/PB/成交额/质量排名
            mf = MultiFactorStrategy()
            selected = mf.select_stocks(df, top_n=top_n)

            # 技术面过滤（简化版：只检查是否有60天以上日线数据）
            results = []
            for _, row in selected.iterrows():
                code = row.get("code", "")
                if "." in code:
                    symbol = code.split(".")[1]
                    exchange = "SSE" if code.startswith("sh.") else "SZSE"
                else:
                    symbol = code
                    exchange = "SSE" if code.startswith("6") else "SZSE"

                # 加载最近一年的日线数据
                bars = self.db.load_bar_data(
                    symbol,
                    Exchange[exchange] if isinstance(exchange, str) else exchange,
                    Interval.DAILY,
                    datetime.now() - timedelta(days=365),
                    datetime.now() + timedelta(days=1),
                )
                if not bars or len(bars) < 60:
                    continue

                # 技术分析: 生成技术信号
                ta = TechnicalAnalysis()
                df_hist = pd.DataFrame([
                    {"日期": b.datetime.strftime("%Y-%m-%d"), "收盘": b.close_price, "成交量": b.volume}
                    for b in bars[-120:]  # 取最近120天数据
                ])
                signals = ta.generate_signals(df_hist)

                # 分类信号: 看多 vs 看空
                bullish = [s for s in signals if s in (
                    "MA_GOLDEN_CROSS", "MACD_BULLISH", "RSI_OVERSOLD", "VOLUME_BREAKOUT"
                )]
                bearish = [s for s in signals if s in (
                    "MA_DEATH_CROSS", "MACD_BEARISH", "RSI_OVERBOUGHT"
                )]

                signal_text = "|".join(bullish) if bullish else "无信号"

                results.append({
                    "symbol": symbol,
                    "code": code,
                    "name": "",
                    "close": row.get("close", 0),
                    "peTTM": row.get("peTTM", 0),
                    "pbMRQ": row.get("pbMRQ", 0),
                    "amount": row.get("amount", 0),
                    "total_score": row.get("total_score", 0),
                    "signals": signal_text,
                    "bullish": bool(bullish),
                    "bearish": bool(bearish),
                })

            self._results = results
            self._populate_table(results)
        except Exception as e:
            import traceback
            traceback.print_exc()
            QtWidgets.QMessageBox.critical(self, "错误", f"选股失败: {e}")
        finally:
            self.btn_run.setEnabled(True)
            self.btn_run.setText("▶ 运行选股")

    def _populate_table(self, results):
        """填充选股结果表格

        对于有看多信号且无看空信号的行，整行标绿背景

        Args:
            results: 选股结果列表，每项包含 symbol/name/close/peTTM/pbMRQ/amount/total_score/signals 等
        """
        self.table.setRowCount(len(results))
        for i, r in enumerate(results):
            self.table.setItem(i, 0, QtWidgets.QTableWidgetItem(r["symbol"]))
            self.table.setItem(i, 1, QtWidgets.QTableWidgetItem(r["name"]))

            close_item = QtWidgets.QTableWidgetItem(f"{r['close']:.2f}")
            self.table.setItem(i, 2, close_item)

            pe = r["peTTM"]
            pe_item = QtWidgets.QTableWidgetItem(f"{pe:.1f}" if pe else "N/A")
            self.table.setItem(i, 3, pe_item)

            pb = r["pbMRQ"]
            pb_item = QtWidgets.QTableWidgetItem(f"{pb:.2f}" if pb else "N/A")
            self.table.setItem(i, 4, pb_item)

            amt_item = QtWidgets.QTableWidgetItem(f"{r['amount']:,.0f}")
            self.table.setItem(i, 5, amt_item)

            score_item = QtWidgets.QTableWidgetItem(f"{r['total_score']:.3f}")
            self.table.setItem(i, 6, score_item)

            sig_item = QtWidgets.QTableWidgetItem(r["signals"])
            self.table.setItem(i, 7, sig_item)

            # 有看多信号的行标绿背景 (220, 255, 220 = 浅绿色)
            if r["bullish"] and not r["bearish"]:
                for col in range(8):
                    item = self.table.item(i, col)
                    if item:
                        item.setBackground(QtGui.QBrush(QtGui.QColor(220, 255, 220)))

        self.table.resizeColumnsToContents()

    def _on_select(self, item):
        """处理双击事件: 获取对应股票代码并发射信号

        Args:
            item: 被双击的 QTableWidgetItem
        """
        row = item.row()
        if 0 <= row < len(self._results):
            symbol = self._results[row]["symbol"]
            print(f"[DEBUG] 选股结果双击: row={row}, symbol={symbol}")
            self.stock_selected.emit(symbol)


class SignalTableWidget(QtWidgets.QWidget):
    """信号查看表格

    功能:
    - 从数据库加载指定日期的所有交易信号
    - 以表格形式展示 (代码/方向/评分/权重/原因/日期)
    - 双击股票发射 stock_selected 信号

    信号:
        stock_selected(str): 双击股票时发射，参数为 symbol
    """

    stock_selected = QtCore.Signal(str)

    def __init__(self, db: BaostockDatabase, parent=None):
        super().__init__(parent)
        self.db = db
        self._signals = []  # 信号数据列表
        self.init_ui()

    def init_ui(self):
        """初始化信号查看页面

        布局结构:
        ┌──────────────────────────────────────────────────┐
        │ [🔄 加载信号]  日期: [📅 2026-06-09]  (控制面板) │
        ├──────────────────────────────────────────────────┤
        │                                                  │
        │  信号表格 (QTableWidget, 6列)                    │
        │  代码 | 方向 | 评分 | 权重 | 原因 | 信号日期     │
        │                                                  │
        └──────────────────────────────────────────────────┘
        """
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)

        ctrl = QtWidgets.QWidget()
        ctrl.setFixedHeight(40)
        ctrl_layout = QtWidgets.QHBoxLayout(ctrl)
        ctrl_layout.setContentsMargins(4, 2, 4, 2)

        self.btn_load = QtWidgets.QPushButton("🔄 加载信号")
        self.btn_load.setObjectName("Primary")
        self.btn_load.clicked.connect(self._load_signals)
        ctrl_layout.addWidget(self.btn_load)

        # 日期选择器 (默认为今天)
        self.date_edit = QtWidgets.QDateEdit()
        self.date_edit.setDate(QtCore.QDate.currentDate())
        self.date_edit.setCalendarPopup(True)  # 弹出日历
        self.date_edit.setFixedWidth(120)
        ctrl_layout.addWidget(QtWidgets.QLabel("日期:"))
        ctrl_layout.addWidget(self.date_edit)

        ctrl_layout.addStretch()
        layout.addWidget(ctrl)

        # 信号表格 (6列)
        self.table = QtWidgets.QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels([
            "代码", "方向", "评分", "权重", "原因", "信号日期"
        ])
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)  # 只读
        self.table.itemDoubleClicked.connect(self._on_select)  # 双击发射信号
        layout.addWidget(self.table)

    def _load_signals(self):
        """从数据库加载指定日期的所有信号并填充表格"""
        date_str = self.date_edit.date().toString("yyyy-MM-dd")
        try:
            signals = self.db.load_all_signals(date_str)
            self._signals = signals
            self._populate_table(signals)
        except Exception as e:
            import traceback
            traceback.print_exc()

    def _populate_table(self, signals):
        """填充信号数据到表格

        Args:
            signals: 信号数据列表，每项包含 symbol/direction/score/target_weight/reason/signal_date
        """
        self.table.setRowCount(len(signals))
        for i, s in enumerate(signals):
            self.table.setItem(i, 0, QtWidgets.QTableWidgetItem(s.get("symbol", "")))
            self.table.setItem(i, 1, QtWidgets.QTableWidgetItem(s.get("direction", "")))
            self.table.setItem(i, 2, QtWidgets.QTableWidgetItem(f"{s.get('score', 0):.4f}"))
            self.table.setItem(i, 3, QtWidgets.QTableWidgetItem(f"{s.get('target_weight', 0):.1%}"))
            self.table.setItem(i, 4, QtWidgets.QTableWidgetItem(s.get("reason", "")))
            self.table.setItem(i, 5, QtWidgets.QTableWidgetItem(s.get("signal_date", "")))
        self.table.resizeColumnsToContents()

    def _on_select(self, item):
        """处理双击事件: 获取对应股票代码并发射信号

        Args:
            item: 被双击的 QTableWidgetItem
        """
        row = item.row()
        if 0 <= row < len(self._signals):
            symbol = self._signals[row].get("symbol", "")
            print(f"[DEBUG] 信号表格双击: row={row}, symbol={symbol}")
            self.stock_selected.emit(symbol)


class BaostockMainWindow(QtWidgets.QMainWindow):
    """Baostock 多因子选股与复盘主窗口

    整体布局结构:
    ┌───────────────────────────────────────────────────────────────────┐
    │  菜单栏 (系统 / 数据 / 帮助)                                       │
    ├───────┬───────────────────────────────────────────────────────────┤
    │       │  顶部行情栏 (QuoteBar)                                     │
    │       ├───────────────────────────────────────────────────────────┤
    │ 股票  │  ┌───────────────────────┬─────────────────────────────┐  │
    │ 列表  │  │  K线图 (KLineWidget)   │  ┌───────────────────────┐  │  │
    │ (200px│  ├───────────────────────┤  │  多因子选股           │  │  │
    │  固定 │  │  分时图 (TimeShare)    │  │  (FactorTableWidget)  │  │  │
    │  宽度)│  │                       │  ├───────────────────────┤  │  │
    │       │  │                       │  │  信号查看             │  │  │
    │       │  │                       │  │  (SignalTableWidget)  │  │  │
    │       │  └───────────────────────┘  └───────────────────────┘  │  │
    │       │  ← K线图/分时图 (3:2)  →  ← 右侧标签页 →                │  │
    │       └─────────────────────────────────────────────────────────┘  │
    └───────┴───────────────────────────────────────────────────────────┘
    │  状态栏                                                            │
    └───────────────────────────────────────────────────────────────────┘

    交互流程:
    1. 用户在左侧选择股票 → _on_stock_selected() → 加载K线和分时图
    2. 用户在选股结果中双击 → _on_factor_select() → 同上
    3. 用户在信号表格中双击 → _on_signal_select() → 同上
    4. 用户切换K线周期 → _on_period_changed() → 重新加载对应周期数据

    属性:
        db: BaostockDatabase — 数据库实例
        _current_symbol: 当前查看的股票代码
        _current_exchange: 当前查看的交易所
        _name_map: 股票代码到名称的映射表
    """

    def __init__(self):
        super().__init__()
        self.db = BaostockDatabase()
        self._current_symbol = None       # 当前查看的股票代码
        self._current_exchange = None     # 当前查看的交易所 (Exchange.SSE/SZSE)
        self._name_map = {}               # 股票代码 → 股票名称 映射表
        self.init_ui()
        self._load_name_map()

    def _load_name_map(self):
        """加载股票代码到名称的映射表

        从数据库获取所有股票基本信息，构建 {symbol: name} 字典。
        用于在行情栏中显示可读的股票名称而非代码。

        注意: 使用 try/except 静默失败，不影响主程序运行。
        """
        try:
            stock_list = self.db.get_stock_list()
            for s in stock_list:
                code = s["code"]
                if "." in code:
                    symbol = code.split(".")[1]
                else:
                    symbol = code
                self._name_map[symbol] = s.get("code_name", "")
            self.quote_bar.set_name_map(self._name_map)
        except Exception:
            pass

    def init_ui(self):
        """初始化主窗口 UI

        窗口大小: 1500 x 950
        页面背景: 浅灰色 (#FAFAFA)
        左侧栏: 200px 固定宽度，灰色背景 (#F5F5F5)

        组件创建顺序:
        1. 左侧股票列表面板
        2. 右侧内容面板:
           a. 顶部行情栏 (QuoteBar)
           b. 水平分割器 (QSplitter):
              - 左: K线图 + 分时图 (垂直排列, 比例 2:1)
              - 右: 多因子选股 / 信号查看 (QTabWidget)
        3. 菜单栏 (系统/数据/帮助)
        4. 状态栏
        """
        self.setWindowTitle("Baostock 多因子选股与复盘终端")
        self.resize(1500, 950)

        # === 中央区域 ===
        central = QtWidgets.QWidget()
        central.setObjectName("central")
        central.setStyleSheet("#central { background-color: #FAFAFA; }")
        self.setCentralWidget(central)
        main_layout = QtWidgets.QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # ====== 左侧: 股票列表面板 ======
        left_panel = QtWidgets.QWidget()
        left_panel.setObjectName("left_panel")
        left_panel.setFixedWidth(200)  # 固定宽度
        left_panel.setStyleSheet("#left_panel { background-color: #F5F5F5; border-right: 1px solid #E0E0E0; }")
        left_layout = QtWidgets.QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        self.stock_list = StockListWidget(self.db)
        self.stock_list.stock_selected.connect(self._on_stock_selected)
        left_layout.addWidget(self.stock_list)

        main_layout.addWidget(left_panel)

        # ====== 右侧: 主内容区 ======
        right_panel = QtWidgets.QWidget()
        right_panel.setObjectName("right_panel")
        right_panel.setStyleSheet("#right_panel { background-color: #FAFAFA; }")
        right_layout = QtWidgets.QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(0)

        # 顶部: 行情信息栏 (固定高度 100px)
        self.quote_bar = QuoteBar()
        self.quote_bar.setFixedHeight(100)
        self.quote_bar.setStyleSheet("background-color: #FFFFFF; border-bottom: 1px solid #E0E0E0;")
        right_layout.addWidget(self.quote_bar)

        # 中间: 水平分割器 — 左侧K线图/分时图，右侧功能标签页
        chart_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        chart_splitter.setHandleWidth(1)
        chart_splitter.setStretchFactor(0, 3)  # K线图/分时图占 3/5
        chart_splitter.setStretchFactor(1, 2)  # 功能标签页占 2/5

        # K线图在上，分时图在下 (垂直排列)
        kline_timeshare = QtWidgets.QWidget()
        kl_ts_layout = QtWidgets.QVBoxLayout(kline_timeshare)
        kl_ts_layout.setContentsMargins(0, 0, 0, 0)
        kl_ts_layout.setSpacing(0)

        self.kline = KLineWidget()
        self.kline.period_changed.connect(self._on_period_changed)
        self.kline.candle_clicked.connect(self._on_candle_clicked)
        kl_ts_layout.addWidget(self.kline, 2)  # 比例 2

        self.timeshare = TimeShareWidget()
        kl_ts_layout.addWidget(self.timeshare, 1)  # 比例 1

        chart_splitter.addWidget(kline_timeshare)

        # 右侧标签页: 多因子选股 / 信号查看
        self.right_tabs = QtWidgets.QTabWidget()

        # 多因子选股页
        self.factor_table = FactorTableWidget(self.db)
        self.factor_table.stock_selected.connect(self._on_factor_select)
        self.right_tabs.addTab(self.factor_table, "多因子选股")

        # 信号查看页
        self.signal_table = SignalTableWidget(self.db)
        self.signal_table.stock_selected.connect(self._on_signal_select)
        self.right_tabs.addTab(self.signal_table, "信号查看")

        chart_splitter.addWidget(self.right_tabs)

        right_layout.addWidget(chart_splitter, 1)

        main_layout.addWidget(right_panel)

        # ====== 菜单栏 ======
        menubar = self.menuBar()

        # 系统菜单: 退出
        sys_menu = menubar.addMenu("系统")
        exit_action = QtGui.QAction("退出", self)
        exit_action.triggered.connect(self.close)
        sys_menu.addAction(exit_action)

        # 数据菜单: 刷新股票列表 + 三个数据采集功能
        data_menu = menubar.addMenu("数据")
        refresh_action = QtGui.QAction("刷新股票列表", self)
        refresh_action.triggered.connect(self._refresh_stock_list)
        data_menu.addAction(refresh_action)

        data_menu.addSeparator()  # 分隔线

        # 更新 K 线数据
        update_kline_action = QtGui.QAction("更新K线数据", self)
        update_kline_action.triggered.connect(self._run_bar_collector)
        data_menu.addAction(update_kline_action)

        # 更新股票信息
        update_stock_action = QtGui.QAction("更新股票信息", self)
        update_stock_action.triggered.connect(self._run_stock_collector)
        data_menu.addAction(update_stock_action)

        # 更新财务数据
        update_financial_action = QtGui.QAction("更新财务数据", self)
        update_financial_action.triggered.connect(self._run_financial_collector)
        data_menu.addAction(update_financial_action)

        # 帮助菜单: 关于
        help_menu = menubar.addMenu("帮助")
        about_action = QtGui.QAction("关于", self)
        about_action.triggered.connect(self._show_about)
        help_menu.addAction(about_action)

        self.statusBar().showMessage("就绪")

    def _on_stock_selected(self, symbol: str, exchange):
        """处理左侧股票列表点击事件

        更新当前股票，加载K线和分时图数据

        Args:
            symbol: 股票代码
            exchange: 交易所 (Exchange.SSE/SZSE)
        """
        print(f"[DEBUG] _on_stock_selected: symbol={symbol}, exchange={exchange}")
        self._current_symbol = symbol
        self._current_exchange = exchange if exchange else (Exchange.SSE if symbol.startswith("6") else Exchange.SZSE)
        print(f"[DEBUG] → 调用 _load_kline_data({symbol}, {self._current_exchange})")
        self._load_kline_data(symbol, self._current_exchange)
        print(f"[DEBUG] → 调用 _load_timeshare({symbol}, {self._current_exchange})")
        self._load_timeshare(symbol, self._current_exchange)
        self.statusBar().showMessage(f"正在查看: {symbol}")

    def _on_factor_select(self, symbol: str):
        """处理选股结果双击事件

        根据代码推断交易所，加载K线和分时图数据

        Args:
            symbol: 股票代码
        """
        print(f"[DEBUG] _on_factor_select: symbol={symbol}")
        self._current_symbol = symbol
        self._current_exchange = Exchange.SSE if symbol.startswith("6") else Exchange.SZSE
        self._load_kline_data(symbol, self._current_exchange)
        self._load_timeshare(symbol, self._current_exchange)
        self.statusBar().showMessage(f"选股结果: {symbol}")

    def _on_signal_select(self, symbol: str):
        """处理信号表格双击事件

        Args:
            symbol: 股票代码
        """
        print(f"[DEBUG] _on_signal_select: symbol={symbol}")
        self._current_symbol = symbol
        self._current_exchange = Exchange.SSE if symbol.startswith("6") else Exchange.SZSE
        self._load_kline_data(symbol, self._current_exchange)
        self._load_timeshare(symbol, self._current_exchange)
        self.statusBar().showMessage(f"信号: {symbol}")

    def _load_kline_data(self, symbol: str, exchange: Exchange, interval: str = "DAILY"):
        """从数据库加载K线数据并更新图表

        流程:
        1. 根据 interval 参数确定数据库查询类型 (DAILY/WEEKLY/monthly)
        2. 查询最近一年的K线数据
        3. 更新行情栏 (最新价 + 涨跌幅)
        4. 更新K线图

        Args:
            symbol: 股票代码
            exchange: 交易所
            interval: 周期类型 "DAILY"/"WEEKLY"/"MONTHLY"
        """
        if not symbol:
            print(f"[DEBUG] _load_kline_data: symbol为空，跳过")
            return
        if not exchange:
            exchange = Exchange.SSE if symbol.startswith("6") else Exchange.SZSE
            self._current_exchange = exchange
        try:
            print(f"[DEBUG] _load_kline_data: 查询 {symbol} {interval} 数据...")
            # 映射周期类型
            if interval == "DAILY":
                db_interval = Interval.DAILY
            elif interval == "WEEKLY":
                db_interval = Interval.WEEKLY
            else:
                db_interval = "monthly"  # 月线用字符串表示

            # 查询最近一年的数据
            bars = self.db.load_bar_data(
                symbol, exchange, db_interval,
                datetime.now() - timedelta(days=365),
                datetime.now() + timedelta(days=1),
            )
            print(f"[DEBUG] _load_kline_data: 查询返回 {len(bars) if bars else 0} 条数据")

            if bars:
                latest = bars[-1]
                # 前一根K线的收盘价作为"昨收" (用于计算涨跌幅)
                pre_close = bars[-2].close_price if len(bars) >= 2 else latest.open_price
                self.quote_bar.update_quote(latest, pre_close)
                self.kline.update_data(bars, interval)
                interval_label = {"DAILY": "日线", "WEEKLY": "周线", "MONTHLY": "月线"}.get(interval, interval)
                self.statusBar().showMessage(f"{symbol} {interval_label} - {len(bars)} 条数据")
            else:
                self.statusBar().showMessage(f"{symbol} - 无数据")
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.statusBar().showMessage(f"加载失败: {e}")

    def _on_period_changed(self, interval: str):
        """处理K线周期切换事件

        非日线模式下隐藏分时图 (因为分时图只适用于日线数据)

        Args:
            interval: 新周期类型 "DAILY"/"WEEKLY"/"MONTHLY"
        """
        # 只在日线模式下显示分时图
        if interval == "DAILY":
            self.timeshare.show()
        else:
            self.timeshare.hide()
        # 更新K线图当前周期
        self.kline._current_interval = interval
        if self._current_symbol:
            self._load_kline_data(self._current_symbol, self._current_exchange, interval)

    def _on_candle_clicked(self, bar: BarData):
        """处理K线图蜡烛点击事件

        点击某根K线蜡烛后，分时图切换到该日的分时走势。

        Args:
            bar: 被点击的K线数据，包含 datetime 属性
        """
        target_date = bar.datetime.date()
        print(f"[DEBUG] _on_candle_clicked: date={target_date}")
        self.statusBar().showMessage(f"查看 {target_date} 分时走势...")
        self._load_timeshare(self._current_symbol, self._current_exchange, target_date=target_date)

    def _load_timeshare(self, symbol: str, exchange: Exchange, target_date=None):
        """加载指定交易日的分时数据

        使用5分钟数据 (baostock采集的是5分钟线) 来模拟分时图。

        流程:
        1. 仅在日线模式下加载
        2. 查询最近30天的5分钟数据
        3. 如果指定了 target_date，提取该日期的数据；否则提取最新交易日
        4. 获取前一日收盘价作为昨收 (用于计算涨跌和Y轴中心)
        5. 更新分时图

        Args:
            symbol: 股票代码
            exchange: 交易所
            target_date: 可选，指定加载哪一天的分时数据 (datetime.date 对象)
        """
        # 非日线模式隐藏分时图
        if self.kline._current_interval != "DAILY":
            print(f"[DEBUG] _load_timeshare: 当前周期={self.kline._current_interval}，非日线模式，隐藏")
            self.timeshare.hide()
            return
        if not symbol:
            print(f"[DEBUG] _load_timeshare: symbol为空，隐藏")
            self.timeshare.hide()
            return
        try:
            print(f"[DEBUG] _load_timeshare: 查询 {symbol} 5分钟数据...")
            # 使用5分钟数据（baostock采集的是5分钟线）
            bars = self.db.load_bar_data(
                symbol, exchange, Interval.MINUTE,
                datetime.now() - timedelta(days=30),
                datetime.now() + timedelta(days=1),
            )
            print(f"[DEBUG] _load_timeshare: 查询返回 {len(bars) if bars else 0} 条5分钟数据")
            if not bars:
                print(f"[DEBUG] _load_timeshare: 无5分钟数据，隐藏分时图")
                self.timeshare.hide()
                self.statusBar().showMessage(f"{symbol} - 无5分钟数据，分时图已隐藏")
                return

            # 确定要显示哪一天的分时数据
            if target_date:
                last_date = target_date
                print(f"[DEBUG] _load_timeshare: 指定日期={last_date}")
            else:
                last_date = bars[-1].datetime.date()
                print(f"[DEBUG] _load_timeshare: 默认最新交易日={last_date}")

            day_bars = [b for b in bars if b.datetime.date() == last_date]
            print(f"[DEBUG] _load_timeshare: {last_date} 当日数据 {len(day_bars)} 条")
            if not day_bars:
                print(f"[DEBUG] _load_timeshare: {last_date} 无分时数据，隐藏")
                self.timeshare.hide()
                self.statusBar().showMessage(f"{symbol} - {last_date} 无分时数据")
                return

            # 获取前一日收盘价作为昨收
            prev_bars = [b for b in bars if b.datetime.date() < last_date]
            pre_close = prev_bars[-1].close_price if prev_bars else day_bars[0].open_price
            print(f"[DEBUG] _load_timeshare: pre_close={pre_close}, 准备更新分时图")
            self.timeshare.show()
            self.timeshare.update_data(day_bars, pre_close)
            print(f"[DEBUG] _load_timeshare: 分时图更新完成")
            self.statusBar().showMessage(f"{symbol} 分时图 - {last_date} ({len(day_bars)} 条5分钟数据)")
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[DEBUG] _load_timeshare: 异常: {e}")
            self.timeshare.hide()
            self.statusBar().showMessage(f"{symbol} - 分时图加载失败: {e}")

    def _refresh_stock_list(self):
        """刷新左侧股票列表和名称映射"""
        self.stock_list.load_stocks_from_db()
        self._load_name_map()
        self.statusBar().showMessage("股票列表已刷新")

    def _get_collector_script(self, name: str) -> str:
        """获取采集器脚本的绝对路径

        Args:
            name: 采集器文件名 (不含路径)

        Returns:
            脚本的绝对路径
        """
        # 脚本位于当前文件同级目录的 baostock 子目录下
        base_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(base_dir, name)

    def _run_collector(self, script_name: str, display_name: str):
        """在后台线程中运行采集器脚本

        使用 subprocess.Popen 启动子进程，不阻塞 GUI 主线程。
        采集过程中状态栏显示进度提示，采集完成后刷新股票列表。

        Args:
            script_name: 采集器脚本文件名
            display_name: 显示名称 (用于状态栏提示)
        """
        import subprocess

        script_path = self._get_collector_script(script_name)
        if not os.path.exists(script_path):
            QtWidgets.QMessageBox.warning(self, "错误", f"采集器脚本不存在: {script_path}")
            return

        print(f"[DEBUG] 启动采集器: {display_name} ({script_name})")
        self.statusBar().showMessage(f"正在{display_name}...")

        def run_in_background():
            try:
                process = subprocess.Popen(
                    [sys.executable, script_path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="gbk",
                    cwd=os.path.dirname(script_path),
                )
                # 实时输出日志到控制台
                for line in process.stdout:
                    print(line, end="")
                process.wait()
                if process.returncode == 0:
                    print(f"[DEBUG] {display_name} 完成")
                    # 采集完成后刷新 GUI 中的股票列表
                    self.stock_list.load_stocks_from_db()
                    self._load_name_map()
                    self.statusBar().showMessage(f"{display_name} 完成")
                else:
                    print(f"[DEBUG] {display_name} 退出码={process.returncode}")
                    self.statusBar().showMessage(f"{display_name} 失败 (退出码 {process.returncode})")
            except Exception as e:
                print(f"[DEBUG] {display_name} 异常: {e}")
                self.statusBar().showMessage(f"{display_name} 异常: {e}")

        # 在后台线程运行，不阻塞 GUI
        thread = threading.Thread(target=run_in_background, daemon=True)
        thread.start()

    def _run_bar_collector(self):
        """更新K线数据 — 调用 baostock_bar_collector.py"""
        self._run_collector("baostock_bar_collector.py", "更新K线数据")

    def _run_stock_collector(self):
        """更新股票信息 — 调用 baostock_collector.py"""
        self._run_collector("baostock_collector.py", "更新股票信息")

    def _run_financial_collector(self):
        """更新财务数据 — 调用 baostock_financial_collector.py"""
        self._run_collector("baostock_financial_collector.py", "更新财务数据")

    def _show_about(self):
        """显示关于对话框"""
        QtWidgets.QMessageBox.information(
            self, "关于",
            "Baostock 多因子选股与复盘终端\n\n"
            "数据源: Baostock (免费，无需 token)\n"
            "数据库: PostgreSQL\n\n"
            "功能:\n"
            "• K线复盘（日/周/月线）\n"
            "• 多因子选股（PE/PB/成交额/质量）\n"
            "• 技术面过滤（MA/MACD/RSI/成交量）\n"
            "• 信号查看（每日收盘后生成）"
        )


def create_app():
    """创建 QApplication 实例并配置全局样式

    配置项:
    - pyqtgraph 抗锯齿 (antialias=True)
    - pyqtgraph 白色背景
    - pyqtgraph 灰色前景 (坐标轴/网格)
    - 全局字体: 微软雅黑 12px
    - 全局 QSS: LIGHT_THEME

    Returns:
        QApplication: 配置好的应用实例
    """
    # pyqtgraph 全局配置
    pg.setConfigOptions(antialias=True)           # 开启抗锯齿
    pg.setConfigOption("background", "#FFFFFF")   # 图表背景色
    pg.setConfigOption("foreground", "#999999")   # 前景色 (坐标轴/文字)

    qapp = QtWidgets.QApplication(sys.argv)
    qapp.setStyleSheet(LIGHT_THEME)
    font = QtGui.QFont("微软雅黑", 12)
    qapp.setFont(font)
    return qapp


def main():
    """程序入口函数

    流程:
    1. 打印启动信息
    2. 创建 QApplication (含全局样式)
    3. 创建主窗口并显示
    4. 进入事件循环
    """
    print("=" * 50)
    print("Baostock 多因子选股与复盘终端")
    print("=" * 50)

    qapp = create_app()
    main_window = BaostockMainWindow()
    main_window.show()

    sys.exit(qapp.exec())


if __name__ == "__main__":
    main()
