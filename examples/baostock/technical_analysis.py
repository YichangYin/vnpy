"""
技术分析模块

对单只股票的历史数据进行技术指标计算，生成交易信号

支持的信号类型:
- MA_GOLDEN_CROSS: 均线金叉（短期均线上穿长期均线）
- MA_DEATH_CROSS: 均线死叉（短期均线下穿长期均线）
- MACD_BULLISH: MACD 金叉/多头
- MACD_BEARISH: MACD 死叉/空头
- RSI_OVERBOUGHT: RSI 超买 (>70)
- RSI_OVERSOLD: RSI 超卖 (<30)
- VOLUME_BREAKOUT: 放量突破（成交量 > 20日均量 * 1.5）
"""

import pandas as pd
import numpy as np


class TechnicalAnalysis:
    """技术分析信号生成器

    支持的信号类型:
    - MA_GOLDEN_CROSS: 均线金叉（短期均线上穿长期均线）
    - MA_DEATH_CROSS: 均线死叉（短期均线下穿长期均线）
    - MACD_BULLISH: MACD 金叉/多头
    - MACD_BEARISH: MACD 死叉/空头
    - RSI_OVERBOUGHT: RSI 超买 (>70)
    - RSI_OVERSOLD: RSI 超卖 (<30)
    - VOLUME_BREAKOUT: 放量突破（成交量 > 20日均量 * 1.5）
    """

    def __init__(
        self,
        ma_short: int = 5,
        ma_long: int = 20,
        rsi_period: int = 14,
        volume_multiplier: float = 1.5,
    ):
        self.ma_short = ma_short
        self.ma_long = ma_long
        self.rsi_period = rsi_period
        self.volume_multiplier = volume_multiplier

    def generate_signals(self, df_hist: pd.DataFrame) -> list[str]:
        """根据历史数据生成技术信号

        Args:
            df_hist: 历史行情 DataFrame，需包含列:
                     trade_date/日期, open/开盘, close/收盘,
                     high/最高, low/最低, vol/成交量

        Returns:
            信号列表，如 ['MA_GOLDEN_CROSS', 'MACD_BULLISH']
        """
        df = df_hist.copy()
        signals = []

        if len(df) < self.ma_long:
            return signals

        # 统一列名映射（支持多种数据源格式）
        # 数据库格式: 日期, 收盘, 成交量
        # baostock API: date, close, volume
        col_map = {
            "trade_date": "日期",
            "日期": "日期",
            "date": "日期",
            "open": "开盘",
            "开盘": "开盘",
            "close": "收盘",
            "收盘": "收盘",
            "high": "最高",
            "最高": "最高",
            "low": "最低",
            "最低": "最低",
            "vol": "成交量",
            "volume": "成交量",
            "成交量": "成交量",
            "amount": "成交额",
            "成交额": "成交额",
            "pct_chg": "涨跌幅",
            "涨跌幅": "涨跌幅",
        }
        df.rename(columns=col_map, inplace=True)

        # 确保数据类型
        df["收盘"] = pd.to_numeric(df["收盘"], errors="coerce")
        df["成交量"] = pd.to_numeric(df["成交量"], errors="coerce")
        df = df.dropna(subset=["收盘", "成交量"])

        if len(df) < self.ma_long:
            return signals

        close = df["收盘"].values
        volume = df["成交量"].values

        # ===== 1. 均线信号 =====
        ma_s = self._sma(close, self.ma_short)
        ma_l = self._sma(close, self.ma_long)

        if len(ma_s) >= 2 and len(ma_l) >= 2:
            if ma_s[-1] > ma_l[-1] and ma_s[-2] <= ma_l[-2]:
                signals.append("MA_GOLDEN_CROSS")
            elif ma_s[-1] < ma_l[-1] and ma_s[-2] >= ma_l[-2]:
                signals.append("MA_DEATH_CROSS")

        # ===== 2. MACD 信号 =====
        macd, signal_line, hist = self._macd(close)
        if len(macd) >= 2 and len(signal_line) >= 2:
            if macd[-1] > signal_line[-1] and macd[-2] <= signal_line[-2]:
                signals.append("MACD_BULLISH")
            elif macd[-1] < signal_line[-1] and macd[-2] >= signal_line[-2]:
                signals.append("MACD_BEARISH")

        # ===== 3. RSI 信号 =====
        rsi = self._rsi(close, self.rsi_period)
        if len(rsi) >= 1:
            if rsi[-1] > 70:
                signals.append("RSI_OVERBOUGHT")
            elif rsi[-1] < 30:
                signals.append("RSI_OVERSOLD")

        # ===== 4. 成交量信号 =====
        if len(volume) >= 20:
            avg_vol = np.mean(volume[-20:-1])
            if volume[-1] > avg_vol * self.volume_multiplier:
                signals.append("VOLUME_BREAKOUT")

        return signals

    @staticmethod
    def _sma(data: np.ndarray, period: int) -> np.ndarray:
        """简单移动平均"""
        cumsum = np.cumsum(np.insert(data, 0, 0))
        return (cumsum[period:] - cumsum[:-period]) / period

    @staticmethod
    def _ema(data: np.ndarray, period: int) -> np.ndarray:
        """指数移动平均"""
        ema = np.zeros_like(data)
        ema[0] = data[0]
        multiplier = 2.0 / (period + 1)
        for i in range(1, len(data)):
            ema[i] = (data[i] - ema[i - 1]) * multiplier + ema[i - 1]
        return ema

    @staticmethod
    def _macd(data: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9):
        """MACD 计算"""
        ema_fast = TechnicalAnalysis._ema(data, fast)
        ema_slow = TechnicalAnalysis._ema(data, slow)
        macd_line = ema_fast - ema_slow
        signal_line = TechnicalAnalysis._ema(macd_line, signal)
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def _rsi(data: np.ndarray, period: int = 14) -> np.ndarray:
        """RSI 计算"""
        if len(data) < period + 1:
            return np.array([])

        deltas = np.diff(data)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        avg_gain = np.zeros(len(data))
        avg_loss = np.zeros(len(data))

        avg_gain[period] = np.mean(gains[:period])
        avg_loss[period] = np.mean(losses[:period])

        for i in range(period + 1, len(data)):
            avg_gain[i] = (avg_gain[i - 1] * (period - 1) + gains[i - 1]) / period
            avg_loss[i] = (avg_loss[i - 1] * (period - 1) + losses[i - 1]) / period

        # 计算 RS 和 RSI
        # 使用 np.divide 的 where 参数避免除零警告
        rs = np.divide(avg_gain, avg_loss, out=np.zeros_like(avg_gain), where=avg_loss != 0)
        rs = np.where(avg_loss > 0, rs, 100)
        rsi = 100 - (100 / (1 + rs))
        return rsi
