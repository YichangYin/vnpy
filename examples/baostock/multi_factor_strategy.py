"""
多因子选股策略模块

基于数据库 baostock_daily_basic 表数据：
- code: baostock代码（如 sh.600000）
- peTTM: 市盈率(滚动TTM)
- pbMRQ: 市净率(最近季度)
- amount: 成交额(元)
- close: 收盘价
- isST: 是否ST

因子体系:
- 估值因子: PE-TTM、PB-MRQ（越低越好）
- 动量因子: 收盘价（越高越好，反映近期趋势）
- 流动性因子: 成交额（越高越好）
- 质量因子: 1/PE 作为 ROE 代理（越高越好）

综合评分 = 各因子排名标准化后加权求和
"""

import pandas as pd
import numpy as np


class MultiFactorStrategy:
    """多因子选股策略

    基于 baostock_daily_basic 表数据，使用排名百分比法计算综合评分。
    排名法避免了极端值和缺失值的影响，适用于不同量纲的因子。
    """

    def __init__(
        self,
        weight_valuation: float = 0.30,     # 估值因子权重
        weight_momentum: float = 0.25,      # 动量因子权重
        weight_liquidity: float = 0.25,     # 流动性因子权重
        weight_quality: float = 0.20,       # 质量因子权重
    ):
        self.weights = {
            "valuation": weight_valuation,
            "momentum": weight_momentum,
            "liquidity": weight_liquidity,
            "quality": weight_quality,
        }

    def select_stocks(self, df_spot: pd.DataFrame, top_n: int = 30) -> pd.DataFrame:
        """从全市场数据中选股

        Args:
            df_spot: 全市场行情 DataFrame（来自 get_latest_daily_basic）
            top_n: 返回前 N 只股票

        Returns:
            按综合评分排序的 DataFrame
        """
        df = df_spot.copy()

        # ===== 数据清洗 =====
        # 过滤无效数据
        if "close" in df.columns:
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = df[df["close"] > 0]

        # ===== 因子计算 =====

        # 1. 估值因子 (PE 越低越好) → 排名百分比法
        df["valuation_score"] = self._rank_factor(df, "peTTM", ascending=True)

        # 2. 动量因子 (收盘价越高越好) → 排名百分比法
        df["momentum_score"] = self._rank_factor(df, "close", ascending=False)

        # 3. 流动性因子 (成交额越高越好) → 排名百分比法
        df["liquidity_score"] = self._rank_factor(df, "amount", ascending=False)

        # 4. 质量因子 (1/PE 越高越好，作为 ROE 代理) → 排名百分比法
        # PE < 0 的公司质量为 0
        if "peTTM" in df.columns:
            pe_vals = pd.to_numeric(df["peTTM"], errors="coerce")
            quality_vals = np.where(pe_vals > 0, 1.0 / pe_vals, 0)
            df["_quality_raw"] = quality_vals
            df["quality_score"] = self._rank_raw(df["_quality_raw"], ascending=False)
            df.drop(columns=["_quality_raw"], inplace=True)
        else:
            df["quality_score"] = 0.5

        # ===== 综合评分 =====
        df["total_score"] = (
            df["valuation_score"] * self.weights["valuation"]
            + df["momentum_score"] * self.weights["momentum"]
            + df["liquidity_score"] * self.weights["liquidity"]
            + df["quality_score"] * self.weights["quality"]
        )

        # ===== 排序筛选 =====
        df = df.sort_values("total_score", ascending=False).head(top_n)

        # ===== 构建返回列 =====
        return_cols = [
            "code",           # baostock代码（sh.600000）
            "close",          # 收盘价
            "peTTM",          # 市盈率
            "pbMRQ",          # 市净率
            "amount",         # 成交额
            "isST",           # 是否ST
            "total_score",    # 综合评分
            "valuation_score",
            "momentum_score",
            "liquidity_score",
            "quality_score",
        ]
        return_cols = [c for c in return_cols if c in df.columns]

        return df[return_cols]

    @staticmethod
    def _rank_factor(df: pd.DataFrame, col: str, ascending: bool = True) -> pd.Series:
        """对指定因子列进行排名标准化 (0~1)

        Args:
            df: 数据 DataFrame
            col: 因子列名
            ascending: True=值越小排名越高, False=值越大排名越高

        Returns:
            标准化后的分数 Series (0~1)，NaN 填充为 0.5
        """
        if col not in df.columns:
            return pd.Series(0.5, index=df.index)

        values = pd.to_numeric(df[col], errors="coerce")
        valid = values.dropna()
        if len(valid) == 0:
            return pd.Series(0.5, index=df.index)

        ranks = valid.rank(ascending=ascending, pct=True)
        result = pd.Series(0.5, index=df.index)
        result[valid.index] = ranks
        return result

    @staticmethod
    def _rank_raw(values: np.ndarray, ascending: bool = True) -> pd.Series:
        """对原始数组进行排名标准化 (0~1)"""
        series = pd.Series(values)
        valid = series.dropna()
        if len(valid) == 0:
            return pd.Series(0.5, index=series.index)

        ranks = valid.rank(ascending=ascending, pct=True)
        result = pd.Series(0.5, index=series.index)
        result[valid.index] = ranks
        return result
