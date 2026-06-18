"""
信号生成脚本（独立进程，非 vn.py 内部）— Baostock 版

每日收盘后运行，执行以下流程:
1. 从数据库读取最新每日指标数据（PE/PB/成交额等）
2. 多因子选股筛选 Top N
3. 对候选股票进行技术面分析
4. 组合优化计算目标权重
5. 写入数据库 baostock_strategy_signals 表

使用方式:
    python signal_generator.py

可配合 APScheduler 每日 16:00 定时执行

前置要求:
    已运行 baostock_collector.py 采集基础数据
    已运行 baostock_bar_collector.py 采集 K 线数据
    已运行 baostock_financial_collector.py 采集财务数据
"""

import sys
import os
import logging
from datetime import datetime, timedelta

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from multi_factor_strategy import MultiFactorStrategy
from technical_analysis import TechnicalAnalysis
from baostock_database import BaostockDatabase

# ==================== 配置 ====================

DB_URL = os.environ.get(
    "BAOSTOCK_DB_URL",
    "postgresql://postgres:postgres@localhost:5432/baostock_vnpy"
)

# 日志目录
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
os.makedirs(LOG_DIR, exist_ok=True)

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "signal_generator.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# 初始化
db = BaostockDatabase(DB_URL)


def fetch_market_data() -> pd.DataFrame:
    """从数据库读取最新每日指标数据

    数据库表 baostock_daily_basic 字段:
    - code: baostock代码（如 sh.600000）
    - date: 交易日期
    - close: 收盘价
    - peTTM: 市盈率(滚动TTM)
    - pbMRQ: 市净率(最近季度)
    - psTTM: 市销率
    - pcfNcfTTM: 市现率
    - isST: 是否ST
    - turn: 换手率(%)
    - volume: 成交量(股)
    - amount: 成交额(元)
    """
    logger.info("正在从数据库读取最新每日指标...")

    records = db.get_latest_daily_basic()
    if not records:
        logger.warning("数据库无每日指标数据，退出")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    logger.info(f"读取到 {len(df)} 条每日指标记录")
    return df


def filter_candidates(df_spot: pd.DataFrame) -> pd.DataFrame:
    """初筛候选股票池"""
    df = df_spot.copy()

    # 过滤科创板(68)、北交所(4/8/bj)
    df = df[~df["code"].str.contains(r"\.68")]
    df = df[~df["code"].str.contains(r"^sh\.[84]")]
    df = df[~df["code"].str.contains(r"^bj\.")]

    # 排除停牌
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df = df[df["volume"] > 0]

    # 价格过滤
    if "close" in df.columns:
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df[(df["close"] >= 3) & (df["close"] <= 200)]

    # 排除 ST
    if "isST" in df.columns:
        df = df[df["isST"] != "1"]

    logger.info(f"初筛后候选: {len(df)} 只")
    return df


def technical_filter(
    candidates: pd.DataFrame,
    ta: TechnicalAnalysis,
) -> list[dict]:
    """对候选股票进行技术面分析"""
    final_signals = []

    for _, stock in candidates.iterrows():
        code = stock.get("code", "")
        if not code:
            continue

        try:
            # 解析 baostock 代码格式（如 sh.600000）
            if "." in code:
                symbol = code.split(".")[1]
                exchange = "SSE" if code.startswith("sh.") else "SZSE"
            else:
                symbol = code
                exchange = "SSE" if code.startswith("6") else "SZSE"

            # 从数据库读取日线数据
            bars = db.get_bar_data(symbol, exchange)
            if not bars or len(bars) < 60:
                continue

            # 转换为技术分析模块兼容的 DataFrame 格式
            df_hist = pd.DataFrame([
                {
                    "日期": b["datetime"].strftime("%Y-%m-%d"),
                    "收盘": b["close"],
                    "成交量": b["volume"],
                }
                for b in bars[-120:]  # 取最近120天
            ])

            signals = ta.generate_signals(df_hist)

            bullish = [s for s in signals if s in (
                "MA_GOLDEN_CROSS", "MACD_BULLISH", "RSI_OVERSOLD", "VOLUME_BREAKOUT"
            )]
            bearish = [s for s in signals if s in (
                "MA_DEATH_CROSS", "MACD_BEARISH", "RSI_OVERBOUGHT"
            )]

            if bullish and not bearish:
                final_signals.append({
                    "code": symbol,
                    "bs_code": code,
                    "price": float(stock.get("close", 0)),
                    "score": float(stock.get("total_score", 0)),
                    "signals": bullish,
                })

        except Exception as e:
            logger.debug(f"{code} 技术分析失败: {e}")
            continue

    logger.info(f"技术面过滤后: {len(final_signals)} 只")
    return final_signals


def generate_daily_signals():
    """每日收盘后运行，生成信号供 vn.py 次日读取"""
    logger.info("=" * 50)
    logger.info("开始生成交易信号")
    logger.info("=" * 50)

    try:
        # 1. 获取全市场行情数据
        df_spot = fetch_market_data()
        if df_spot.empty:
            logger.error("未获取到行情数据，退出")
            return

        # 2. 初筛
        candidates = filter_candidates(df_spot)
        if candidates.empty:
            logger.warning("无候选股票")
            return

        # 3. 多因子选股
        mf = MultiFactorStrategy()
        selected = mf.select_stocks(candidates, top_n=30)
        logger.info(f"多因子选股完成: {len(selected)} 只")

        if selected.empty:
            logger.warning("多因子选股无结果")
            return

        # 4. 技术面过滤
        ta = TechnicalAnalysis()
        final_signals = technical_filter(selected, ta)
        logger.info(f"技术面过滤完成: {len(final_signals)} 只")

        if not final_signals:
            logger.warning("无符合条件的信号")
            return

        # 5. 组合优化
        weight = min(0.05, 1.0 / len(final_signals))
        signal_date = datetime.now().strftime("%Y-%m-%d")

        records = []
        for item in final_signals:
            code = item["code"]
            exchange = "SSE" if code.startswith("6") else "SZSE"
            records.append({
                "signal_date": signal_date,
                "symbol": code,
                "exchange": exchange,
                "direction": "LONG",
                "score": round(float(item["score"]), 4),
                "reason": "|".join(item["signals"]),
                "target_weight": round(weight, 4),
            })

        # 6. 写入数据库
        count = db.save_signals(records)
        logger.info(f"✅ 已生成 {count} 条信号写入 baostock_strategy_signals")

        # 7. 清理过期信号（保留7天）
        cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        deleted = db.delete_expired_signals(cutoff)
        logger.info(f"清理 {deleted} 条过期信号")

        # 8. 打印摘要
        logger.info("-" * 50)
        for r in records[:10]:
            logger.info(f"  {r['symbol']} | 评分:{r['score']:.2f} | "
                       f"权重:{r['target_weight']:.1%} | {r['reason']}")
        if len(records) > 10:
            logger.info(f"  ... 还有 {len(records) - 10} 只")

    except Exception as e:
        logger.error(f"信号生成失败: {e}", exc_info=True)
    finally:
        logger.info("=" * 50)
        logger.info("信号生成完成")
        logger.info("=" * 50)


if __name__ == "__main__":
    generate_daily_signals()
