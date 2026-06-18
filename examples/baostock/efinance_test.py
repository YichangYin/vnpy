"""
efinance K线数据接口测试

测试 efinance 的历史K线行情接口 (get_quote_history)，
验证日线/周线/月线/分钟线数据获取，与 baostock_bar_collector 采集数据对比。

安装: pip install efinance

使用方式:
    python efinance_test.py                    # 运行全部K线测试
    python efinance_test.py --daily            # 只测日线
    python efinance_test.py --weekly           # 只测周线
    python efinance_test.py --monthly          # 只测月线
    python efinance_test.py --minute           # 只测分钟线
    python efinance_test.py --compare          # 对比 efinance vs baostock 数据
"""

import sys
import os
import time
import requests
from datetime import datetime, timedelta

import efinance as ef
import pandas as pd

# 请求重试配置
MAX_RETRIES = 3
RETRY_DELAY = 2  # 秒


def retry_call(func, *args, max_retries=MAX_RETRIES, **kwargs):
    """带重试的函数调用，处理东方财富 API 连接不稳定的问题"""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.ConnectionError as e:
            if attempt < max_retries - 1:
                print(f"  {yellow(f'连接失败，{RETRY_DELAY}s 后重试 ({attempt+1}/{max_retries})')}...")
                time.sleep(RETRY_DELAY)
            else:
                raise
        except Exception:
            raise

# 测试用股票
TEST_STOCKS = {
    "浦发银行": "600000",
    "贵州茅台": "600519",
    "比亚迪": "002594",
    "宁德时代": "300750",
}

# 颜色
def green(text): return f"\033[32m{text}\033[0m"
def yellow(text): return f"\033[33m{text}\033[0m"
def red(text): return f"\033[31m{text}\033[0m"
def cyan(text): return f"\033[36m{text}\033[0m"

def section(title: str):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")

def print_df(df, n=5):
    if df is None or df.empty:
        print(f"  {yellow('无数据')}")
        return
    print(f"  {green('DataFrame')} 形状: {df.shape}  列: {list(df.columns)}")
    print(df.head(n).to_string(index=False))
    if len(df) > n:
        print(f"  ... ({len(df) - n} 行省略)")


# ==================== 1. 日线 (klt=101) ====================

def test_daily():
    """日线行情"""
    section("1. 日线行情 (klt=101)")
    for name, code in TEST_STOCKS.items():
        df = ef.stock.get_quote_history(
            code,
            beg="20240101",
            end="20261231",
            klt=101,
            fqt=1,  # 前复权
        )
        print(f"\n  {cyan(name)} ({code}) 日线:")
        print_df(df, 5)
        time.sleep(0.3)


# ==================== 2. 周线 (klt=102) ====================

def test_weekly():
    """周线行情"""
    section("2. 周线行情 (klt=102)")
    for name, code in TEST_STOCKS.items():
        df = ef.stock.get_quote_history(
            code,
            beg="20240101",
            end="20261231",
            klt=102,
            fqt=1,
        )
        print(f"\n  {cyan(name)} ({code}) 周线:")
        print_df(df, 5)
        time.sleep(0.3)


# ==================== 3. 月线 (klt=103) ====================

def test_monthly():
    """月线行情"""
    section("3. 月线行情 (klt=103)")
    for name, code in TEST_STOCKS.items():
        df = ef.stock.get_quote_history(
            code,
            beg="20200101",
            end="20261231",
            klt=103,
            fqt=1,
        )
        print(f"\n  {cyan(name)} ({code}) 月线:")
        print_df(df, 5)
        time.sleep(0.3)


# ==================== 4. 分钟线 ====================

def test_minute():
    """分钟线行情 (1/5/15/30/60分钟)"""
    section("4. 分钟线行情 (近10个交易日)")
    klt_map = {
        "1分钟": 1,
        "5分钟": 5,
        "15分钟": 15,
        "30分钟": 30,
        "60分钟": 60,
    }
    start = (datetime.now() - timedelta(days=15)).strftime("%Y%m%d")
    end = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
    code = "600519"  # 贵州茅台

    for label, klt in klt_map.items():
        df = ef.stock.get_quote_history(code, beg=start, end=end, klt=klt, fqt=1)
        print(f"\n  {cyan(label)} ({klt}):")
        print_df(df, 3)
        time.sleep(0.3)


# ==================== 5. 不复权 vs 前复权 vs 后复权 ====================

def test_fqt():
    """对比三种复权方式"""
    section("5. 复权方式对比 (不复权/前复权/后复权)")
    code = "600519"
    fqt_map = {0: "不复权", 1: "前复权", 2: "后复权"}
    for fqt, label in fqt_map.items():
        df = ef.stock.get_quote_history(code, beg="20260601", end="20260610", klt=101, fqt=fqt)
        print(f"\n  {label} (fqt={fqt}):")
        if not df.empty:
            print(df[["日期", "开盘", "收盘", "最高", "最低", "成交量"]].head(5).to_string(index=False))
        time.sleep(0.3)


# ==================== 6. 多股票批量 ====================

def test_batch():
    """多只股票批量查询"""
    section("6. 多股票批量日线查询")
    codes = list(TEST_STOCKS.values())
    dfs = ef.stock.get_quote_history(codes, beg="20260601", end="20260610", klt=101, fqt=1)
    if isinstance(dfs, dict):
        for code, df in dfs.items():
            print(f"\n  股票 {code}: {len(df)} 条")
            print(df.head(3).to_string(index=False))
    time.sleep(0.3)


# ==================== 7. 对比 baostock 数据 ====================

def compare_with_baostock():
    """对比 efinance 和 baostock_bar_collector 采集的数据库数据"""
    section("7. 数据对比: efinance vs baostock (数据库)")

    code = "600519"
    symbol = code
    from vnpy.trader.constant import Exchange, Interval

    # 导入数据库
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from baostock_database import BaostockDatabase
    db = BaostockDatabase()

    # 从数据库加载日线
    db_bars = db.load_bar_data(
        symbol, Exchange.SSE, Interval.DAILY,
        datetime.now() - timedelta(days=90),
        datetime.now() + timedelta(days=1),
    )
    print(f"\n  数据库日线: {len(db_bars)} 条")
    if db_bars:
        rows = [{"日期": b.datetime.strftime("%Y-%m-%d"), "开盘": b.open_price,
                 "收盘": b.close_price, "最高": b.high_price, "最低": b.low_price,
                 "成交量": b.volume} for b in db_bars[-5:]]
        print(pd.DataFrame(rows).to_string(index=False))

    # 从 efinance 获取
    ef_df = ef.stock.get_quote_history(code, beg="20260301", end="20261231", klt=101, fqt=1)
    print(f"\n  efinance日线: {len(ef_df)} 条")
    if not ef_df.empty:
        print(ef_df[["日期", "开盘", "收盘", "最高", "最低", "成交量"]].tail(5).to_string(index=False))

    # 对比最近交易日
    if db_bars and not ef_df.empty:
        db_last = db_bars[-1]
        ef_last = ef_df.iloc[-1]
        print(f"\n  最近交易日对比:")
        print(f"    数据库日期: {db_last.datetime.strftime('%Y-%m-%d')}, 收盘: {db_last.close_price:.2f}")
        print(f"    efinance日期: {ef_last['日期']}, 收盘: {ef_last['收盘']:.2f}")
        diff = abs(db_last.close_price - ef_last['收盘'])
        print(f"    收盘价差异: {diff:.2f} ({'✓ 一致' if diff < 0.01 else red('不一致')})")

    time.sleep(0.3)


# ==================== 主函数 ====================

TESTS = {
    "daily": test_daily,
    "weekly": test_weekly,
    "monthly": test_monthly,
    "minute": test_minute,
    "fqt": test_fqt,
    "batch": test_batch,
    "compare": compare_with_baostock,
}


def _check_network():
    """检测东方财富 API 网络连通性"""
    import socket
    try:
        socket.create_connection(("push2his.eastmoney.com", 443), timeout=5)
        print(f"  {green('网络检测')} push2his.eastmoney.com:443 可达")
        return True
    except Exception as e:
        print(f"  {red('网络检测')} push2his.eastmoney.com:443 不可达: {e}")
        print(f"  {yellow('提示')} 请检查网络连接或代理设置，或稍后重试")
        return False


def main():
    tock_df = ef.stock.get_quote_history(['600519','300750'], '20260601', '20260610', 101)
    print(tock_df)
    # import argparse
    # parser = argparse.ArgumentParser(description="efinance K线数据测试")
    # parser.add_argument("--daily", action="store_true", help="只测日线")
    # # parser.add_argument("--weekly", action="store_true", help="只测周线")
    # # parser.add_argument("--monthly", action="store_true", help="只测月线")
    # # parser.add_argument("--minute", action="store_true", help="只测分钟线")
    # # parser.add_argument("--fqt", action="store_true", help="测复权对比")
    # # parser.add_argument("--batch", action="store_true", help="测批量查询")
    # # parser.add_argument("--compare", action="store_true", help="对比baostock数据")
    # args = parser.parse_args()
    #
    # # 选择要运行的测试
    # selected = [k for k, v in vars(args).items() if v]
    # if not selected:
    #     # 默认运行日线和对比
    #     selected = ["daily", "minute", "fqt", "batch", "compare"]
    #
    # print("=" * 70)
    # print("  efinance K线数据测试")
    # print(f"  时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    # print(f"  测试项: {', '.join(selected)}")
    # print("=" * 70)
    #
    # # 网络连通性检测
    # if not _check_network():
    #     print(f"\n  {red('网络不可达，跳过测试')}")
    #     return
    #
    # ok, fail = 0, 0
    # for name in selected:
    #     try:
    #         TESTS[name]()
    #         ok += 1
    #     except Exception as e:
    #         fail += 1
    #         print(f"\n  {red('❌ 失败')}: {e}")
    #         import traceback
    #         traceback.print_exc()
    #     time.sleep(0.2)
    #
    # print(f"\n{'=' * 70}")
    # print(f"  {green('完成')}: 成功 {ok}, 失败 {fail}")
    # print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
