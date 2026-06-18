#!/usr/bin/env python
"""baostock 接口连通性 + 耗时测试 — 基于官方 API 文档

官方文档: https://www.baostock.com/mainContent?file=pythonAPI.md

API 签名对照:
  bs.login()
  bs.logout()
  bs.query_stock_basic()
  bs.query_stock_industry(date="YYYY-MM-DD")
  bs.query_profit_data(code="sh.XXXXXX", year=YYYY, quarter=N)
  bs.query_growth_data(code="sh.XXXXXX", year=YYYY, quarter=N)
  bs.query_balance_data(code="sh.XXXXXX", year=YYYY, quarter=N)
  bs.query_cash_flow_data(code="sh.XXXXXX", year=YYYY, quarter=N)
  bs.query_history_k_data_plus(code, fields, start_date, end_date, frequency, adjustflag)
"""
import time
import sys
import os
import threading
import baostock as bs
import pandas as pd

# 日志和数据目录
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CSV_DIR = os.path.join(DATA_DIR, "csv")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

API_TIMEOUT = 30  # 单接口超时时间（秒）


def timeout_call(func, timeout=API_TIMEOUT, *args, **kwargs):
    """在子线程中执行 API，主线程等待超时"""
    result = [None]

    def _run():
        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            result[0] = type("Rs", (), {"error_code": "-1", "error_msg": str(e)})()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=timeout)

    if t.is_alive():
        return None
    return result[0]


def timed_call(name, func, *args, **kwargs):
    t0 = time.time()
    rs = timeout_call(func, API_TIMEOUT, *args, **kwargs)
    elapsed = time.time() - t0

    if rs is None:
        print(f"   耗时:   {elapsed:.2f}s (超时)")
        print(f"   状态:   TIMEOUT (>{API_TIMEOUT}s)")
        return None, elapsed

    print(f"   耗时:   {elapsed:.2f}s")
    print(f"   返回码: {rs.error_code}")
    print(f"   消息:   {rs.error_msg}")
    return rs, elapsed


def count_rows(rs, csv_file=None):
    """读取结果集并统计行数，可选导出到 CSV"""
    data_list = []
    while rs.error_code == "0" and rs.next():
        data_list.append(rs.get_row_data())
    if data_list:
        result = pd.DataFrame(data_list, columns=rs.fields)
        print(f"   行数:   {len(result)}")
        if csv_file:
            result.to_csv(csv_file, index=False, encoding="utf-8-sig")
            print(f"   已导出: {csv_file}")
        return len(result)
    print(f"   行数:   0")
    return 0


def print_summary(results):
    print("\n" + "=" * 50)
    print("耗时汇总 (建议超时 = 实际耗时 x 3)")
    print("=" * 50)
    max_time = 0
    for name, t, ok in results:
        status = f"{t:6.2f}s" if ok else f"{t:6.2f}s (超时)"
        suggested = max(int(t * 3), 5) if ok else "N/A"
        print(f"   {name:20s} {status}  -> 建议超时 {suggested}")
        if ok and t > max_time:
            max_time = t
    if max_time > 0:
        print(f"\n   最慢接口: {max_time:.2f}s")
        print(f"   建议全局超时: {max(int(max_time * 3), 30)}s")
    else:
        print(f"\n   所有接口均超时，建议检查网络")


# ========== 主流程 ==========

print("=" * 55)
print("baostock 接口连通性 + 耗时测试（官方 API 对照）")
print("=" * 55)

# 1. 登录 — bs.login() 无参数
print("\n1. bs.login()")
lg, _ = timed_call("login", bs.login)
if lg is None:
    print("\n   登录超时，程序退出")
    sys.exit(1)
if lg.error_code != "0":
    print(f"\n   登录失败: {lg.error_msg}")
    sys.exit(1)

results = []

# 2. 股票列表 — bs.query_stock_basic() 无参数
print("\n2. bs.query_stock_basic()")
# rs, t = timed_call("query_stock_basic", bs.query_stock_basic)
# if rs is not None and rs.error_code == "0" and rs.next():
#     print(f"   数据:   {rs.get_row_data()}")
# results.append(("query_stock_basic", t, rs is not None and rs.error_code == "0"))

# 3. 日线 — bs.query_history_k_data_plus(code, fields, start_date, end_date, frequency, adjustflag)
print("\n3. bs.query_history_k_data_plus('sh.600000', ..., frequency='d')")
rs, t = timed_call(
    "query_history_k_data_plus",
    bs.query_history_k_data_plus, "sh.600000",
    "date,open,high,low,close,volume,amount",
    start_date="2025-05-01", end_date="2025-06-02",
    frequency="d", adjustflag="2")
if rs is not None and rs.error_code == "0":
    count_rows(rs, os.path.join(CSV_DIR, "history_k_data_d.csv"))
results.append(("日线行情(d)", t, rs is not None and rs.error_code == "0"))

# 4. 5分钟线 — frequency="5"
print("\n4. bs.query_history_k_data_plus('sh.600000', ..., frequency='5')")
rs, t = timed_call(
    "query_history_k_data_plus",
    bs.query_history_k_data_plus, "",
    "date,open,high,low,close,volume,amount",
    start_date="2025-06-01", end_date="2025-06-02",
    frequency="d", adjustflag="2")
if rs is not None and rs.error_code == "0":
    count_rows(rs, os.path.join(CSV_DIR, "history_k_data_5min.csv"))
results.append(("5分钟线(5)", t, rs is not None and rs.error_code == "0"))

# 5. 利润表 — bs.query_profit_data(code, year, quarter) 全部关键字参数
print("\n5. bs.query_profit_data(code='sh.600000', year=2025, quarter=1)")
rs, t = timed_call(
    "query_profit_data",
    bs.query_profit_data, code="sh.600000", year=2025, quarter=1)
if rs is not None and rs.error_code == "0":
    count_rows(rs, os.path.join(CSV_DIR, "profit_data.csv"))
results.append(("利润表", t, rs is not None and rs.error_code == "0"))

# 6. 行业分类 — bs.query_stock_industry(date="YYYY-MM-DD")
print("\n6. bs.query_stock_industry(date='2025-06-02')")
rs, t = timed_call(
    "query_stock_industry",
    bs.query_stock_industry, date="2026-06-02", code='sh.600000')
if rs is not None and rs.error_code == "0":
    count_rows(rs, os.path.join(CSV_DIR, "industry_data.csv"))
results.append(("行业分类", t, rs is not None and rs.error_code == "0"))

# 登出 + 汇总
bs.logout()
print_summary(results)
