"""
Baostock 股票基础数据采集器

  [步骤1] 股票列表              — bs.query_stock_basic()
  [步骤2] 行业分类              — bs.query_stock_industry(date=xxx)

财务数据采集请使用 baostock_financial_collector.py
"""

import sys
import os
import logging
import time
import json
import threading
from datetime import datetime, timedelta

import baostock as bs
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from baostock_database import BaostockDatabase

# ==================== 配置 ====================

DB_URL = os.environ.get(
    "BAOSTOCK_DB_URL",
    "postgresql://postgres:postgres@localhost:5432/baostock_vnpy"
)

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

PROGRESS_FILE = os.path.join(DATA_DIR, "collect_progress.json")
API_COUNTER_FILE = os.path.join(DATA_DIR, "api_request_counter.json")
API_LOG_FILE = os.path.join(LOG_DIR, "api_call.log")
API_TIMEOUT = 30
MAX_DAILY_REQUESTS = 45000
STOCK_LIST_LIMIT = 0

bs_lock = threading.Lock()

# ==================== API 请求计数器 ====================

_counter_lock = threading.Lock()


def _counter_load() -> dict:
    today = datetime.now().strftime("%Y-%m-%d")
    if os.path.exists(API_COUNTER_FILE):
        try:
            with open(API_COUNTER_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("date") != today:
                return {"date": today, "success": 0, "retry": 0, "timeout": 0,
                        "conn_reset": 0, "fail": 0, "total": 0}
            return data
        except Exception:
            pass
    return {"date": today, "success": 0, "retry": 0, "timeout": 0,
            "conn_reset": 0, "fail": 0, "total": 0}


def _counter_save(data: dict):
    with open(API_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def api_counter_increment(key: str) -> int:
    with _counter_lock:
        data = _counter_load()
        data[key] = data.get(key, 0) + 1
        if key in ("success", "retry", "fail"):
            data["total"] = data.get("total", 0) + 1
        _counter_save(data)
        return data["total"]


def api_counter_get() -> dict:
    with _counter_lock:
        return _counter_load()


def api_counter_check():
    total = api_counter_total()
    if total >= MAX_DAILY_REQUESTS:
        raise SystemExit(
            f"今日 API 请求已达上限 {MAX_DAILY_REQUESTS} 次（当前 {total} 次），"
            f"程序退出。下次运行将自动重置计数。")


def api_counter_total() -> int:
    with _counter_lock:
        return _counter_load().get("total", 0)


def api_counter_reset():
    with _counter_lock:
        _counter_save({"date": datetime.now().strftime("%Y-%m-%d"),
                       "success": 0, "retry": 0, "timeout": 0,
                       "conn_reset": 0, "fail": 0, "total": 0})


def api_stats_log(stats: dict):
    total = stats.get("total", stats["success"] + stats.get("retry", 0) + stats.get("fail", 0))
    logger.info(f"API 调用汇总（{stats['date']}）: "
               f"成功 {stats['success']} 次, "
               f"重试成功 {stats.get('retry', 0)} 次, "
               f"超时 {stats.get('timeout', 0)} 次, "
               f"连接断开 {stats.get('conn_reset', 0)} 次, "
               f"失败 {stats.get('fail', 0)} 次, "
               f"总请求 {total}/{MAX_DAILY_REQUESTS} 次")

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "baostock_collector.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

api_logger = logging.getLogger("api_call")
api_logger.setLevel(logging.DEBUG)
api_logger.propagate = False
api_logger.addHandler(logging.FileHandler(API_LOG_FILE, encoding="utf-8"))


def _should_include(code: str) -> bool:
    if code.startswith("sh.68") or code.startswith(("sh.8", "sh.4", "bj.")):
        return False
    return code.startswith(("sh.60", "sz.00", "sz.30"))


def _classify_board(code: str) -> str:
    if code.startswith("sh.68"): return "40"
    if code.startswith("sz.30"): return "30"
    if code.startswith("bj."): return "50"
    return "10"


def _fetch_rs_to_df(rs) -> pd.DataFrame:
    data_list = []
    while rs.error_code == "0" and rs.next():
        data_list.append(rs.get_row_data())
    return pd.DataFrame(data_list, columns=rs.fields) if data_list else pd.DataFrame()


# ==================== API 调用封装 ====================

def _reconnect_and_continue(func_name: str, attempt: int, max_retries: int) -> bool:
    """重连 baostock，所有失败情形均 return False，由外层循环控制重试"""
    try:
        api_logger.info(f"⚡ 正在重新登录 baostock（{func_name} 第 {attempt + 1} 次）...")
        logger.info(f"[API] 重新登录 baostock（第 {attempt + 1} 次）")
        with bs_lock:
            time.sleep(3)
            bs.logout()
            login_result = [None]
            def _login():
                try:
                    login_result[0] = bs.login()
                except Exception as e:
                    login_result[0] = type("Rs", (), {"error_code": "-1", "error_msg": str(e)})()
            t = threading.Thread(target=_login, daemon=True)
            t.start()
            t.join(timeout=30)
            if t.is_alive():
                api_logger.error("⚡ login 超时（30s），判定重连失败")
                logger.error("[API] login 超时（30s），判定重连失败")
                return False
            lg = login_result[0]

        if lg is not None and lg.error_code == "0":
            api_logger.info("✅ 重新登录成功")
            logger.info("[API] 重新登录成功")
        else:
            api_logger.error(f"❌ 重新登录失败: {getattr(lg, 'error_msg', '未知')}")
            logger.error(f"[API] 重新登录失败: {getattr(lg, 'error_msg', '未知')}")
            return False
    except Exception as e:
        api_logger.error(f"❌ 重新登录异常: {e}")
        logger.error(f"[API] 重新登录异常: {e}")
        return False

    return True


def _bs_query(func, *args, max_retries=5, **kwargs):
    func_name = func.__name__ if hasattr(func, '__name__') else str(func)
    params_list = [str(a) for a in args[:2]]
    params_list.extend([f"{k}={v}" for k, v in kwargs.items()
                        if k in ("code", "start_date", "end_date", "frequency", "year", "quarter", "date")])
    params = ", ".join(params_list)

    attempt = 0
    while attempt < max_retries:
        result = [None]
        start_time = time.time()
        request_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        def _call():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as e:
                result[0] = type("Rs", (), {"error_code": "-1", "error_msg": str(e)})()

        t = threading.Thread(target=_call, daemon=True)
        t.start()
        t.join(timeout=API_TIMEOUT)
        elapsed = time.time() - start_time

        if t.is_alive():
            api_counter_increment("timeout")
            api_counter_check()
            api_logger.warning(f"超时 | {request_time} | {func_name} | {params} | 耗时 {elapsed:.2f}s")
            logger.warning(f"[API] 超时 {func_name} (第 {attempt + 1} 次)")
            _reconnect_and_continue(func_name, attempt, max_retries)
            attempt += 1
            logger.info(f"[API] 超时重连后重试 {func_name} (第 {attempt + 1} 次)")
            continue

        rs = result[0]
        is_conn_reset = False
        if rs.error_code != "0":
            err_msg = getattr(rs, 'error_msg', '')
            keywords = ["远程主机强迫关闭", "10054", "接收数据异常", "网络接收错误", "10002007", "connection reset"]
            is_conn_reset = any(kw.lower() in str(err_msg).lower() for kw in keywords)

        if is_conn_reset:
            api_counter_increment("conn_reset")
            api_counter_check()
            api_logger.warning(f"连接断开 | {request_time} | {func_name} | {params} | error={rs.error_code}")
            logger.warning(f"[API] 连接断开 {func_name}")
            _reconnect_and_continue(func_name, attempt, max_retries)
            attempt += 1
            logger.info(f"[API] 连接重连后重试 {func_name} (第 {attempt + 1} 次)")
            continue

        if rs.error_code == "0":
            if attempt == 0:
                api_counter_increment("success")
                api_counter_check()
                api_logger.debug(f"成功 | {request_time} | {func_name} | {params} | 耗时 {elapsed:.2f}s")
            else:
                api_counter_increment("retry")
                api_counter_check()
                api_logger.info(f"重试成功 | {request_time} | {func_name} | {params} | 耗时 {elapsed:.2f}s")
                logger.info(f"[API] 重试成功 {func_name} (第 {attempt + 1} 次)")
            return rs

        if attempt == 0:
            api_counter_increment("fail")
            api_counter_check()
            api_logger.warning(f"失败 | {request_time} | {func_name} | {params} | error={rs.error_code}")
            logger.warning(f"[API] 失败 {func_name}")
        attempt += 1
        if attempt < max_retries:
            time.sleep(1 * attempt)

    api_logger.error(f"❌ 重试失败 | {request_time} | {func_name} | {params}")
    raise SystemExit(f"API 请求失败，已重试 {max_retries} 次: {func_name}")


# ==================== 进度管理 ====================

class Progress:
    def __init__(self, filepath: str = PROGRESS_FILE):
        self.filepath = filepath
        self.data = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            "last_run": None,
            "steps": {
                "1_stock_list": {"done": True, "idx": 0},
                "2_stock_industry": {"done": True, "idx": 0},
            }
        }

    def save(self):
        self.data["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 添加结构说明注释（首次写入时）
        if "_comment" not in self.data:
            self.data["_comment"] = {
                "description": "股票基础数据采集进度文件",
                "step_keys": {
                    "1_stock_list": "股票列表", "2_stock_industry": "行业分类"
                },
                "fields": {
                    "done": "步骤是否已完成（true/false）",
                    "idx": "当前处理到的股票索引（用于断点续采）"
                }
            }
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def mark_step_done(self, step_key: str):
        self.data["steps"][step_key] = {"done": True, "idx": 0}
        self.save()

    def is_step_done(self, step_key: str) -> bool:
        return self.data["steps"].get(step_key, {}).get("done", False)

    def reset(self):
        for key in self.data["steps"]:
            self.data["steps"][key] = {"done": False, "idx": 0}
        self.save()


# ==================== 采集器 ====================

class BaostockCollector:
    def __init__(self, db_url: str = DB_URL):
        self.db = BaostockDatabase(db_url)
        self.progress = Progress()
        self._logged_in = False

    def login(self) -> bool:
        if self._logged_in:
            return True
        lg = bs.login()
        if lg.error_code != "0":
            logger.error(f"Baostock 登录失败: {lg.error_msg}")
            return False
        self._logged_in = True
        logger.info("Baostock 登录成功")
        counter = api_counter_get()
        logger.info(f"今日已请求 {counter.get('total', 0)}/{MAX_DAILY_REQUESTS} 次")
        return True

    def logout(self):
        if self._logged_in:
            counter = api_counter_get()
            logger.info(f"退出时 API 总请求: {counter.get('total', 0)} 次")
            bs.logout()
            self._logged_in = False
            logger.info("Baostock 已登出")

    # ========== 步骤1: 股票列表 ==========

    def step1_stock_list(self) -> list[dict]:
        logger.info("=" * 60)
        logger.info("[步骤1/2] 股票列表")
        logger.info("=" * 60)

        rs = _bs_query(bs.query_stock_basic)
        if rs.error_code != "0":
            logger.error(f"  获取失败: {rs.error_msg}")
            return []

        stocks, basic_records = [], []
        for _, row in _fetch_rs_to_df(rs).iterrows():
            code = str(row["code"])
            if _should_include(code):
                stocks.append({"code": code, "code_name": str(row.get("code_name", "")),
                               "industry": str(row.get("industry", "")),
                               "industryClassification": str(row.get("industryClassification", ""))})
                basic_records.append({
                    "baostock_code": code, "security_code": code.split(".")[1],
                    "security_name": str(row.get("code_name", "")),
                    "exchange": "sse" if code.startswith("sh.") else "szse",
                    "board": _classify_board(code), "status": str(row.get("status", "1")),
                    "market": "1", "is_hs": str(row.get("is_hs", "")),
                    "list_date": str(row.get("ipoDate", "19900101")).replace("-", ""),
                    "delist_date": str(row.get("outDate", "")).replace("-", ""),
                    "industry": str(row.get("industry", "")),
                })

        stocks.sort(key=lambda x: x["code"])
        if STOCK_LIST_LIMIT > 0:
            stocks = stocks[:STOCK_LIST_LIMIT]
            logger.info(f"[调试] 限制股票列表为 {STOCK_LIST_LIMIT} 只")

        self.db.save_stock_list(stocks)
        self.db.save_basic_info(basic_records)
        self.progress.mark_step_done("1_stock_list")
        logger.info(f"  保存 {len(stocks)} 只股票")
        return stocks

    # ========== 步骤2: 行业分类 ==========

    def step2_stock_industry(self) -> int:
        logger.info("=" * 60)
        logger.info("[步骤2/2] 行业分类")
        logger.info("=" * 60)

        date_str = datetime.now().strftime("%Y-%m-%d")
        rs = _bs_query(bs.query_stock_industry, date=date_str)
        if rs.error_code != "0":
            date_str = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            rs = _bs_query(bs.query_stock_industry, date=date_str)
        if rs.error_code != "0":
            logger.error(f"  获取失败: {rs.error_msg}")
            return 0

        results = []
        for _, row in _fetch_rs_to_df(rs).iterrows():
            code = str(row["code"])
            if _should_include(code):
                results.append({"code": code, "code_name": str(row.get("code_name", "")),
                                "industry": str(row.get("industry", "")),
                                "industryClassification": str(row.get("industryClassification", "")),
                                "date": date_str})

        self.db.save_stock_industry(results)
        self.progress.mark_step_done("2_stock_industry")
        logger.info(f"  保存 {len(results)} 条")
        return len(results)

    # ========== 汇总 ==========

    def _print_summary(self):
        from sqlalchemy import text
        logger.info("=" * 60)
        logger.info("数据汇总")
        logger.info("=" * 60)
        logger.info(f"  stock_list:     {self.db.get_stock_count():>10,}")
        logger.info(f"  basic_info:     {self.db.get_basic_count():>10,}")

        with self.db.engine.connect() as conn:
            for table, name in [("baostock_stock_industry", "industry")]:
                try:
                    r = conn.execute(text(f"SELECT count(*) FROM {table}")).fetchone()
                    logger.info(f"  {name:15s} {r[0]:>10,}")
                except Exception:
                    logger.info(f"  {name:15s} {'表不存在':>10s}")


# ==================== 入口 ====================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Baostock 股票基础数据采集器")
    parser.add_argument("--reset", action="store_true", help="重置进度和计数器")
    parser.add_argument("--reset-counter", action="store_true", help="重置计数器")
    parser.add_argument("--step", type=int, choices=[1, 2], help="只采集指定步骤 (1-2)")
    args = parser.parse_args()

    collector = BaostockCollector()

    if args.reset:
        collector.progress.reset()
        api_counter_reset()
        logger.info("进度和计数器已重置")
    elif args.reset_counter:
        api_counter_reset()
        logger.info("计数器已重置")

    if not collector.login():
        return

    try:
        if args.step in (None, 1):
            stocks = collector.step1_stock_list()
        else:
            stocks = collector.db.get_stock_list(limit=STOCK_LIST_LIMIT)

        if not stocks:
            logger.error("未获取到股票列表，退出")
            return

        if args.step in (None, 2):
            collector.step2_stock_industry()

        collector._print_summary()
        logger.info("=" * 60)
        api_stats_log(api_counter_get())
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"采集失败: {e}", exc_info=True)
        api_stats_log(api_counter_get())
    finally:
        collector.logout()
        logger.info(f"采集结束: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
