"""
Baostock 财务数据采集器

  [步骤1] 利润表                — bs.query_profit_data(code, year, quarter)
  [步骤2] 成长性数据            — bs.query_growth_data(code, year, quarter)
  [步骤3] 资产负债表            — bs.query_balance_data(code, year, quarter)
  [步骤4] 现金流量表            — bs.query_cash_flow_data(code, year, quarter)
  [步骤5] 分红数据              — bs.query_dividend_data(code, year, yearType)
  [步骤6] 业绩快报              — bs.query_performance_express_report(code, year, quarter)

股票基础数据采集请使用 baostock_collector.py
"""

import sys
import os
import logging
import time
import json
import re
import threading
from datetime import datetime, timedelta

import baostock as bs
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from baostock_database import (
    BaostockDatabase,
    ProfitRecord, GrowthRecord, BalanceRecord, CashFlowRecord,
    DividendRecord, PerformanceRecord,
)

# ==================== 配置 ====================

DB_URL = os.environ.get(
    "BAOSTOCK_DB_URL",
    "postgresql://postgres:postgres@localhost:5432/baostock_vnpy"
)

# 日志和数据目录
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

PROGRESS_FILE = os.path.join(DATA_DIR, "financial_progress.json")
API_COUNTER_FILE = os.path.join(DATA_DIR, "api_request_counter.json")
API_LOG_FILE = os.path.join(LOG_DIR, "api_call.log")
REQUEST_DELAY = 0.05
BATCH_COMMIT = 10         # 财务数据每 10 条入库一次
API_TIMEOUT = 30      # 单接口超时秒数
MAX_DAILY_REQUESTS = 45000  # 每天接口请求上限
STOCK_LIST_LIMIT = 0         # 股票列表数量限制（0=不限制，调试用）

# baostock API 锁
bs_lock = threading.Lock()

# ==================== API 请求计数器（持久化，按日统计） ====================

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


def api_counter_total() -> int:
    with _counter_lock:
        data = _counter_load()
        return data.get("total", 0)


def api_counter_check():
    total = api_counter_total()
    if total >= MAX_DAILY_REQUESTS:
        raise SystemExit(
            f"今日 API 请求已达上限 {MAX_DAILY_REQUESTS} 次（当前 {total} 次），"
            f"程序退出。下次运行将自动重置计数。")


def api_counter_reset():
    today = datetime.now().strftime("%Y-%m-%d")
    with _counter_lock:
        _counter_save({"date": today, "success": 0, "retry": 0, "timeout": 0,
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
        logging.FileHandler(
            os.path.join(LOG_DIR, "baostock_financial_collector.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# API 调用日志（独立文件，不传播到 root logger）
api_logger = logging.getLogger("api_call")
api_logger.setLevel(logging.DEBUG)
api_logger.propagate = False
api_logger.addHandler(logging.FileHandler(API_LOG_FILE, encoding="utf-8"))


# ==================== 工具函数 ====================

def _sf(v):
    """安全转换 float"""
    s = str(v).strip()
    if not s or s == "":
        return None
    try:
        m = re.match(r'^([\d.]+)', s)
        if m:
            return float(m.group(1))
    except (ValueError, TypeError):
        pass
    return None


def _should_include(code: str) -> bool:
    """过滤科创板(68)、北交所(4/8/bj)，只保留主板/创业板"""
    if code.startswith("sh.68") or code.startswith(("sh.8", "sh.4", "bj.")):
        return False
    return code.startswith(("sh.60", "sz.00", "sz.30"))


def _classify_board(code: str) -> str:
    """根据代码判断板块：10主板/30创业板/40科创板/50北交所"""
    if code.startswith("sh.68"):
        return "40"
    if code.startswith("sz.30"):
        return "30"
    if code.startswith("bj."):
        return "50"
    return "10"


def _fetch_rs_to_df(rs) -> pd.DataFrame:
    """将 baostock ResultSet 转为 DataFrame"""
    data_list = []
    while rs.error_code == "0" and rs.next():
        data_list.append(rs.get_row_data())
    if data_list:
        return pd.DataFrame(data_list, columns=rs.fields)
    return pd.DataFrame()


# ==================== API 调用封装（超时+重连+重试） ====================

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

            t_login = threading.Thread(target=_login, daemon=True)
            t_login.start()
            t_login.join(timeout=30)
            if t_login.is_alive():
                api_logger.error("⚡ login 超时（30s），判定重连失败")
                logger.error("[API] login 超时（30s），判定重连失败")
                return False
            lg = login_result[0]

        if lg is not None and lg.error_code == "0":
            api_logger.info("✅ 重新登录成功")
            logger.info("[API] 重新登录成功")
        else:
            err_msg = getattr(lg, 'error_msg', '未知错误') if lg else 'login 返回空'
            api_logger.error(f"❌ 重新登录失败: {err_msg}")
            logger.error(f"[API] 重新登录失败: {err_msg}")
            return False
    except Exception as e:
        api_logger.error(f"❌ 重新登录异常: {e}")
        logger.error(f"[API] 重新登录异常: {e}")
        return False

    return True


def _bs_query(func, *args, max_retries=5, **kwargs):
    """baostock API 调用封装：超时保护 + 自动重试 + 断线重连 + 结果日志"""
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

        # ========== 超时 → 重连再试 ==========
        if t.is_alive():
            api_counter_increment("timeout")
            api_counter_check()
            api_logger.warning(f"超时 | {request_time} | {func_name} | {params} | 耗时 {elapsed:.2f}s (第 {attempt + 1} 次, {API_TIMEOUT}s无响应)")
            logger.warning(f"[API] 超时 {func_name} (第 {attempt + 1} 次)")
            _reconnect_and_continue(func_name, attempt, max_retries)
            attempt += 1
            api_logger.info(f"🔄 重连后重试 {func_name} (第 {attempt + 1} 次)...")
            logger.info(f"[API] 超时重连后重试 {func_name} (第 {attempt + 1} 次)")
            continue

        rs = result[0]

        # ========== 检测连接被强制关闭的错误 → 重连再试 ==========
        is_conn_reset = False
        if rs.error_code != "0":
            err_msg = getattr(rs, 'error_msg', '')
            conn_reset_keywords = ["远程主机强迫关闭", "10054", "接收数据异常",
                                  "网络接收错误", "10002007", "connection reset",
                                  "network error", "socket error"]
            is_conn_reset = any(kw.lower() in str(err_msg).lower() for kw in conn_reset_keywords)

        if is_conn_reset:
            api_counter_increment("conn_reset")
            api_counter_check()
            api_logger.warning(f"连接断开 | {request_time} | {func_name} | {params} | error={rs.error_code} {getattr(rs, 'error_msg', '')}")
            logger.warning(f"[API] 连接断开 {func_name}")
            _reconnect_and_continue(func_name, attempt, max_retries)
            attempt += 1
            api_logger.info(f"🔄 重连后重试 {func_name} (第 {attempt + 1} 次)...")
            logger.info(f"[API] 连接重连后重试 {func_name} (第 {attempt + 1} 次)")
            continue

        # ========== 请求成功 ==========
        if rs.error_code == "0":
            if attempt == 0:
                api_counter_increment("success")
                api_counter_check()
                api_logger.debug(f"成功  | {request_time} | {func_name} | {params} | 耗时 {elapsed:.2f}s")
            else:
                api_counter_increment("retry")
                api_counter_check()
                api_logger.info(f"重试成功 | {request_time} | {func_name} | {params} | 耗时 {elapsed:.2f}s (第 {attempt + 1} 次尝试)")
                logger.info(f"[API] 重试成功 {func_name} (第 {attempt + 1} 次)")
            return rs

        # ========== 普通错误 → 简单重试 ==========
        if attempt == 0:
            api_counter_increment("fail")
            api_counter_check()
            api_logger.warning(f"失败  | {request_time} | {func_name} | {params} | error={rs.error_code} {getattr(rs, 'error_msg', '')} | 耗时 {elapsed:.2f}s")
            logger.warning(f"[API] 失败 {func_name}: {getattr(rs, 'error_msg', '')}")
        attempt += 1
        if attempt < max_retries:
            time.sleep(1 * attempt)

    api_logger.error(f"❌ 重试失败 | {request_time} | {func_name} | {params} | 耗时 {elapsed:.2f}s (已重试 {max_retries} 次)")
    raise SystemExit(f"API 请求失败，已重试 {max_retries} 次仍失败: {func_name}")


# ==================== 进度管理 ====================

class Progress:
    """进度管理 — 财务数据步骤"""

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
                "1_profit": {"done": False, "idx": 0},
                "2_growth": {"done": False, "idx": 0},
                "3_balance": {"done": False, "idx": 0},
                "4_cash_flow": {"done": False, "idx": 0},
                "5_dividend": {"done": False, "idx": 0},
                "6_performance": {"done": False, "idx": 0},
            }
        }

    def save(self):
        self.data["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 添加结构说明注释（首次写入时）
        if "_comment" not in self.data:
            self.data["_comment"] = {
                "description": "财务数据采集进度文件，按步骤(step)分组记录",
                "step_keys": {
                    "1_profit": "利润表", "2_growth": "成长性数据", "3_balance": "资产负债表",
                    "4_cash_flow": "现金流量表", "5_dividend": "分红数据", "6_performance": "业绩快报"
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

    def set_step_idx(self, step_key: str, idx: int):
        self.data["steps"][step_key] = {"done": False, "idx": idx}
        self.save()

    def get_step_idx(self, step_key: str) -> int:
        return self.data["steps"].get(step_key, {}).get("idx", 0)

    def is_step_done(self, step_key: str) -> bool:
        return self.data["steps"].get(step_key, {}).get("done", False)

    def reset(self):
        for key in self.data["steps"]:
            self.data["steps"][key] = {"done": False, "idx": 0}
        self.save()


# ==================== 数据采集器 ====================

class FinancialCollector:
    def __init__(self, db_url: str = DB_URL):
        self.db = BaostockDatabase(db_url)
        self.progress = Progress()
        self._logged_in = False

    # ---------- 登录/登出 ----------

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

    # ---------- 获取股票列表 ----------

    def get_stocks(self) -> list[dict]:
        stocks = self.db.get_stock_list(limit=STOCK_LIST_LIMIT)
        if not stocks:
            logger.error("数据库无股票列表，请先运行 baostock_collector.py")
            return []
        logger.info(f"从数据库加载股票列表: {len(stocks)} 只")
        return stocks

    # ---------- 通用：财务步骤 ----------

    def _run_financial_step(self, stocks: list[dict], step_name: str,
                            record_model, query_fn, save_fn, progress_key: str) -> int:
        existing = self._get_existing_codes(record_model)
        to_fetch = sorted([s for s in stocks if s["code"] not in existing], key=lambda x: x["code"])

        start_idx = self.progress.get_step_idx(progress_key)
        total_original = len(to_fetch) + start_idx
        logger.info(f"[{step_name}] 已有: {len(existing)} 只, 待采: {len(to_fetch)} 只, 起始: {start_idx}")

        if not to_fetch or start_idx >= len(to_fetch):
            self.progress.mark_step_done(progress_key)
            logger.info(f"[{step_name}] 全部已有数据，跳过")
            return 0

        to_fetch = to_fetch[start_idx:]
        total_success = 0
        batch_results = []

        def flush_batch():
            nonlocal batch_results
            if batch_results:
                save_fn(batch_results)
                batch_results = []

        for i in range(0, len(to_fetch), 500):
            batch = to_fetch[i:i + 500]
            batch_success = 0

            for stock in batch:
                for retry in range(3):
                    try:
                        results = query_fn(stock)
                        if results:
                            batch_results.extend(results)
                            batch_success += 1
                        break
                    except Exception as e:
                        if retry < 2:
                            time.sleep(1 * (retry + 1))
                        else:
                            logger.debug(f"[{step_name}] {stock['code']} 失败: {e}")
                time.sleep(REQUEST_DELAY)

                # 数据入库成功后才记录进度
                if len(batch_results) >= BATCH_COMMIT:
                    flush_batch()
                    current_idx = start_idx + i
                    self.progress.set_step_idx(progress_key, current_idx)

            total_success += batch_success
            logger.info(f"[{step_name}] 进度: {total_success}/{total_original} 只成功 | "
                       f"缓存 {len(batch_results):>4} 条 | 累计 {total_success} 只")

        # 入库剩余数据
        flush_batch()
        self.progress.mark_step_done(progress_key)
        logger.info(f"[{step_name}] 完成: {total_success}/{len(to_fetch) + start_idx} 只成功")
        return total_success

    # ---------- 辅助 ----------

    def _get_existing_codes(self, record_model) -> set:
        from sqlalchemy import select
        from sqlalchemy.orm import Session
        with Session(self.db.engine) as session:
            rows = session.execute(select(record_model.code).distinct()).scalars().all()
        return set(rows)

    # ========== 步骤1: 利润表 ==========

    def step1_profit(self, stocks: list[dict]) -> int:
        """bs.query_profit_data(code, year, quarter)"""
        logger.info("=" * 60)
        logger.info("[步骤1/6] 利润表")
        logger.info("=" * 60)

        def query_fn(stock):
            results = []
            now = datetime.now()
            # 查询过去3年到去年（baostock 财报数据有滞后，当年数据通常次年才披露完整）
            for year in range(now.year - 3, now.year):
                for quarter in [1, 2, 3, 4]:
                    rs = _bs_query(bs.query_profit_data,
                                   code=stock["code"], year=str(year), quarter=quarter)
                    time.sleep(REQUEST_DELAY)
                    if rs.error_code != "0":
                        break
                    if not rs.next():
                        continue
                    rd = dict(zip(rs.fields, rs.get_row_data()))
                    results.append({
                        "code": stock["code"], "statDate": rd.get("statDate", f"{year}q{quarter}"),
                        "roeAvg": _sf(rd.get("roeAvg")), "npMargin": _sf(rd.get("npMargin")),
                        "gpMargin": _sf(rd.get("gpMargin")), "netProfit": _sf(rd.get("netProfit")),
                        "epsTTM": _sf(rd.get("epsTTM")), "MBRevenue": _sf(rd.get("MBRevenue")),
                        "totalShare": _sf(rd.get("totalShare")), "liqaShare": _sf(rd.get("liqaShare")),
                    })
            return results

        return self._run_financial_step(
            stocks=stocks, step_name="利润表",
            record_model=ProfitRecord,
            query_fn=query_fn, save_fn=self.db.save_profit,
            progress_key="1_profit"
        )

    # ========== 步骤2: 成长性数据 ==========

    def step2_growth(self, stocks: list[dict]) -> int:
        """bs.query_growth_data(code, year, quarter)"""
        logger.info("=" * 60)
        logger.info("[步骤2/6] 成长性数据")
        logger.info("=" * 60)

        def query_fn(stock):
            results = []
            now = datetime.now()
            for year in range(now.year - 3, now.year):
                for quarter in [1, 2, 3, 4]:
                    rs = _bs_query(bs.query_growth_data,
                                   code=stock["code"], year=str(year), quarter=quarter)
                    time.sleep(REQUEST_DELAY)
                    if rs.error_code != "0":
                        break
                    if not rs.next():
                        continue
                    rd = dict(zip(rs.fields, rs.get_row_data()))
                    results.append({
                        "code": stock["code"], "statDate": rd.get("statDate", f"{year}q{quarter}"),
                        "YOYEquity": _sf(rd.get("YOYEquity")), "YOYAsset": _sf(rd.get("YOYAsset")),
                        "YOYNI": _sf(rd.get("YOYNI")), "YOYEPSBasic": _sf(rd.get("YOYEPSBasic")),
                        "YOYPNI": _sf(rd.get("YOYPNI")),
                    })
            return results

        return self._run_financial_step(
            stocks=stocks, step_name="成长性",
            record_model=GrowthRecord,
            query_fn=query_fn, save_fn=self.db.save_growth,
            progress_key="2_growth"
        )

    # ========== 步骤3: 资产负债表 ==========

    def step3_balance(self, stocks: list[dict]) -> int:
        """bs.query_balance_data(code, year, quarter)"""
        logger.info("=" * 60)
        logger.info("[步骤3/6] 资产负债表")
        logger.info("=" * 60)

        def query_fn(stock):
            results = []
            now = datetime.now()
            for year in range(now.year - 3, now.year):
                for quarter in [1, 2, 3, 4]:
                    rs = _bs_query(bs.query_balance_data,
                                   code=stock["code"], year=str(year), quarter=quarter)
                    time.sleep(REQUEST_DELAY)
                    if rs.error_code != "0":
                        break
                    if not rs.next():
                        continue
                    rd = dict(zip(rs.fields, rs.get_row_data()))
                    results.append({
                        "code": stock["code"], "statDate": rd.get("statDate", f"{year}q{quarter}"),
                        "currentRatio": _sf(rd.get("currentRatio")), "quickRatio": _sf(rd.get("quickRatio")),
                        "cashRatio": _sf(rd.get("cashRatio")), "YOYLiability": _sf(rd.get("YOYLiability")),
                        "liabilityToAsset": _sf(rd.get("liabilityToAsset")), "assetToEquity": _sf(rd.get("assetToEquity")),
                    })
            return results

        return self._run_financial_step(
            stocks=stocks, step_name="资产负债表",
            record_model=BalanceRecord,
            query_fn=query_fn, save_fn=self.db.save_balance,
            progress_key="3_balance"
        )

    # ========== 步骤4: 现金流量表 ==========

    def step4_cash_flow(self, stocks: list[dict]) -> int:
        """bs.query_cash_flow_data(code, year, quarter)"""
        logger.info("=" * 60)
        logger.info("[步骤4/6] 现金流量表")
        logger.info("=" * 60)

        def query_fn(stock):
            results = []
            now = datetime.now()
            for year in range(now.year - 3, now.year):
                for quarter in [1, 2, 3, 4]:
                    rs = _bs_query(bs.query_cash_flow_data,
                                   code=stock["code"], year=str(year), quarter=quarter)
                    time.sleep(REQUEST_DELAY)
                    if rs.error_code != "0":
                        break
                    if not rs.next():
                        continue
                    rd = dict(zip(rs.fields, rs.get_row_data()))
                    results.append({
                        "code": stock["code"], "statDate": rd.get("statDate", f"{year}q{quarter}"),
                        "CAToAsset": _sf(rd.get("CAToAsset")), "NCAToAsset": _sf(rd.get("NCAToAsset")),
                        "tangibleAssetToAsset": _sf(rd.get("tangibleAssetToAsset")),
                        "ebitToInterest": _sf(rd.get("ebitToInterest")),
                        "CFOToOR": _sf(rd.get("CFOToOR")), "CFOToNP": _sf(rd.get("CFOToNP")),
                        "CFOToGr": _sf(rd.get("CFOToGr")),
                    })
            return results

        return self._run_financial_step(
            stocks=stocks, step_name="现金流量表",
            record_model=CashFlowRecord,
            query_fn=query_fn, save_fn=self.db.save_cash_flow,
            progress_key="4_cash_flow"
        )

    # ========== 步骤5: 分红数据 ==========

    def step5_dividend(self, stocks: list[dict]) -> int:
        """bs.query_dividend_data(code, year, yearType)

        yearType: 1=年报, 2=半年报, 3=一季报, 4=三季报
        默认采集最近2年的年报
        """
        logger.info("=" * 60)
        logger.info("[步骤5/6] 分红数据")
        logger.info("=" * 60)

        def query_fn(stock):
            results = []
            now = datetime.now()
            # 采集过去3年的年报数据（yearType: 1=年报）
            for year in range(now.year - 3, now.year):
                rs = _bs_query(bs.query_dividend_data,
                               code=stock["code"], year=str(year), yearType="1")
                time.sleep(REQUEST_DELAY)
                if rs.error_code != "0":
                    break
                df = _fetch_rs_to_df(rs)
                for _, row in df.iterrows():
                    results.append({
                        "code": stock["code"],
                        "dividPlanDate": str(row.get("dividPlanDate", "")),
                        "dividRegistDate": str(row.get("dividRegistDate", "")),
                        "dividOperateDate": str(row.get("dividOperateDate", "")),
                        "dividPayDate": str(row.get("dividPayDate", "")),
                        "dividStockMarketDate": str(row.get("dividStockMarketDate", "")),
                        "dividCashPsBeforeTax": _sf(row.get("dividCashPsBeforeTax")),
                        "dividCashPsAfterTax": _sf(row.get("dividCashPsAfterTax")),
                        "dividStocksPs": _sf(row.get("dividStocksPs")),
                        "dividCashStock": _sf(row.get("dividCashStock")),
                        "dividReserveToStockPs": _sf(row.get("dividReserveToStockPs")),
                    })
            return results

        return self._run_financial_step(
            stocks=stocks, step_name="分红数据",
            record_model=DividendRecord,
            query_fn=query_fn, save_fn=self.db.save_dividend,
            progress_key="5_dividend"
        )

    # ========== 步骤6: 业绩快报 ==========

    def step6_performance(self, stocks: list[dict]) -> int:
        """bs.query_performance_express_report(code, year, quarter)"""
        logger.info("=" * 60)
        logger.info("[步骤6/6] 业绩快报")
        logger.info("=" * 60)

        def query_fn(stock):
            results = []
            now = datetime.now()
            for year in range(now.year - 3, now.year):
                for quarter in [1, 2, 3, 4]:
                    rs = _bs_query(bs.query_performance_express_report,
                                   code=stock["code"], year=str(year), quarter=quarter)
                    time.sleep(REQUEST_DELAY)
                    if rs.error_code != "0":
                        break
                    if not rs.next():
                        continue
                    rd = dict(zip(rs.fields, rs.get_row_data()))
                    results.append({
                        "code": stock["code"],
                        "performanceExpPubDate": str(rd.get("performanceExpPubDate", "")),
                        "performanceExpStatDate": str(rd.get("performanceExpStatDate", "")),
                        "performanceExpressROEWa": _sf(rd.get("performanceExpressROEWa")),
                        "performanceExpressEPS": _sf(rd.get("performanceExpressEPS")),
                        "totalShare": _sf(rd.get("totalShare")),
                        "totalAssets": _sf(rd.get("totalAssets")),
                        "totalLiab": _sf(rd.get("totalLiab")),
                        "totalEquity": _sf(rd.get("totalEquity")),
                        "BPS": _sf(rd.get("BPS")),
                        "netProfitYOY": _sf(rd.get("netProfitYOY")),
                        "netProfit": _sf(rd.get("netProfit")),
                        "performanceExpressPubDate": str(rd.get("performanceExpressPubDate", "")),
                    })
            return results

        return self._run_financial_step(
            stocks=stocks, step_name="业绩快报",
            record_model=PerformanceRecord,
            query_fn=query_fn, save_fn=self.db.save_performance,
            progress_key="6_performance"
        )

    # ========== 全量采集 ==========

    def collect_all(self, reset: bool = False):
        if reset:
            self.progress.reset()
            api_counter_reset()
            logger.info("进度文件和计数器已重置")

        if not self.login():
            return

        try:
            stocks = self.get_stocks()
            if not stocks:
                logger.error("未获取到股票列表，退出")
                return

            financial_steps = [
                ("1_profit", self.step1_profit, "利润表"),
                ("2_growth", self.step2_growth, "成长性"),
                ("3_balance", self.step3_balance, "资产负债表"),
                ("4_cash_flow", self.step4_cash_flow, "现金流量表"),
                ("5_dividend", self.step5_dividend, "分红数据"),
                ("6_performance", self.step6_performance, "业绩快报"),
            ]

            for step_key, func, name in financial_steps:
                if not self.progress.is_step_done(step_key):
                    func(stocks)
                else:
                    logger.info(f"  [跳过] {name}（已完成）")

            self._print_summary()

            # API 调用统计
            logger.info("=" * 60)
            api_stats_log(api_counter_get())
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"采集失败: {e}", exc_info=True)
            api_stats_log(api_counter_get())
        finally:
            self.logout()
            logger.info(f"采集结束: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # ========== 汇总 ==========

    def _print_summary(self):
        from sqlalchemy import text
        logger.info("=" * 60)
        logger.info("数据汇总")
        logger.info("=" * 60)

        with self.db.engine.connect() as conn:
            for table, name in [
                ("baostock_profit", "profit"),
                ("baostock_growth", "growth"),
                ("baostock_balance", "balance"),
                ("baostock_cash_flow", "cash_flow"),
                ("baostock_dividend", "dividend"),
                ("baostock_performance", "performance"),
            ]:
                try:
                    r = conn.execute(text(f"SELECT count(*) FROM {table}")).fetchone()
                    logger.info(f"  {name:15s} {r[0]:>10,}")
                except Exception:
                    logger.info(f"  {name:15s} {'表不存在':>10s}")

        stats = self.db.get_call_stats()
        if stats:
            for s in stats:
                logger.info(f"  API({s['interface_name']}) {s['call_count']:>10,}")
        else:
            logger.info(f"  API(db):              0")


# ==================== 入口 ====================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Baostock 财务数据采集器")
    parser.add_argument("--reset", action="store_true", help="重置进度文件和计数器，从头采集")
    parser.add_argument("--reset-counter", action="store_true", help="重置今日接口请求计数器")
    parser.add_argument("--step", type=int, choices=range(1, 7),
                        help="只采集指定步骤 (1-6)")
    args = parser.parse_args()

    collector = FinancialCollector()

    if args.reset:
        collector.progress.reset()
        api_counter_reset()
        logger.info("进度文件和计数器已重置")
    elif args.reset_counter:
        api_counter_reset()
        logger.info("今日接口请求计数器已重置")

    if not collector.login():
        return

    try:
        stocks = collector.get_stocks()
        if not stocks:
            logger.error("未获取到股票列表，退出")
            return

        steps = [
            (1, collector.step1_profit, "利润表"),
            (2, collector.step2_growth, "成长性"),
            (3, collector.step3_balance, "资产负债表"),
            (4, collector.step4_cash_flow, "现金流量表"),
            (5, collector.step5_dividend, "分红数据"),
            (6, collector.step6_performance, "业绩快报"),
        ]

        for step_num, func, name in steps:
            if args.step is None or args.step == step_num:
                func(stocks)
            elif args.step is not None:
                logger.info(f"  [跳过] {name}（未指定步骤 {step_num}）")

        collector._print_summary()

        # API 调用统计
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
