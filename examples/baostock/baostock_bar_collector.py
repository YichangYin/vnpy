"""
Baostock 行情数据采集器 — 只采集 5分钟线/日线/周线/月线

支持断点续采：按股票代码排序，记录最新查询位置，下次从上次位置继续。
行情数据按频率分批入库（5分钟/日线 1000 条、周线 100 条、月线 10 条），日志记录入库进度。

使用方式:
    python baostock_bar_collector.py
    python baostock_bar_collector.py --reset        # 重置进度，从头采集
    python baostock_bar_collector.py --step daily   # 只采集日线

"""

import sys
import os
import logging
import time
import json
import calendar
import threading
from datetime import datetime, timedelta

import baostock as bs
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from vnpy.trader.object import BarData
from vnpy.trader.constant import Exchange, Interval

# Interval 没有 MONTHLY，月线用字符串 "monthly" 替代
MonthlyInterval = "monthly"

from baostock_database import BaostockDatabase

# ==================== 配置 ====================

DB_URL = os.environ.get(
    "BAOSTOCK_DB_URL",
    "postgresql://postgres:postgres@localhost:5432/baostock_vnpy"
)

# 日志和数据目录
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CSV_DIR = os.path.join(DATA_DIR, "csv")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

API_LOG_FILE = os.path.join(LOG_DIR, "api_call.log")
EMPTY_DATA_FILE = os.path.join(DATA_DIR, "baostock_bar_empty.json")
PROGRESS_FILE = os.path.join(DATA_DIR, "bar_collect_progress.json")
API_COUNTER_FILE = os.path.join(DATA_DIR, "api_request_counter.json")
REQUEST_DELAY = 0.5
API_TIMEOUT = 30      # 单接口超时秒数
MAX_RETRY = 10          # API 异常/报错重试次数上限
MAX_DAILY_REQUESTS = 45000  # 每天接口请求上限
STOCK_LIST_LIMIT = 5000         # 股票列表数量限制（0=不限制，调试用）

# 不同频率的入库批次大小（条数达到后触发入库）
BATCH_COMMIT = {
    "5": 1000,    # 5分钟线
    "d": 1000,    # 日线
    "w": 100,     # 周线
    "m": 10,      # 月线
}

# baostock API 锁
bs_lock = threading.Lock()

# ==================== 空数据 JSON 文件管理 ====================

_empty_data_lock = threading.Lock()


def _empty_data_backup():
    """每次采集前，将旧的空数据 JSON 备份到日期子目录中"""
    if not os.path.exists(EMPTY_DATA_FILE):
        return
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        file_date = datetime.fromtimestamp(os.path.getmtime(EMPTY_DATA_FILE)).strftime("%Y-%m-%d")
        if file_date == today:
            return  # 今天已备份过
        dest_dir = os.path.join(LOG_DIR, file_date)
        os.makedirs(dest_dir, exist_ok=True)
        dest = os.path.join(dest_dir, os.path.basename(EMPTY_DATA_FILE))
        if os.path.exists(dest):
            base, ext = os.path.splitext(os.path.basename(EMPTY_DATA_FILE))
            i = 1
            while os.path.exists(dest):
                dest = os.path.join(dest_dir, f"{base}_{i}{ext}")
                i += 1
        os.rename(EMPTY_DATA_FILE, dest)
    except Exception:
        pass  # 备份失败不影响主流程


def _empty_data_load() -> list:
    """加载空数据 JSON 文件，返回记录列表"""
    if not os.path.exists(EMPTY_DATA_FILE):
        return []
    try:
        with open(EMPTY_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def _empty_data_save(records: list):
    """保存空数据记录列表到 JSON 文件"""
    with open(EMPTY_DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)


def empty_data_add(frequency: str, start_date: str, end_date: str, stock_code: str):
    """记录一只空数据股票到 JSON 文件（线程安全）

    Args:
        frequency: 频率（"5"/"d"/"w"/"m"）
        start_date: 采集起始日期
        end_date: 采集结束日期
        stock_code: baostock 股票代码
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    record = {
        "code": [stock_code],
        "data_type": frequency,
        "period_start": start_date,
        "period_end": end_date,
        "query_time": now,
    }
    with _empty_data_lock:
        records = _empty_data_load()
        # 尝试合并到同频率+同周期的已有记录中
        for r in records:
            if r.get("data_type") == frequency and r.get("period_start") == start_date and r.get("period_end") == end_date:
                r["code"].append(stock_code)
                r["记录时间"] = now
                _empty_data_save(records)
                return
        records.append(record)
        _empty_data_save(records)


def setup_logging():
    """配置日志：先归档历史日志（在创建 FileHandler 之前），再创建新的日志文件"""
    # 归档必须在 FileHandler 创建之前执行，否则新打开的文件 mtime 会变成"今天"
    from baostock_database import _rotate_old_logs
    _rotate_old_logs()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(
                os.path.join(LOG_DIR, "baostock_bar_collector.log"),
                encoding="utf-8"
            ),
            logging.StreamHandler(),
        ],
    )

    # API 调用日志（独立文件，不传播到 root logger）
    api_logger = logging.getLogger("api_call")
    api_logger.setLevel(logging.DEBUG)
    api_logger.propagate = False
    api_logger.addHandler(logging.FileHandler(API_LOG_FILE, encoding="utf-8"))

    return logging.getLogger(__name__), api_logger


# 日志配置
logger, api_logger = setup_logging()

# ==================== API 请求计数器（持久化，按日统计） ====================

_counter_lock = threading.Lock()


def _counter_load() -> dict:
    """加载计数器文件"""
    today = datetime.now().strftime("%Y-%m-%d")
    if os.path.exists(API_COUNTER_FILE):
        try:
            with open(API_COUNTER_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            # 日期变更自动重置
            if data.get("date") != today:
                return {"date": today, "success": 0, "retry": 0, "timeout": 0,
                        "conn_reset": 0, "fail": 0, "total": 0}
            return data
        except Exception:
            pass
    return {"date": today, "success": 0, "retry": 0, "timeout": 0,
            "conn_reset": 0, "fail": 0, "total": 0}


def _counter_save(data: dict):
    """保存计数器到文件"""
    with open(API_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def api_counter_increment(key: str) -> int:
    """原子增加计数器指定字段，返回当前 total 值

    Args:
        key: 计数字段名，如 "success"、"retry"、"fail"、"timeout"、"conn_reset"

    Returns:
        当前总请求数 total
    """
    with _counter_lock:
        data = _counter_load()
        data[key] = data.get(key, 0) + 1
        if key in ("success", "retry", "fail"):
            data["total"] = data.get("total", 0) + 1
        _counter_save(data)
        return data["total"]


def api_counter_get() -> dict:
    """获取计数器当前快照（线程安全）

    Returns:
        计数器字典，包含 date、success、retry、timeout、conn_reset、fail、total
    """
    with _counter_lock:
        return _counter_load()


def api_counter_total() -> int:
    """获取当前总请求数，不修改计数器文件

    Returns:
        当日累计总请求次数
    """
    with _counter_lock:
        data = _counter_load()
        return data.get("total", 0)


def api_counter_check():
    """检查是否超过每日 API 请求上限，超限则抛出 SystemExit 退出程序

    Raises:
        SystemExit: 当总请求数 >= MAX_DAILY_REQUESTS 时退出
    """
    total = api_counter_total()
    if total >= MAX_DAILY_REQUESTS:
        raise SystemExit(
            f"今日 API 请求已达上限 {MAX_DAILY_REQUESTS} 次（当前 {total} 次），"
            f"程序退出。下次运行将自动重置计数。")


def api_counter_reset():
    """手动重置计数器为初始状态

    说明：计数器每天自动重置（日期变更时 _counter_load 会检测），
    一般不需要手动调用，除非需要中途清零重新计数。
    """
    today = datetime.now().strftime("%Y-%m-%d")
    with _counter_lock:
        _counter_save({"date": today, "success": 0, "retry": 0, "timeout": 0,
                       "conn_reset": 0, "fail": 0, "total": 0})


def api_stats_log(stats: dict):
    """打印 API 统计汇总到日志

    Args:
        stats: 计数器快照，包含 success、retry、timeout、conn_reset、fail、date 等字段
    """
    total = stats.get("total", stats["success"] + stats.get("retry", 0) + stats.get("fail", 0))
    logger.info(f"API 调用汇总（{stats['date']}）: "
               f"成功 {stats['success']} 次, "
               f"重试成功 {stats.get('retry', 0)} 次, "
               f"超时 {stats.get('timeout', 0)} 次, "
               f"连接断开 {stats.get('conn_reset', 0)} 次, "
               f"失败 {stats.get('fail', 0)} 次, "
               f"总请求 {total}/{MAX_DAILY_REQUESTS} 次")


# ==================== 工具函数 ====================


def _get_last_friday(d: datetime) -> datetime:
    """返回最近一个周五（如果 d 是周五则返回 d）"""
    weekday = d.weekday()  # Mon=0, Fri=4
    if weekday >= 5:  # 周六/周日 → 上周五
        return d - timedelta(days=weekday - 4)
    elif weekday == 4:  # 周五
        return d
    else:  # 周一至周四 → 上周五
        return d - timedelta(days=weekday + 3)


def _get_last_month_end(d: datetime) -> datetime:
    """返回最近一个月份的最后一天（如果 d 是月末则返回 d）"""
    _, last_day = calendar.monthrange(d.year, d.month)
    month_end = d.replace(day=last_day)
    if d.day == last_day:
        return d.replace(hour=0, minute=0, second=0, microsecond=0)
    if d > month_end.replace(hour=0, minute=0, second=0, microsecond=0):
        # 本月已过月末，返回上月月末
        if d.month == 1:
            return d.replace(year=d.year - 1, month=12, day=31,
                            hour=0, minute=0, second=0, microsecond=0)
        else:
            # 当月1号减1天 = 上月最后一天
            first = d.replace(month=d.month - 1, day=1,
                              hour=0, minute=0, second=0, microsecond=0)
            _, prev_last = calendar.monthrange(first.year, first.month)
            return first.replace(day=prev_last)
    # 还没到月末，返回上月月末
    if d.month == 1:
        return d.replace(year=d.year - 1, month=12, day=31,
                        hour=0, minute=0, second=0, microsecond=0)
    else:
        # 当月1号减1天 = 上月最后一天
        first = d.replace(month=d.month - 1, day=1,
                          hour=0, minute=0, second=0, microsecond=0)
        _, prev_last = calendar.monthrange(first.year, first.month)
        return first.replace(day=prev_last)


def _is_in_skip_window(frequency: str) -> bool:
    """判断当前是否处于周线/月线的跳过窗口

    周线跳过窗口: 周五 18:00 ~ 周一 09:00
    月线跳过窗口: 月末当天 18:00 ~ 次月 1 日 09:00
    """
    now = datetime.now()
    weekday = now.weekday()  # Mon=0, Sun=6

    if frequency == "w":
        # 周五 18:00 之后
        if weekday == 4 and now.hour >= 18:
            return True
        # 周六、周日全天
        if weekday >= 5:
            return True
        # 周一 09:00 之前
        if weekday == 0 and now.hour < 9:
            return True
        return False

    if frequency == "m":
        _, last_day = calendar.monthrange(now.year, now.month)
        # 月末当天 18:00 之后
        if now.day == last_day and now.hour >= 18:
            return True
        # 次月 1 日 09:00 之前
        if now.day == 1 and now.hour < 9:
            return True
        return False

    return False


def calculate_date_range(frequency: str, last_period_end: str = None,
                         last_period_start: str = None,
                         is_period_done: bool = False,
                         period_key: str = None) -> dict:
    """计算当前周期的采集日期范围和跳过状态

    逻辑:
    1. 首次采集 → 默认范围（5分钟30天，日线当年，周/月线当年）
    2. 上次已完成 → 新周期，start=上次结束日期(无缝衔接), end=今天（采集遗漏+新数据）
    3. 上次未完成 → 续采，start/end **都不变**，保持原始周期范围
    4. 周线/月线已完成且仍在同一周期内 → 跳过
    """
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")

    # 归一化：空字符串视为 None，避免 "" 被当作 truthy
    if not last_period_end:
        last_period_end = None
    if not last_period_start:
        last_period_start = None
    if not period_key:
        period_key = None

    if frequency == "5":
        has_new_day = last_period_end and last_period_end < today_str

        if is_period_done:
            # 上次已完成
            if has_new_day:
                # 跨新的一天 → 新周期
                new_start = last_period_end if last_period_end else (today - timedelta(days=30)).strftime("%Y-%m-%d")
                logger.info(f"[周期计算-5min] 新周期: start={new_start}, end={today_str}")
                return {
                    "start_date": new_start,
                    "end_date": today_str,
                    "skip": False,
                    "skip_reason": "",
                    "period_key": today_str,
                }
            else:
                # 同一天内已完成 → 跳过
                logger.info(f"[周期计算-5min] 跳过: 上次周期已完成且未跨天")
                return {
                    "start_date": today_str,
                    "end_date": today_str,
                    "skip": True,
                    "skip_reason": "5分钟线已完成且未跨天",
                    "period_key": period_key or today_str,
                }
        else:
            # 上次未完成 → 续采，保持原有周期范围
            if last_period_start:
                logger.info(f"[周期计算-5min] 续采: start={last_period_start}, end={last_period_end}")
                return {
                    "start_date": last_period_start,
                    "end_date": last_period_end,
                    "skip": False,
                    "skip_reason": "",
                    "period_key": period_key or today_str,
                }
            else:
                # 首次采集 → 最近30天
                logger.info(f"[周期计算-5min] 首次采集: start={(today - timedelta(days=30)).strftime('%Y-%m-%d')}, end={today_str}")
                return {
                    "start_date": (today - timedelta(days=30)).strftime("%Y-%m-%d"),
                    "end_date": today_str,
                    "skip": False,
                    "skip_reason": "",
                    "period_key": today_str,
                }

    if frequency == "d":
        has_new_day = last_period_end and last_period_end < today_str

        if is_period_done:
            # 上次已完成
            if has_new_day:
                # 跨新的一天 → 新周期
                new_start = last_period_end if last_period_end else today.strftime("%Y-01-01")
                logger.info(f"[周期计算-daily] 新周期: start={new_start}, end={today_str}")
                return {
                    "start_date": new_start,
                    "end_date": today_str,
                    "skip": False,
                    "skip_reason": "",
                    "period_key": today_str,
                }
            else:
                # 同一天内已完成 → 跳过
                logger.info(f"[周期计算-daily] 跳过: 上次周期已完成且未跨天")
                return {
                    "start_date": today_str,
                    "end_date": today_str,
                    "skip": True,
                    "skip_reason": "日线已完成且未跨天",
                    "period_key": period_key or today_str,
                }
        else:
            # 上次未完成 → 续采，保持原有周期范围
            if last_period_start:
                logger.info(f"[周期计算-daily] 续采: start={last_period_start}, end={last_period_end}")
                return {
                    "start_date": last_period_start,
                    "end_date": last_period_end,
                    "skip": False,
                    "skip_reason": "",
                    "period_key": period_key or today_str,
                }
            else:
                # 首次采集 → 当年1月1日
                logger.info(f"[周期计算-daily] 首次采集: start={today.strftime('%Y-01-01')}, end={today_str}")
                return {
                    "start_date": today.strftime("%Y-01-01"),
                    "end_date": today_str,
                    "skip": False,
                    "skip_reason": "",
                    "period_key": today_str,
                }

    if frequency == "w":
        last_friday = _get_last_friday(today).strftime("%Y-%m-%d")
        year_start = today.strftime("%Y-01-01")
        has_new_day = last_period_end and last_period_end < today_str

        if is_period_done:
            if period_key == last_friday:
                # 同一周，跳过
                return {
                    "start_date": today_str,
                    "end_date": today_str,
                    "skip": True,
                    "skip_reason": f"上次周期 {last_friday} 与当前周五相同，周数据已完成",
                    "period_key": last_friday,
                }
            # 新的一周 → end_date 设为上周五（本周数据尚未完整，不能采）
            new_start = last_period_end if last_period_end else year_start
            logger.info(f"[周期计算-weekly] 新周期: start={new_start}, end={last_friday}（上周完整数据）")
            return {
                "start_date": new_start,
                "end_date": last_friday,
                "skip": False,
                "skip_reason": "",
                "period_key": last_friday,
            }
        elif last_period_start:
            # 上次未完成 → 续采，保持原有周期范围
            logger.info(f"[周期计算-weekly] 续采: start={last_period_start}, end={last_period_end}")
            return {
                "start_date": last_period_start,
                "end_date": last_period_end,
                "skip": False,
                "skip_reason": "",
                "period_key": period_key or last_friday,
            }
        else:
            # 首次采集
            skip = _is_in_skip_window("w")
            return {
                "start_date": year_start,
                "end_date": today_str,
                "skip": skip,
                "skip_reason": "当前处于跳过窗口（周五 18:00 ~ 周一 09:00），周线无新数据" if skip else "",
                "period_key": last_friday,
            }

    if frequency == "m":
        last_month_end = _get_last_month_end(today).strftime("%Y-%m-%d")
        year_start = today.strftime("%Y-01-01")
        has_new_day = last_period_end and last_period_end < today_str

        if is_period_done:
            if period_key == last_month_end:
                # 同一月，跳过
                return {
                    "start_date": today_str,
                    "end_date": today_str,
                    "skip": True,
                    "skip_reason": f"上次周期 {last_month_end} 与当前月末相同，月数据已完成",
                    "period_key": last_month_end,
                }
            # 新的一月 → end_date 设为上月月末（本月数据尚未完整，不能采）
            new_start = last_period_end if last_period_end else year_start
            logger.info(f"[周期计算-monthly] 新周期: start={new_start}, end={last_month_end}（上月完整数据）")
            return {
                "start_date": new_start,
                "end_date": last_month_end,
                "skip": False,
                "skip_reason": "",
                "period_key": last_month_end,
            }
        elif last_period_start:
            # 上次未完成 → 续采
            logger.info(f"[周期计算-monthly] 续采: start={last_period_start}, end={last_period_end}")
            return {
                "start_date": last_period_start,
                "end_date": last_period_end,
                "skip": False,
                "skip_reason": "",
                "period_key": period_key or last_month_end,
            }
        else:
            # 首次采集
            skip = _is_in_skip_window("m")
            return {
                "start_date": year_start,
                "end_date": today_str,
                "skip": skip,
                "skip_reason": "当前处于跳过窗口（月末 18:00 ~ 次月 1 日 09:00），月线无新数据" if skip else "",
                "period_key": last_month_end,
            }

    # 默认 fallback
    return {
        "start_date": today_str,
        "end_date": today_str,
        "skip": False,
        "skip_reason": "",
        "period_key": today_str,
    }


def _should_include(code: str) -> bool:
    """过滤股票代码，只保留主板和创业板

    排除范围：
    - 科创板（sh.68 开头）
    - 北交所（sh.4/8/bj. 开头）

    Args:
        code: baostock 格式股票代码，如 sh.600000、sz.000001

    Returns:
        True 保留该股票，False 排除
    """
    if code.startswith("sh.68") or code.startswith(("sh.8", "sh.4", "bj.")):
        return False
    return code.startswith(("sh.60", "sz.00", "sz.30"))


def _bs_code_to_symbol(bs_code: str) -> str:
    """将 baostock 代码转为纯数字股票代码

    Args:
        bs_code: baostock 格式代码，如 sh.600000

    Returns:
        纯数字代码，如 600000
    """
    return bs_code.split(".")[1]


def _bs_code_to_exchange(bs_code: str) -> Exchange:
    """根据 baostock 代码前缀判断交易所

    Args:
        bs_code: baostock 格式代码，如 sh.600000 或 sz.000001

    Returns:
        Exchange.SSE（上交所）或 Exchange.SZSE（深交所）
    """
    return Exchange.SSE if bs_code.startswith("sh.") else Exchange.SZSE


def _fetch_rs_to_df(rs) -> pd.DataFrame:
    """将 baostock ResultSet 遍历为 pandas DataFrame

    逐行读取结果集直到 rs.next() 返回 False。如果 API 调用出错或无数据，返回空 DataFrame。

    Args:
        rs: baostock query 返回的 ResultSet 对象

    Returns:
        包含查询数据的 DataFrame，列名为 rs.fields
    """
    data_list = []
    while rs.error_code == "0" and rs.next():
        data_list.append(rs.get_row_data())
    if data_list:
        return pd.DataFrame(data_list, columns=rs.fields)
    return pd.DataFrame()


def _fetch_rs_to_df_with_count(rs) -> tuple:
    """将 baostock ResultSet 转为 DataFrame，同时返回数据条数

    Args:
        rs: baostock query 返回的 ResultSet 对象

    Returns:
        (DataFrame, 数据条数) 元组。无数据时返回 (空DataFrame, 0)
    """
    data_list = []
    while rs.error_code == "0" and rs.next():
        data_list.append(rs.get_row_data())
    count = len(data_list)
    if data_list:
        return pd.DataFrame(data_list, columns=rs.fields), count
    return pd.DataFrame(), 0


# ==================== API 调用封装 ====================

def _bs_query(func, *args, max_retries=5, **kwargs):
    """baostock API 调用统一封装：超时保护 + 自动重试 + 断线重连 + 结果日志

    在独立线程中执行 API 调用，通过 t.join(timeout) 控制超时时间。
    失败时自动重连（logout + login）后重试。

    Args:
        func: baostock API 函数对象，如 bs.query_history_k_data_plus
        *args: 传给 func 的位置参数
        max_retries: 最大重试次数（默认 5 次）
        **kwargs: 传给 func 的关键字参数

    Returns:
        (rs, elapsed_time) — rs 为 API 返回的 ResultSet，elapsed_time 为调用耗时（秒）

    Raises:
        SystemExit: 重试 max_retries 次后仍然失败，退出程序
    """
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
            return rs, elapsed

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


def _reconnect_and_continue(func_name: str, attempt: int, max_retries: int) -> bool:
    """执行 baostock 断线重连

    策略：连接断开时直接调用 bs.login() 重新登录，不先 logout()
    （避免在断开状态下 logout 污染 socket 状态）。

    不主动退出程序，所有失败情形均 return False，由外层 while 循环
    根据 max_retries 决定是否继续重试。

    Returns:
        True  重连成功
        False 重连失败（含超时、网络错误等）
    """
    try:
        api_logger.info(f"⚡ 正在重新登录 baostock（{func_name} 第 {attempt + 1} 次）...")
        logger.info(f"[API] 重新登录 baostock（第 {attempt + 1} 次）")
        with bs_lock:
            time.sleep(3*attempt)

            # 清理旧连接（断开状态下 logout 是安全的）
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


# ==================== 进度管理 ====================

class BarProgress:
    """记录每种周期的最新查询位置和采集周期

    JSON 结构:
    {
      "5": {
        "last_code": "sz.302132",
        "last_query_time": "2026-06-06 07:45:32",
        "progress": "3000/4920",             # 当前股票索引/总股票数
        "period_start": "2026-01-01",        # 本轮数据采集起始日期
        "period_end": "2026-06-06",          # 本轮数据采集结束日期
        "is_period_done": true,              # 本轮是否全部采集完
        "period_key": "2026-06-06",          # 周期标识（用于判断是否重复）
        "last_run": "2026-06-06 07:47:23"
      },
      "d": { ... },
      "w": { ... },
      "m": { ... }
    }
    """

    def __init__(self, filepath: str = PROGRESS_FILE):
        self.filepath = filepath
        self.data = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                # 兼容旧格式: 将平铺的 key 迁移为子对象
                return self._migrate(raw)
            except Exception:
                pass
        return {}

    @staticmethod
    def _migrate(raw: dict) -> dict:
        """兼容旧版平铺格式，迁移为子对象格式"""
        FREQS = ("5", "d", "w", "m")
        migrated = {}
        flat_keys_to_remove = set()

        for freq in FREQS:
            if isinstance(raw.get(freq), dict):
                # 已经是新格式
                migrated[freq] = raw[freq]
            else:
                # 旧格式或不存在，从平铺 key 组装
                obj = {}
                last_code_val = raw.get(freq)
                if last_code_val:
                    obj["last_code"] = last_code_val
                    flat_keys_to_remove.add(freq)
                for suffix in ("last_query_time", "progress", "last_run",
                               "period_start", "period_end", "is_period_done", "period_key"):
                    key = f"{freq}_{suffix}"
                    if key in raw:
                        obj[suffix] = raw[key]
                        flat_keys_to_remove.add(key)
                if obj:
                    migrated[freq] = obj

        # 合并未被迁移的其他 key
        for k, v in raw.items():
            if k not in flat_keys_to_remove and k not in migrated:
                migrated[k] = v

        return migrated

    def save(self):
        self.data["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 添加结构说明注释（首次写入时）
        if "_comment" not in self.data:
            self.data["_comment"] = {
                "description": "K线数据采集进度文件，按频率(frequency)分组记录",
                "frequency_keys": {
                    "5": "5分钟线", "d": "日线", "w": "周线", "m": "月线"
                },
                "fields": {
                    "last_code": "最后查询的baostock股票代码（如 sh.600000）",
                    "last_query_time": "最后一次API请求时间",
                    "progress": "当前进度（已处理股票数/总股票数）",
                    "period_start": "本轮采集起始日期",
                    "period_end": "本轮采集结束日期",
                    "is_period_done": "本轮是否全部采集完（true/false）",
                    "period_key": "周期标识，用于判断是否同一周期",
                    "last_run": "最后一次运行完成时间"
                }
            }
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def _ensure(self, frequency: str) -> dict:
        """确保频率子对象存在"""
        if frequency not in self.data:
            self.data[frequency] = {}
        return self.data[frequency]

    def get_last_code(self, frequency: str) -> str:
        """获取指定频率的最后查询股票代码

        Args:
            frequency: 频率标识，"5"=5分钟线、"d"=日线、"w"=周线、"m"=月线

        Returns:
            baostock 格式股票代码，如 "sh.600000"；首次采集返回空字符串
        """
        return self.data.get(frequency, {}).get("last_code", "")

    def set_last_code(self, frequency: str, code: str, progress: str = None):
        """记录指定频率的最后查询股票代码，同时保存请求时间和进度

        每次成功查询一只股票后调用，用于断点续采。

        Args:
            frequency: 频率标识（"5"/"d"/"w"/"m"）
            code: baostock 格式股票代码
            progress: 进度文字，如 "3000/4920"，可选
        """
        obj = self._ensure(frequency)
        obj["last_code"] = code
        obj["last_query_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if progress is not None:
            obj["progress"] = progress
        self.save()

    def get_last_time(self, frequency: str) -> str:
        """获取指定频率上次运行完成的时间戳

        Args:
            frequency: 频率标识（"5"/"d"/"w"/"m"）

        Returns:
            时间字符串，如 "2026-06-06 07:47:23"；无记录返回空字符串
        """
        return self.data.get(frequency, {}).get("last_run", "")

    def get_last_query_time(self, frequency: str) -> str:
        """获取指定频率最后一次 API 请求的时间戳

        与 get_last_time 不同：此方法返回的是最后一次发起请求的时间，
        而非整个频率采集完成的时间。

        Args:
            frequency: 频率标识（"5"/"d"/"w"/"m"）

        Returns:
            时间字符串，如 "2026-06-06 07:45:32"；无记录返回空字符串
        """
        return self.data.get(frequency, {}).get("last_query_time", "")

    def get_progress_text(self, frequency: str) -> str:
        """获取指定频率的进度文字

        Args:
            frequency: 频率标识（"5"/"d"/"w"/"m"）

        Returns:
            进度文字，如 "3000/4920"（已处理数/总数）；无记录返回空字符串
        """
        return self.data.get(frequency, {}).get("progress", "")

    def get_period_start(self, frequency: str) -> str:
        """获取指定频率本轮数据采集的起始日期

        Args:
            frequency: 频率标识（"5"/"d"/"w"/"m"）

        Returns:
            日期字符串，如 "2026-01-01"；无记录返回空字符串
        """
        return self.data.get(frequency, {}).get("period_start", "")

    def get_period_end(self, frequency: str) -> str:
        """获取指定频率本轮数据采集的结束日期

        Args:
            frequency: 频率标识（"5"/"d"/"w"/"m"）

        Returns:
            日期字符串，如 "2026-06-06"；无记录返回空字符串
        """
        return self.data.get(frequency, {}).get("period_end", "")

    def is_period_done(self, frequency: str) -> bool:
        """判断指定频率本轮采集是否全部完成

        Args:
            frequency: 频率标识（"5"/"d"/"w"/"m"）

        Returns:
            True 表示该频率本轮采集已全部完成，False 表示未完成或无记录
        """
        return self.data.get(frequency, {}).get("is_period_done", False)

    def get_period_key(self, frequency: str) -> str:
        """获取指定频率的周期标识

        用于判断是否同一周期，避免重复采集。
        例如日线的 period_key 是当天日期，月线的 period_key 是月末日期。

        Args:
            frequency: 频率标识（"5"/"d"/"w"/"m"）

        Returns:
            周期标识字符串；无记录返回空字符串
        """
        return self.data.get(frequency, {}).get("period_key", "")

    def set_last_run(self, frequency: str,
                     period_start: str = None, period_end: str = None,
                     is_period_done: bool = None, period_key: str = None):
        """记录指定频率本轮运行完成时间及周期信息

        每轮采集完成后调用，保存运行时间和周期状态，用于下一轮判断是否跳过。

        Args:
            frequency: 频率标识（"5"/"d"/"w"/"m"）
            period_start: 本轮采集起始日期，可选
            period_end: 本轮采集结束日期，可选
            is_period_done: 本轮是否全部完成，可选
            period_key: 周期标识，可选
        """
        obj = self._ensure(frequency)
        obj["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if period_start is not None:
            obj["period_start"] = period_start
        if period_end is not None:
            obj["period_end"] = period_end
        if is_period_done is not None:
            obj["is_period_done"] = is_period_done
        if period_key is not None:
            obj["period_key"] = period_key
        self.save()

    def set_period_info(self, frequency: str,
                        period_start: str = None, period_end: str = None,
                        is_period_done: bool = None, period_key: str = None):
        """仅保存周期信息，不修改 last_run 时间戳

        用于步骤开始时就写入周期上下文，避免覆盖原有的运行完成时间。

        Args:
            frequency: 频率标识（"5"/"d"/"w"/"m"）
            period_start: 本轮采集起始日期，可选
            period_end: 本轮采集结束日期，可选
            is_period_done: 本轮是否全部完成，可选
            period_key: 周期标识，可选
        """
        obj = self._ensure(frequency)
        if period_start is not None:
            obj["period_start"] = period_start
        if period_end is not None:
            obj["period_end"] = period_end
        if is_period_done is not None:
            obj["is_period_done"] = is_period_done
        if period_key is not None:
            obj["period_key"] = period_key
        self.save()

    def save_period_meta(self, frequency: str,
                        period_start: str = None, period_end: str = None,
                        period_key: str = None):
        """仅保存周期元数据，不修改 is_period_done

        用于跳过已完成步骤时更新周期范围，避免误判周期状态。

        Args:
            frequency: 频率标识（"5"/"d"/"w"/"m"）
            period_start: 本轮采集起始日期，可选
            period_end: 本轮采集结束日期，可选
            period_key: 周期标识，可选
        """
        obj = self._ensure(frequency)
        if period_start is not None:
            obj["period_start"] = period_start
        if period_end is not None:
            obj["period_end"] = period_end
        if period_key is not None:
            obj["period_key"] = period_key
        self.save()

    def is_all_done(self) -> bool:
        """判断所有频率是否都已完成采集（大周期完成标志）

        检查 5分钟线、日线、周线、月线四个频率是否全部标记为 is_period_done。

        Returns:
            True 表示全部完成，False 表示至少有一个频率未完成
        """
        FREQS = ("5", "d", "w", "m")
        return all(self.is_period_done(f) for f in FREQS)

    def reset(self, frequency: str = None):
        """重置采集进度

        Args:
            frequency: 指定要重置的频率（"5"/"d"/"w"/"m"），
                       不传则清空所有频率的进度
        """
        if frequency:
            self.data.pop(frequency, None)
        else:
            self.data.clear()
        self.save()


# ==================== 行情采集器 ====================

class BarCollector:
    """只采集 5min / daily / weekly / monthly K线"""

    # 频率到 step 名称的映射
    FREQ_TO_STEP = {
        "5": "5min",
        "d": "daily",
        "w": "weekly",
        "m": "monthly",
    }

    def __init__(self, db_url: str = DB_URL):
        self.db = BaostockDatabase(db_url)
        self.progress = BarProgress()
        self._logged_in = False

    # ---------- 登录/登出 ----------

    def login(self) -> bool:
        """登录 baostock 服务

        如果已经登录则直接返回 True，不重复登录。

        Returns:
            True 登录成功，False 登录失败
        """
        if self._logged_in:
            return True
        lg = bs.login()
        if lg.error_code != "0":
            logger.error(f"Baostock 登录失败: {lg.error_msg}")
            return False
        self._logged_in = True
        logger.info("Baostock 登录成功")
        return True

    def logout(self):
        """登出 baostock 服务

        如果当前未登录则不执行任何操作。
        """
        if self._logged_in:
            bs.logout()
            self._logged_in = False
            logger.info("Baostock 已登出")

    # ---------- 获取股票列表 ----------

    def get_stocks(self) -> list[dict]:
        """获取股票列表

        优先从数据库读取已有股票列表；如果数据库为空，则从 baostock API 获取并保存。
        股票列表会经过 _should_include 过滤，并按代码排序。

        Returns:
            股票列表，每项为 {"code": "sh.600000"} 格式的字典
        """
        stocks = self.db.get_stock_list(limit=STOCK_LIST_LIMIT)
        if stocks:
            logger.info(f"从数据库加载股票列表: {len(stocks)} 只")
        else:
            logger.info("数据库无股票列表，从 API 获取...")
            rs, _ = _bs_query(bs.query_stock_basic)
            if rs.error_code != "0":
                logger.error(f"  获取失败: {rs.error_msg}")
                return []
            stocks = []
            for _, row in _fetch_rs_to_df(rs).iterrows():
                code = str(row["code"])
                if _should_include(code):
                    stocks.append({"code": code})
            # API 获取时也排序+限制
            stocks = sorted(stocks, key=lambda x: x["code"])
            if STOCK_LIST_LIMIT > 0:
                stocks = stocks[:STOCK_LIST_LIMIT]
            self.db.save_stock_list(stocks)
            logger.info(f"从 API 获取并保存 {len(stocks)} 只股票")

        return stocks

    # ---------- 通用行情采集（支持断点续采 + 周期管理） ----------

    def _run_bar_step(self, stocks: list[dict], step_name: str,
                      frequency: str, save_fn,
                      bar_interval: Interval,
                      start_date: str = None, end_date: str = None,
                      batch_commit: int = None) -> tuple:
        """通用行情采集核心方法，支持断点续采 + 周期管理

        流程：
        1. 计算当前周期的采集日期范围（新周期/续采/跳过）
        2. 周线/月线已完成且仍在同一周期内则跳过
        3. 按股票代码排序，从上次断点位置继续采集
        4. 逐只股票调用 baostock API，解析为 BarData 对象
        5. 达到批次大小后触发入库，每只股票成功后保存进度
        6. 采集完成后更新周期状态

        Args:
            stocks: 股票列表，每项为 {"code": "sh.600000"}
            step_name: 步骤名称，用于日志输出（如 "5分钟线"、"日线"）
            frequency: baostock 频率参数（"5"/"d"/"w"/"m"）
            save_fn: 数据入库函数（如 self.db.save_bar_data）
            bar_interval: vnpy Interval 枚举（MINUTE/DAILY/WEEKLY）或 "monthly"
            start_date: 外部指定的起始日期，可选
            end_date: 外部指定的结束日期，可选
            batch_commit: 批次大小，达到后触发入库；不传则使用 BATCH_COMMIT 默认值

        Returns:
            (total_bars, success_count) — 入库数据条数、成功采集股票数
        """
        # ==================== 1. 计算周期日期范围 ====================
        last_period_end = self.progress.get_period_end(frequency)
        last_period_start = self.progress.get_period_start(frequency)
        period_done = self.progress.is_period_done(frequency)
        last_period_key = self.progress.get_period_key(frequency)
        period_info = calculate_date_range(frequency, last_period_end,
                                           last_period_start, period_done,
                                           last_period_key)

        # 如果外部指定了日期范围，优先使用
        if start_date:
            period_info["start_date"] = start_date
        if end_date:
            period_info["end_date"] = end_date

        # ==================== 2. 跳过判断（周线/月线） ====================
        if period_info["skip"] and period_done:
            logger.info("=" * 60)
            logger.info(f"[{step_name}] {period_info['skip_reason']}")
            logger.info(f"  上次周期: {period_info.get('period_key', 'N/A')}")
            logger.info("=" * 60)
            return 0, 0

        start_date = period_info["start_date"]
        end_date = period_info["end_date"]

        # 新周期开始 → 清空断点位置，从头开始采集
        period_end_advanced = (last_period_end and end_date > last_period_end)
        if period_done and period_end_advanced:
            logger.info(f"[{step_name}] 新周期: start={start_date}, end={end_date}")
            self.progress.set_last_code(frequency, "", progress="")

        # 跨天续采：上次未完成但 end_date 已推进到更新的时间
        # 不清除 last_code，续采从断点继续；旧 stock 的新日期数据由数据库 ON CONFLICT 去重处理
        if period_end_advanced and not period_done:
            resume_from = self.progress.get_last_code(frequency) or "第1只"
            logger.info(f"[{step_name}] 跨天续采: 周期范围延伸到 {end_date}，从断点 {resume_from} 继续")

        # 提前保存周期信息（即使中断也能保留周期上下文）
        self.progress.set_period_info(
            frequency,
            period_start=start_date,
            period_end=end_date,
            is_period_done=False,
            period_key=period_info.get("period_key", "")
        )

        # ==================== 3. 断点续采 ====================
        # 按代码排序
        to_fetch = sorted(stocks, key=lambda x: x["code"])

        # 找到上次最后查询的股票位置
        last_code = self.progress.get_last_code(frequency)
        last_time = self.progress.get_last_time(frequency)
        start_idx = 0
        if last_code:
            for i, s in enumerate(to_fetch):
                if s["code"] == last_code:
                    start_idx = i + 1  # 从下一只开始
                    break
                elif s["code"] > last_code:
                    start_idx = i
                    break

        # 判断是新周期还是续采
        is_resume = last_code and start_idx > 0 and start_idx < len(to_fetch)
        is_all_done = start_idx >= len(to_fetch)

        logger.info("=" * 60)
        logger.info(f"[{step_name}] 目标: {len(to_fetch)} 只, "
                   f"时间: {start_date} ~ {end_date}")

        if is_all_done:
            # last_code 为空但 start_idx >= len(to_fetch) 说明列表为空
            # last_code 非空且超出列表说明全部已采集完
            if last_code:
                logger.info(f"  上次已采集完全部 {len(to_fetch)} 只股票")
            else:
                logger.info(f"  股票列表为空，跳过")
            self.progress.set_last_run(frequency,
                                       period_start=start_date,
                                       period_end=end_date,
                                       is_period_done=True,
                                       period_key=period_info.get("period_key", ""))
            logger.info(f"[{step_name}] 全部已有数据，跳过")
            logger.info("=" * 60)
            return 0, 0

        # ==================== 4. 采集循环 ====================
        total_all_stocks = len(to_fetch)
        to_fetch = to_fetch[start_idx:]
        total_bars = 0
        success_count = 0
        failed_count = 0
        failed_stocks = []  # 记录失败股票名称，用于数据补偿
        batch_bars = []
        batch_count = batch_commit or BATCH_COMMIT.get(frequency, 1000)

        def flush_batch():
            nonlocal total_bars, batch_bars
            if batch_bars:
                bars_to_save = batch_bars.copy()
                batch_bars.clear()
                save_fn(bars_to_save)
                total_bars += len(bars_to_save)
                logger.debug(f"  入库 {len(bars_to_save)} 条, 累计 {total_bars} 条")

        def save_progress(code: str):
            """每只股票成功后立即保存进度（先 flush 在途数据保证一致性）"""
            flush_batch()
            global_idx = start_idx + success_count
            self.progress.set_last_code(
                frequency, code,
                progress=f"{global_idx}/{total_all_stocks}"
            )

        for stock in to_fetch:
            bs_code = stock["code"]
            symbol = _bs_code_to_symbol(bs_code)
            exchange = _bs_code_to_exchange(bs_code)

            # --- 重试循环：仅针对 API 报错和异常 ---
            retry = 0
            stock_ok = False  # 标记该股票是否处理完成（成功/空数据都算处理过）

            while retry <= MAX_RETRY:
                try:
                    rs, elapsed = _bs_query(bs.query_history_k_data_plus, bs_code,
                                   "date,open,high,low,close,volume,amount",
                                   start_date=start_date, end_date=end_date,
                                   frequency=frequency, adjustflag="2")

                    if rs.error_code != "0":
                        # API 报错：重试
                        retry += 1
                        if retry > MAX_RETRY:
                            logger.error(f"[{step_name}] {bs_code}: API 报错 {rs.error_code}，重试 {MAX_RETRY} 次后仍失败，退出程序")
                            sys.exit(1)
                        logger.warning(f"[{step_name}] {bs_code}: API 报错 {rs.error_code}，第 {retry} 次重试...")
                        time.sleep(REQUEST_DELAY * 2 * retry)
                        continue

                    df, row_count = _fetch_rs_to_df_with_count(rs)
                    if df.empty:
                        # 空数据：记录到 JSON 文件，不重试
                        empty_data_add(frequency, start_date, end_date, bs_code)
                        failed_count += 1
                        failed_stocks.append(bs_code)
                        stock_ok = True  # 处理完成（不重试）
                        break

                    api_logger.debug(f"数据  | {datetime.now().strftime('%H:%M:%S')} | {bs_code} | {frequency} | 耗时 {elapsed:.2f}s | 返回 {row_count} 条")

                    for _, row in df.iterrows():
                        try:
                            bar = BarData(
                                symbol=symbol, exchange=exchange,
                                datetime=pd.Timestamp(str(row["date"])).to_pydatetime(),
                                interval=bar_interval,
                                open_price=float(row["open"]), high_price=float(row["high"]),
                                low_price=float(row["low"]), close_price=float(row["close"]),
                                volume=float(row["volume"]), turnover=float(row["amount"]),
                                gateway_name="BAOSTOCK",
                            )
                            batch_bars.append(bar)
                            if len(batch_bars) >= batch_count:
                                flush_batch()
                        except (ValueError, KeyError) as e:
                            logger.debug(f"  解析失败 {bs_code}: {e}")

                    # 有数据返回 → 成功
                    stock_ok = True
                    break

                except Exception as e:
                    # 网络异常/超时等：重试
                    retry += 1
                    if retry > MAX_RETRY:
                        logger.error(f"[{step_name}] {bs_code}: 异常 {e}，重试 {MAX_RETRY} 次后仍失败，退出程序")
                        sys.exit(1)
                    logger.warning(f"[{step_name}] {bs_code}: 异常 {e}，第 {retry} 次重试...")
                    time.sleep(REQUEST_DELAY * 2)
                    continue

            if not stock_ok:
                # 不应该走到这里（sys.exit 会先拦截），但以防万一
                failed_count += 1
                failed_stocks.append(bs_code)
                continue

            # 成功或有数据（含空数据）→ 累计成功进度
            success_count += 1
            save_progress(bs_code)
            global_idx = start_idx + success_count
            if success_count % 20 == 0:
                logger.info(f"  进度: {bs_code} | 缓存 {len(batch_bars):>5} 条 | "
                           f"成功 {global_idx}/{total_all_stocks} 只 | "
                           f"总进度 {global_idx}/{total_all_stocks} | "
                           f"空数据 {failed_count} 只")
            time.sleep(REQUEST_DELAY)

        flush_batch()

        # 本轮采集完成，标记周期状态
        # 仅判断进度是否到 100%，不依赖空数据失败次数（空数据后续可根据 error 日志补充）
        is_this_period_done = (success_count >= total_all_stocks)
        self.progress.set_last_run(frequency,
                                   period_start=start_date,
                                   period_end=end_date,
                                   is_period_done=is_this_period_done,
                                   period_key=period_info.get("period_key", ""))

        logger.info(f"[{step_name}] 完成: {success_count}/{total_all_stocks} 只已处理, "
                   f"{total_bars} 条入库, {failed_count} 只空数据（详见 baostock_bar_empty.json）")
        if failed_stocks:
            logger.warning(f"[{step_name}] 空数据股票 ({len(failed_stocks)} 只): {', '.join(failed_stocks)}")
        logger.info("=" * 60)
        return total_bars, success_count

    # ---------- 四个行情步骤 ----------

    def step_5min(self, stocks: list[dict], start_date: str = None, end_date: str = None) -> tuple:
        """采集 5 分钟 K 线数据

        调用 baostock frequency='5'，默认采集最近 30 天数据。
        数据入库到 baostock_bar_5min 表。

        Args:
            stocks: 股票列表
            start_date: 起始日期，可选
            end_date: 结束日期，可选

        Returns:
            (入库数据条数, 成功采集股票数)
        """
        logger.info("=" * 60)
        logger.info("[1/4] 5 分钟线 → baostock_bar_5min")
        logger.info("=" * 60)
        return self._run_bar_step(
            stocks=stocks, step_name="5分钟线",
            frequency="5", save_fn=self.db.save_bar_5min,
            bar_interval=Interval.MINUTE,
            start_date=start_date, end_date=end_date,
            batch_commit=BATCH_COMMIT["5"]
        )

    def step_daily(self, stocks: list[dict], start_date: str = None, end_date: str = None) -> tuple:
        """采集日线 K 线数据

        调用 baostock frequency='d'，默认采集当年 1 月 1 日至今的数据。
        数据入库到 baostock_bar_data 表。

        Args:
            stocks: 股票列表
            start_date: 起始日期，可选
            end_date: 结束日期，可选

        Returns:
            (入库数据条数, 成功采集股票数)
        """
        logger.info("=" * 60)
        logger.info("[2/4] 日线 → baostock_bar_data")
        logger.info("=" * 60)
        return self._run_bar_step(
            stocks=stocks, step_name="日线",
            frequency="d", save_fn=self.db.save_bar_data,
            bar_interval=Interval.DAILY,
            start_date=start_date, end_date=end_date,
            batch_commit=BATCH_COMMIT["d"]
        )

    def step_weekly(self, stocks: list[dict], start_date: str = None, end_date: str = None) -> tuple:
        """采集周线 K 线数据

        调用 baostock frequency='w'，默认采集当年 1 月 1 日至今的数据。
        数据入库到 baostock_bar_weekly 表。

        Args:
            stocks: 股票列表
            start_date: 起始日期，可选
            end_date: 结束日期，可选

        Returns:
            (入库数据条数, 成功采集股票数)
        """
        logger.info("=" * 60)
        logger.info("[3/4] 周线 → baostock_bar_weekly")
        logger.info("=" * 60)
        return self._run_bar_step(
            stocks=stocks, step_name="周线",
            frequency="w", save_fn=self.db.save_bar_weekly,
            bar_interval=Interval.WEEKLY,
            start_date=start_date, end_date=end_date,
            batch_commit=BATCH_COMMIT["w"]
        )

    def step_monthly(self, stocks: list[dict], start_date: str = None, end_date: str = None) -> tuple:
        """采集月线 K 线数据

        调用 baostock frequency='m'，默认采集当年 1 月 1 日至今的数据。
        数据入库到 baostock_bar_monthly 表。

        Args:
            stocks: 股票列表
            start_date: 起始日期，可选
            end_date: 结束日期，可选

        Returns:
            (入库数据条数, 成功采集股票数)
        """
        logger.info("=" * 60)
        logger.info("[4/4] 月线 → baostock_bar_monthly")
        logger.info("=" * 60)
        return self._run_bar_step(
            stocks=stocks, step_name="月线",
            frequency="m", save_fn=self.db.save_bar_monthly,
            bar_interval=MonthlyInterval,
            start_date=start_date, end_date=end_date,
            batch_commit=BATCH_COMMIT["m"]
        )

    # ---------- 全量采集 ----------

    def collect(self):
        """全量行情采集入口

        按顺序采集 5min → daily → weekly → monthly。

        大周期逻辑：
        - 如果全部 4 个频率都已完成 → 重置所有进度，开始新的大周期
        - 否则 → 跳过已完成的步骤，从上次中断处继续
        - 新一天检测：部分频率的周期结束日期 < 今天时，清空断点开始新周期

        采集完成后打印各频率数据条数汇总和数据库验证信息。
        """
        if not self.login():
            return

        # 备份旧的空数据 JSON 文件（按日期归档）
        _empty_data_backup()

        try:
            stocks = self.get_stocks()
            if not stocks:
                logger.error("未获取到股票列表，退出")
                return

            # 以 calculate_date_range 为准判断周期状态，不提前重置进度
            if self.progress.is_all_done():
                logger.info("=" * 60)
                logger.info("大周期已完成，重置所有进度，开始新周期")
                logger.info("=" * 60)
                for f in ("5", "d", "w", "m"):
                    self.progress.set_last_run(f, is_period_done=False)

            # 串行采集：只运行未完成的步骤
            freq_map = [
                ("5", "5分钟线", self.step_5min, Interval.MINUTE),
                ("d", "日线", self.step_daily, Interval.DAILY),
                ("w", "周线", self.step_weekly, Interval.WEEKLY),
                ("m", "月线", self.step_monthly, MonthlyInterval),
            ]

            bars_count = {"5": 0, "d": 0, "w": 0, "m": 0}
            for freq, name, step_fn, interval in freq_map:
                if self.progress.is_period_done(freq):
                    logger.info(f"  [{name}] 已完成，跳过")
                    continue
                bars, _ = step_fn(stocks)
                bars_count[freq] = bars

            # 汇总
            logger.info("=" * 60)
            logger.info("行情采集完成汇总")
            logger.info("=" * 60)
            logger.info(f"  5分钟线:  {bars_count['5']:>10,} 条")
            logger.info(f"  日线:     {bars_count['d']:>10,} 条")
            logger.info(f"  周线:     {bars_count['w']:>10,} 条")
            logger.info(f"  月线:     {bars_count['m']:>10,} 条")
            logger.info(f"  总计:     {sum(bars_count.values()):>10,} 条")

            # 数据库验证
            overview = self.db.get_bar_overview()
            for iv in [Interval.MINUTE, Interval.DAILY, Interval.WEEKLY, MonthlyInterval]:
                count = sum(o.count for o in overview if o.interval == iv)
                stock_count = len(set(o.symbol for o in overview if o.interval == iv))
                iv_name = iv.value if hasattr(iv, 'value') else iv
                logger.info(f"  数据库 {iv_name}: {stock_count} 只股票, {count:,} 条")

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

    # ---------- 空数据补采 ----------

    def process_empty_data(self, date_str: str = None):
        """处理指定日期的空数据股票补采

        从指定日期的 `log/{date}/baostock_bar_empty.json` 读取空数据记录，
        按频率和周期分组后调用对应采集接口补采，进度记录到 `baostock_bar_empty_done.json`。

        Args:
            date_str: 日期字符串，格式 YYYY-MM-DD，不传则使用上次采集的日期
        """
        if not self.login():
            return

        _empty_data_backup()

        try:
            # 确定日期
            if not date_str:
                # 找最近的空数据 JSON 目录
                latest_date = None
                for d in sorted(os.listdir(LOG_DIR), reverse=True):
                    fpath = os.path.join(LOG_DIR, d, os.path.basename(EMPTY_DATA_FILE))
                    if os.path.exists(fpath):
                        latest_date = d
                        break
                if not latest_date:
                    logger.error("未找到空数据 JSON 文件，请传入日期参数，如: process_empty_data('2026-06-16')")
                    return
                date_str = latest_date

            src_dir = os.path.join(LOG_DIR, date_str)
            src_file = os.path.join(src_dir, os.path.basename(EMPTY_DATA_FILE))
            done_file = os.path.join(src_dir, "baostock_bar_empty_done.json")

            if not os.path.exists(src_file):
                logger.error(f"文件不存在: {src_file}")
                return

            # 加载空数据记录
            with open(src_file, "r", encoding="utf-8") as f:
                records = json.load(f)
            if not isinstance(records, list) or not records:
                logger.info(f"空数据文件为空: {src_file}")
                return

            # 加载已完成记录
            done_records = []
            if os.path.exists(done_file):
                try:
                    with open(done_file, "r", encoding="utf-8") as f:
                        done_records = json.load(f)
                    if not isinstance(done_records, list):
                        done_records = []
                except Exception:
                    done_records = []

            # 构建已完成记录的匹配集合 (data_type, period_start, period_end)
            done_keys = set()
            for dr in done_records:
                key = (dr.get("data_type"), dr.get("period_start"), dr.get("period_end"))
                done_keys.add(key)

            # 过滤未处理的记录
            todo = [r for r in records if (r.get("data_type"), r.get("period_start"), r.get("period_end")) not in done_keys]
            if not todo:
                logger.info(f"所有空数据已处理完毕: {src_file}")
                return

            logger.info("=" * 60)
            logger.info(f"空数据补采: {date_str}，共 {len(todo)} 条记录待处理")
            logger.info("=" * 60)

            # 按 (data_type, period_start, period_end) 分组
            groups = {}
            for r in todo:
                key = (r.get("data_type"), r.get("period_start"), r.get("period_end"))
                codes = r.get("code", [])
                if key not in groups:
                    groups[key] = []
                groups[key].extend(codes)

            # 频率映射
            freq_step = {"5": self.step_5min, "d": self.step_daily, "w": self.step_weekly, "m": self.step_monthly}

            stocks = self.get_stocks()
            if not stocks:
                logger.error("未获取到股票列表，退出")
                return

            all_stock_map = {s["code"]: s for s in stocks}

            for (data_type, p_start, p_end), codes in groups.items():
                if data_type not in freq_step:
                    logger.warning(f"未知频率: {data_type}，跳过")
                    continue

                # 从全量股票列表中筛选出对应的股票
                target_stocks = [all_stock_map[c] for c in codes if c in all_stock_map]
                if not target_stocks:
                    logger.warning(f"{data_type} [{p_start} ~ {p_end}]: {len(codes)} 只股票均不在列表中，跳过")
                    # 仍然标记为已完成
                    done_records.append({
                        "code": codes,
                        "data_type": data_type,
                        "period_start": p_start,
                        "period_end": p_end,
                        "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    with open(done_file, "w", encoding="utf-8") as f:
                        json.dump(done_records, f, indent=2, ensure_ascii=False)
                    continue

                logger.info(f"  补采 {data_type} [{p_start} ~ {p_end}]: {len(target_stocks)} 只股票")
                step_fn = freq_step[data_type]

                # 调用对应的采集步骤，只采集空数据的股票
                step_fn(target_stocks, start_date=p_start, end_date=p_end)

                # 标记为已完成
                done_records.append({
                    "code": codes,
                    "data_type": data_type,
                    "period_start": p_start,
                    "period_end": p_end,
                    "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
                # 立即保存进度
                with open(done_file, "w", encoding="utf-8") as f:
                    json.dump(done_records, f, indent=2, ensure_ascii=False)

            logger.info("=" * 60)
            logger.info(f"空数据补采完成: {date_str}，已处理 {len(todo)} 条记录")
            logger.info(f"进度文件: {done_file}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"空数据补采失败: {e}", exc_info=True)
        finally:
            self.logout()


# ==================== 入口 ====================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Baostock 行情数据采集器")
    parser.add_argument("--step", choices=["5min", "daily", "weekly", "monthly", "all"],
                        default="all", help="采集步骤: 5min/daily/weekly/monthly/all")
    parser.add_argument("--reset", action="store_true", help="重置进度文件和计数器，从头采集")
    parser.add_argument("--reset-step", choices=["5min", "daily", "weekly", "monthly"],
                        help="重置指定步骤的进度")
    parser.add_argument("--reset-counter", action="store_true", help="重置今日接口请求计数器")
    parser.add_argument("--empty", metavar="DATE", nargs="?", const=None,
                        help="空数据补采: 传入日期 YYYY-MM-DD，不传则自动找最近的空数据文件")
    args = parser.parse_args()

    collector = BarCollector()

    # 重置进度
    if args.reset:
        collector.progress.reset()
        api_counter_reset()
        logger.info("进度文件和计数器已重置")
    else:
        if args.reset_step:
            freq_map = {"5min": "5", "daily": "d", "weekly": "w", "monthly": "m"}
            collector.progress.reset(freq_map[args.reset_step])
            logger.info(f"已重置 {args.reset_step} 的进度")
        if args.reset_counter:
            api_counter_reset()
            logger.info("今日接口请求计数器已重置")

    if not collector.login():
        return

    # 打印当前计数器状态
    counter = api_counter_get()
    logger.info(f"今日已请求 {counter.get('total', 0)}/{MAX_DAILY_REQUESTS} 次")

    # 空数据补采模式
    if args.empty is not None:
        collector.logout()
        collector.process_empty_data(args.empty if args.empty else None)
        return

    try:
        stocks = collector.get_stocks()
        if not stocks:
            logger.error("未获取到股票列表，退出")
            return

        if args.step == "all":
            # 以 calculate_date_range 为准判断周期状态，不提前重置进度
            if collector.progress.is_all_done():
                logger.info("=" * 60)
                logger.info("大周期已完成，重置所有进度，开始新周期")
                logger.info("=" * 60)
                for f in ("5", "d", "w", "m"):
                    collector.progress.set_last_run(f, is_period_done=False)

            freq_map = [
                ("5", "5分钟线", collector.step_5min),
                ("d", "日线", collector.step_daily),
                ("w", "周线", collector.step_weekly),
                ("m", "月线", collector.step_monthly),
            ]
            for freq, name, step_fn in freq_map:
                if collector.progress.is_period_done(freq):
                    logger.info(f"  [{name}] 已完成，跳过")
                    continue
                step_fn(stocks)
        else:
            # 单步模式：按原逻辑运行指定步骤
            step_map = {
                "5min": collector.step_5min,
                "daily": collector.step_daily,
                "weekly": collector.step_weekly,
                "monthly": collector.step_monthly,
            }
            step_map[args.step](stocks)

        # 打印 API 统计汇总
        logger.info("=" * 60)
        api_stats_log(api_counter_get())
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"采集失败: {e}", exc_info=True)
        # 异常时也打印已发生的统计
        api_stats_log(api_counter_get())
    finally:
        collector.logout()


if __name__ == "__main__":
    main()
