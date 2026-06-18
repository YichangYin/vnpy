"""
APScheduler 定时任务调度器 — Baostock 版

调度计划:
| 组件         | 频率        | 说明                    |
|-------------|------------|------------------------|
| 数据采集     | 每日15:30  | 收盘后采集全天日线数据    |
| 信号生成     | 每日16:00  | 多因子+技术分析生成交易信号 |

使用方式:
    python scheduler.py

前置要求:
    pip install apscheduler baostock sqlalchemy
"""

import os
import logging
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

# 日志目录
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
os.makedirs(LOG_DIR, exist_ok=True)

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "scheduler.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def collect_data():
    logger.info("⏰ 触发数据采集任务")
    try:
        from baostock_collector import collect_all_stocks
        collect_all_stocks()
        logger.info("✅ 数据采集完成")
    except Exception as e:
        logger.error(f"数据采集失败: {e}", exc_info=True)


def generate_signals():
    logger.info("⏰ 触发信号生成任务")
    try:
        from signal_generator import generate_daily_signals
        generate_daily_signals()
        logger.info("✅ 信号生成完成")
    except Exception as e:
        logger.error(f"信号生成失败: {e}", exc_info=True)


def health_check():
    try:
        from baostock_database import BaostockDatabase
        db = BaostockDatabase("postgresql://postgres:postgres@localhost:5432/baostock_vnpy")
        overview = db.get_bar_overview()
        logger.info(f"💚 健康检查通过: 数据库中有 {len(overview)} 个品种")
    except Exception as e:
        logger.error(f"健康检查失败: {e}")


def main():
    logger.info("=" * 50)
    logger.info("Baostock + vn.py 定时任务调度器启动")
    logger.info("=" * 50)

    executors = {"default": ThreadPoolExecutor(max_workers=4)}
    job_defaults = {"coalesce": True, "max_instances": 1}

    scheduler = BlockingScheduler(
        executors=executors,
        job_defaults=job_defaults,
        timezone="Asia/Shanghai",
    )

    # 每日 15:30 — 数据采集
    scheduler.add_job(collect_data, "cron", hour=15, minute=30,
                      day_of_week="mon-fri", id="collect_data", name="采集全市场日线数据")

    # 每日 16:00 — 信号生成
    scheduler.add_job(generate_signals, "cron", hour=16, minute=0,
                      day_of_week="mon-fri", id="generate_signals", name="生成交易信号")

    # 每日 08:00 — 健康检查
    scheduler.add_job(health_check, "cron", hour=8, minute=0,
                      day_of_week="mon-fri", id="health_check", name="数据库健康检查")

    logger.info("启动时执行一次完整流程...")
    collect_data()
    generate_signals()
    health_check()

    logger.info("\n已注册的任务:")
    for job in scheduler.get_jobs():
        logger.info(f"  - {job.name} : {job.trigger}")

    logger.info("\n调度器运行中... (Ctrl+C 停止)")
    logger.info("=" * 50)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("调度器已停止")


if __name__ == "__main__":
    main()
