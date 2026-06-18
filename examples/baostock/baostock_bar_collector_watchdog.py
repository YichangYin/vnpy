"""
Baostock 行情数据采集器 — 自动重启版

启动采集器并监控，如果进程卡死（长时间无输出）或崩溃退出，自动重启。

使用方式:
    python baostock_bar_collector_watchdog.py

配置:
    - HEARTBEAT_TIMEOUT:  进程无输出超过此秒数视为卡死，默认 300 秒
    - RESTART_DELAY:      重启前等待秒数，默认 60 秒
    - MAX_RESTARTS:       最大重启次数，0=无限制，默认 0
"""

import subprocess
import time
import threading
import sys
import os
from datetime import datetime
import logging

# ==================== 配置 ====================

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
os.makedirs(LOG_DIR, exist_ok=True)

COLLECTOR_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "baostock_bar_collector.py")

HEARTBEAT_TIMEOUT = 300   # 卡死判定时间（秒），300 秒无输出即认为卡死
RESTART_DELAY = 60        # 重启前等待时间（秒）
MAX_RESTARTS = 0          # 最大重启次数，0=无限制

WATCHDOG_LOG = os.path.join(LOG_DIR, "watchdog.log")

# ==================== 日志 ====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(WATCHDOG_LOG, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ==================== 看门狗 ====================

class Watchdog:
    def __init__(self, script_path: str):
        self.script_path = script_path
        self.restart_count = 0
        self._alive = True
        self._heartbeat = time.time()  # 上次收到输出的时间戳
        self._process = None

    def _start_collector(self):
        """启动采集器进程"""
        logger.info(f"启动采集器: python {os.path.basename(self.script_path)}")
        logger.info(f"  卡死判定: {HEARTBEAT_TIMEOUT}s 无输出")
        logger.info(f"  重启延迟: {RESTART_DELAY}s")
        logger.info(f"  最大重启: {'无限制' if MAX_RESTARTS == 0 else MAX_RESTARTS}")
        logger.info("-" * 50)

        cmd = [sys.executable, self.script_path]
        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # 行缓冲
        )
        self._heartbeat = time.time()
        return self._process

    def _monitor_output(self):
        """监控子进程输出，更新心跳"""
        for line in iter(self._process.stdout.readline, ""):
            stripped = line.strip()
            if stripped:
                self._heartbeat = time.time()
                logger.info(f"  [{os.getpid()}] {stripped}")
        # 进程已结束
        self._process.stdout.close()

    def run(self):
        """主循环"""
        logger.info("=" * 60)
        logger.info("Baostock 行情采集器 — 自动重启看门狗")
        logger.info(f"采集脚本: {os.path.basename(self.script_path)}")
        logger.info(f"日志文件: {WATCHDOG_LOG}")
        logger.info("=" * 60)

        while self._alive:
            # 检查是否超过最大重启次数
            if MAX_RESTARTS > 0 and self.restart_count >= MAX_RESTARTS:
                logger.error(f"已达到最大重启次数 ({MAX_RESTARTS})，退出看门狗")
                break

            if self.restart_count > 0:
                logger.info(f"第 {self.restart_count} 次重启...")

            # 启动采集器
            proc = self._start_collector()

            # 在子线程中读取输出（更新心跳）
            monitor_thread = threading.Thread(target=self._monitor_output, daemon=True)
            monitor_thread.start()

            # 主循环：监控心跳
            while proc.poll() is None:
                time.sleep(10)  # 每 10 秒检查一次
                idle = time.time() - self._heartbeat

                if idle > HEARTBEAT_TIMEOUT:
                    logger.warning(f"⚠️ 检测到卡死！进程已 {idle:.0f}s 无输出，强制终止...")
                    self._kill_process(proc)
                    break

            # 进程退出，判断是否正常退出
            return_code = proc.returncode
            if return_code == 0:
                logger.info(f"采集器正常退出 (code={return_code})")
                logger.info("任务完成，看门狗退出")
                self._alive = False
            elif return_code is not None:
                logger.warning(f"采集器异常退出 (code={return_code})，准备重启")

            # 重启前等待
            if self._alive:
                self.restart_count += 1
                logger.info(f"等待 {RESTART_DELAY}s 后重启...")
                time.sleep(RESTART_DELAY)

        logger.info("=" * 60)
        logger.info(f"看门狗结束，总重启次数: {self.restart_count}")
        logger.info("=" * 60)

    def _kill_process(self, proc):
        """强制终止进程树（Windows 兼容）"""
        try:
            if os.name == "nt":
                subprocess.call(
                    f'taskkill /F /T /PID {proc.pid}',
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            else:
                import signal
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception as e:
            logger.debug(f"终止进程失败: {e}")
        proc.wait(timeout=10)

    def stop(self):
        """手动停止"""
        logger.info("收到停止信号")
        self._alive = False
        if self._process and self._process.poll() is None:
            self._kill_process(self._process)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Baostock 行情采集器 — 自动重启看门狗")
    parser.add_argument("--timeout", type=int, default=HEARTBEAT_TIMEOUT,
                        help=f"卡死判定时间（秒），默认 {HEARTBEAT_TIMEOUT}")
    parser.add_argument("--delay", type=int, default=RESTART_DELAY,
                        help=f"重启延迟（秒），默认 {RESTART_DELAY}")
    parser.add_argument("--max-restarts", type=int, default=MAX_RESTARTS,
                        help=f"最大重启次数，0=无限制，默认 {MAX_RESTARTS}")
    args = parser.parse_args()

    # 覆盖配置（模块级变量直接赋值即可）
    globals()["HEARTBEAT_TIMEOUT"] = args.timeout
    globals()["RESTART_DELAY"] = args.delay
    globals()["MAX_RESTARTS"] = args.max_restarts

    watchdog = Watchdog(COLLECTOR_SCRIPT)

    try:
        watchdog.run()
    except KeyboardInterrupt:
        logger.info("\n用户中断")
        watchdog.stop()


if __name__ == "__main__":
    main()
