"""
数据采集日志管理器 — 将 API 请求结果写入日志文件，支持断点续传

使用方式:
    # 写入日志
    logger = DataLogger("data_log.jsonl", "collect_progress.json")
    logger.start_step("9_daily")
    for stock in stocks:
        if logger.should_skip(stock["code"]):
            continue
        data = fetch_api(stock["code"])
        logger.write(stock["code"], data)
    logger.end_step("9_daily")

    # 从日志入库
    db = BaostockDatabase()
    DataLogger.load_to_db("data_log.jsonl", db, save_fn=db.save_bar_data)
"""

import json
import os
import time
from datetime import datetime


class DataLogger:
    """数据采集日志管理器 — JSON Lines 格式，支持断点续传"""

    def __init__(self, log_file: str, progress_file: str = None):
        self.log_file = log_file
        self.progress_file = progress_file or "collect_progress.json"
        self._file = None
        self._current_step = None
        self._processed_codes = set()
        self._load_progress()

    def _load_progress(self):
        """从进度文件加载已处理的股票代码"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # 加载日志文件中已记录的代码
                if os.path.exists(self.log_file):
                    with open(self.log_file, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                try:
                                    record = json.loads(line)
                                    self._processed_codes.add(record.get("code", ""))
                                except json.JSONDecodeError:
                                    pass
            except Exception:
                pass

    def start_step(self, step_name: str):
        """开始一个采集步骤"""
        self._current_step = step_name
        print(f"[{step_name}] 已加载 {len(self._processed_codes)} 条已处理记录")

    def end_step(self, step_name: str):
        """结束一个采集步骤"""
        self._current_step = None

    def should_skip(self, code: str) -> bool:
        """检查是否已处理过该股票"""
        return code in self._processed_codes

    def write(self, code: str, data: any, metadata: dict = None):
        """将采集结果写入日志文件"""
        record = {
            "code": code,
            "timestamp": datetime.now().isoformat(),
            "data": data,
        }
        if metadata:
            record["metadata"] = metadata

        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

        self._processed_codes.add(code)
        self._save_progress()

    def _save_progress(self):
        """保存进度到文件"""
        progress = {
            "last_update": datetime.now().isoformat(),
            "log_file": self.log_file,
            "processed_count": len(self._processed_codes),
            "processed_codes": list(self._processed_codes),
        }
        with open(self.progress_file, "w", encoding="utf-8") as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)

    @staticmethod
    def load_to_db(log_file: str, save_fn, batch_size: int = 500):
        """从日志文件读取数据并入库

        Args:
            log_file: 日志文件路径
            save_fn: 保存函数，接收 (stock_code, data) 参数
            batch_size: 批量处理大小
        """
        if not os.path.exists(log_file):
            print(f"日志文件不存在: {log_file}")
            return

        total = 0
        processed = 0
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    code = record.get("code", "")
                    data = record.get("data", {})
                    save_fn(code, data)
                    processed += 1
                    total += 1

                    if total % batch_size == 0:
                        print(f"  已处理 {total} 条")
                except Exception as e:
                    print(f"  处理失败: {line[:100]}... {e}")

        print(f"完成: 共处理 {total} 条记录")

    @staticmethod
    def count_records(log_file: str) -> int:
        """统计日志文件中的记录数"""
        if not os.path.exists(log_file):
            return 0
        count = 0
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    count += 1
        return count


# ==================== 使用示例 ====================

if __name__ == "__main__":
    # 示例：如何使用 DataLogger
    print("=== DataLogger 使用示例 ===")

    # 1. 创建日志管理器
    logger = DataLogger("test_data_log.jsonl", "test_progress.json")

    # 2. 开始采集步骤
    logger.start_step("9_daily")

    # 3. 模拟采集数据
    test_stocks = [
        {"code": "sh.600000", "name": "浦发银行"},
        {"code": "sh.600001", "name": "测试股票"},
    ]

    for stock in test_stocks:
        code = stock["code"]
        if logger.should_skip(code):
            print(f"跳过已处理: {code}")
            continue

        # 模拟 API 调用
        fake_data = {
            "dates": ["2026-01-01", "2026-01-02"],
            "opens": [10.5, 10.6],
            "closes": [10.8, 10.9],
        }

        # 写入日志
        logger.write(code, fake_data, metadata={"name": stock["name"]})
        print(f"已记录: {code}")

    logger.end_step("9_daily")

    # 4. 查看统计
    print(f"\n日志文件记录数: {DataLogger.count_records('test_data_log.jsonl')}")

    # 5. 从日志入库（示例）
    def mock_save_fn(code, data):
        print(f"  入库: {code} -> {len(data.get('dates', []))} 条数据")

    print("\n=== 从日志入库 ===")
    DataLogger.load_to_db("test_data_log.jsonl", mock_save_fn)

    # 清理测试文件
    for f in ["test_data_log.jsonl", "test_progress.json"]:
        if os.path.exists(f):
            os.remove(f)

    print("\n测试完成!")
