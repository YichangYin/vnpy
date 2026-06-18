"""
Baostock + vn.py 集成模块

架构设计:
    [baostock 数据采集] → 写入 PostgreSQL → [vn.py 读取数据]
    [外部分析脚本] → 生成信号 → [vn.py 策略执行]

使用方式:
    1. 安装 baostock: pip install baostock
    2. 运行数据采集: python baostock_collector.py
    3. 生成交易信号: python signal_generator.py
    4. 启动 vn.py 执行交易
"""
