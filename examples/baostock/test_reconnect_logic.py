"""验证 _reconnect_and_continue 重试逻辑是否正确"""
import threading
import time
import sys
import os

# 指向 baostock_bar_collector 所在目录
sys.path.insert(0, os.path.dirname(__file__))

# ---- 模拟 baostock ----
class FakeBs:
    """可控制 login() 行为的模拟对象"""
    login_result_code = "0"       # 期望返回的错误码
    login_delay = 0               # login() 内部延迟
    should_raise = False          # login() 是否抛异常
    login_call_count = 0

    def login(self):
        FakeBs.login_call_count += 1
        time.sleep(self.login_delay)
        if self.should_raise:
            raise ConnectionError("socket error")
        return type("Rs", (), {"error_code": self.login_result_code,
                                "error_msg": "test error"})()

    def logout(self):
        pass

# 把模拟对象注入到模块中
import baostock_bar_collector as mod
mod.bs = FakeBs()
mod.api_logger = mod._bs_query.__globals__['api_logger']  # 保留原始 logger

# ---- 工具函数 ----
def reset():
    FakeBs.login_result_code = "0"
    FakeBs.login_delay = 0
    FakeBs.should_raise = False
    FakeBs.login_call_count = 0

def test_return_value(case_name, expect_ok):
    """调用 _reconnect_and_continue 并检查返回值"""
    FakeBs.login_call_count = 0
    result = mod._reconnect_and_continue("query_k_data", 0, 5)
    status = "PASS" if (result == expect_ok and FakeBs.login_call_count == 1) else "FAIL"
    print(f"  [{status}] {case_name}: return={result}, login_called={FakeBs.login_call_count}")

def test_outer_loop_retry(success_on_attempt):
    """模拟外层 while 循环：前 N-1 次 login 失败，第 N 次成功"""
    FakeBs.login_call_count = 0
    current_attempt = 0

    def _simulate_reconnect(attempt, max_retries):
        FakeBs.login_result_code = "0" if FakeBs.login_call_count + 1 >= success_on_attempt else "1"
        return mod._reconnect_and_continue("test", attempt, max_retries)

    max_retries = 5
    attempts = []
    for attempt in range(max_retries):
        ok = _simulate_reconnect(attempt, max_retries)
        attempts.append(ok)
        if ok:
            break  # 成功，跳出
        current_attempt += 1

    passed = (attempts == [False] * (success_on_attempt - 1) + [True])
    status = "PASS" if passed else "FAIL"
    print(f"  [{status}] 第 {success_on_attempt} 次重连成功: attempts={attempts}")

# ---- 运行测试 ----
print("=" * 60)
print("测试 _reconnect_and_continue 返回值")
print("=" * 60)

reset(); FakeBs.login_result_code = "0"
test_return_value("登录成功", expect_ok=True)

reset(); FakeBs.login_result_code = "1"
test_return_value("登录返回错误码", expect_ok=False)

reset(); FakeBs.should_raise = True
test_return_value("登录抛异常", expect_ok=False)

print()
print("=" * 60)
print("测试外层循环：连续失败后终于成功")
print("=" * 60)

reset(); test_outer_loop_retry(1)   # 第1次就成功
reset(); test_outer_loop_retry(3)   # 第3次才成功
reset(); test_outer_loop_retry(5)   # 第5次才成功

print()
print("=" * 60)
print("测试外层循环：5 次全失败")
print("=" * 60)

reset()
FakeBs.login_result_code = "1"
FakeBs.login_call_count = 0
max_retries = 5
for attempt in range(max_retries):
    ok = mod._reconnect_and_continue("test", attempt, max_retries)
    if ok:
        break
status = "PASS" if FakeBs.login_call_count == 5 else "FAIL"
print(f"  [{status}] 全失败时重试了 {FakeBs.login_call_count} 次（预期 5 次）")

print()
print("全部测试完成。")
