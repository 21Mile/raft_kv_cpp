#include "raftClient.hpp"
#include "m_utils.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <map>
#include <random>
#include <cassert>

using std::cout;
using std::endl;

// 小工具：更友好的断言展示
static void expect_eq(const std::string &what, const std::string &got, const std::string &expect);

// 小工具：毫秒级睡眠，优先用标准库（你项目里也有 sleepNMilliseconds，可二选一）
static void SleepMs(int ms);

int main()
{
  Client client;

  // 0) 初始化客户端，加载配置（比如 peers 列表、重试参数等）
  //    建议：此处失败要尽早退出，避免后续误判
  try
  {
    client.Init("/home/cyan/CPP_Project/My_Raft_KV/configs/test.toml");
    std::puts("[Init] 客户端初始化完成，准备开始分阶段测试 ...");
  }
  catch (const std::exception &e)
  {
    std::fprintf(stderr, "[Init] 初始化失败：%s\n", e.what());
    return 1;
  }

  // 0.1) 给集群一点时间完成选主（尤其是你刚刚启动服务端时）
  std::puts("[Warmup] 等待集群完成选主/稳定 ...");
  SleepMs(1500);

  // =========================
  // 测试一：单键高频写读（覆盖）
  // 目的：验证基本 Put/Get、以及覆盖写入后能读到最新值
  // =========================
  {
    std::puts("\n===== 测试一：单键循环写读（key = x）=====");
    const std::string key = "x";
    int rounds = 10; // 轮数适中即可，方便观察日志
    for (int i = 0; i < rounds; ++i)
    {
      std::string val = "v" + std::to_string(i);
      client.Put(key, val);
      std::string got = client.Get(key);
      expect_eq("单键写读 x", got, val);
      SleepMs(50); // 轻微间隔，便于观察心跳/提交推进
    }
  }
  SleepMs(300);

  // =========================
  // 测试二：多键样例数据写读
  // 目的：验证不同 key 的独立性、KV 编码/路由是否正常
  // =========================
  {
    std::puts("\n===== 测试二：多键样例数据写读 =====");
    std::map<std::string, std::string> samples = {
        {"name", "ZhangSan"},
        {"age", "23"},
        {"city", "Nanjing"},
        {"lang", "C++17"},
        {"project", "raft-kv"}};
    for (auto &kv : samples)
    {
      client.Put(kv.first, kv.second);
      std::string got = client.Get(kv.first);
      expect_eq("多键写读 " + kv.first, got, kv.second);
    }
  }
  SleepMs(300);

  // =========================
  // 测试三：覆盖写（同一键重复 Put）
  // 目的：确认“最后一次写入”可见；日志可能携带多个同 key 指令
  // =========================
  {
    std::puts("\n===== 测试三：覆盖写（同 key 多次 Put）=====");
    const std::string key = "cover";
    client.Put(key, "old");
    SleepMs(30);
    client.Put(key, "mid");
    SleepMs(30);
    client.Put(key, "new");
    std::string got = client.Get(key);
    expect_eq("覆盖写 cover", got, "new");
  }
  SleepMs(300);

  // =========================
  // 测试四：大 Value（例如 ~8KB）
  // 目的：验证序列化/传输/日志切分是否正常（视实现）
  // =========================
  {
    std::puts("\n===== 测试四：大 Value 写读 =====");
    const std::string key = "big";
    std::string big(8 * 1024, 'A'); // 8KB 的 'A'
    client.Put(key, big);
    auto got = client.Get(key);
    if (got.size() == big.size() && got == big)
    {
      std::printf("[OK]   大Value 长度=%zu 验证通过\n", got.size());
    }
    else
    {
      std::printf("[FAIL] 大Value 长度不一致：期望=%zu 实际=%zu\n", big.size(), got.size());
    }
  }
  SleepMs(300);

  // =========================
  // 测试五：不存在的键
  // 目的：确认未写入的 key 返回约定值（空串/NOT_FOUND/异常），
  //       下方采用“允许空串”的方式展示，你可以按你实际的 API 约定改判定
  // =========================
  {
    std::puts("\n===== 测试五：不存在的键 =====");
    const std::string key = "not_exist_key_123456";
    std::string got = client.Get(key);
    if (got.empty())
    {
      std::puts("[OK]   不存在键返回空串（与当前客户端约定一致）");
    }
    else
    {
      std::printf("[WARN] 不存在键返回了非空：%s（请确认服务端约定）\n", got.c_str());
    }
  }
  SleepMs(300);

  // =========================
  // 测试六：简单并发（多线程不同 key）
  // 目的：模拟多客户端并发写读，重点看 Raft 复制与提交在压力下的稳定性
  //      每个线程写自己的 key 域，避免读写冲突过多
  // =========================
  {
    std::puts("\n===== 测试六：并发写读（4 线程 * 50 次）=====");
    const int THREADS = 4;
    const int OPS_PER_THREAD = 50;

    auto worker = [&](int tid)
    {
      for (int i = 0; i < OPS_PER_THREAD; ++i)
      {
        std::string key = "k_" + std::to_string(tid) + "_" + std::to_string(i);
        std::string val = "val_" + std::to_string(tid) + "_" + std::to_string(i);
        client.Put(key, val);
        std::string got = client.Get(key);
        if (got != val)
        {
          std::fprintf(stderr, "[T%d][FAIL] %s -> 期望=%s 实际=%s\n", tid, key.c_str(), val.c_str(), got.c_str());
        }
        // 不要太快，避免把日志刷成纯吞吐压测；留出复制/提交间隔
        SleepMs(5);
      }
    };

    std::vector<std::thread> ths;
    ths.reserve(THREADS);
    for (int t = 0; t < THREADS; ++t)
      ths.emplace_back(worker, t);
    for (auto &th : ths)
      th.join();

    std::puts("[OK]   并发阶段完成（如有 FAIL 请回看服务端 DPrintf 日志）");
  }
  SleepMs(300);

  // =========================
  // 测试七：小规模“回归循环”方便你长期观察（可选）
  // 目的：长跑观察心跳/提交，定位偶发现象（可按需放开）
  // =========================
  {
    std::puts("\n===== 测试七：小规模回归循环（可选）=====");
    int count = 20;
    while (count--)
    {
      std::string key = "loop_key";
      std::string val = "loop_val_" + std::to_string(count);
      client.Put(key, val);
      std::string got = client.Get(key);
      expect_eq("回归循环 loop_key", got, val);
      SleepMs(30);
    }
  }

  std::puts("\n[Done] 所有基础测试结束。可以结合服务端 DPrintf，按阶段定位问题。");
  return 0;
}

static void expect_eq(const std::string &what, const std::string &got, const std::string &expect)
{
  if (got != expect)
  {
    std::fprintf(stderr, "[FAIL] %s 期望=%s 实际=%s\n", what.c_str(), expect.c_str(), got.c_str());
  }
  else
  {
    std::printf("[OK]   %s -> %s\n", what.c_str(), got.c_str());
  }
}
static void SleepMs(int ms)
{
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}