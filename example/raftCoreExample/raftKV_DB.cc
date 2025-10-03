//
// Created by swx on 23-12-28.
//
#include <iostream>
// #include "raft.h"
// #include "kvServer.h"
#include "kvServer.hpp"
#include <unistd.h>
#include <iostream>
#include <random>
#include <toml.hpp>
#include "mlog.hpp" //有的时候头文件的检测是有问题
// 节点配置结构
struct NodeConfig
{
  std::string nodeName;
  std::string ip;
  int port;
};
void ShowArgsHelp();
std::unordered_map<std::string, NodeConfig> parseNodeConfig(const std::string &filename);
// main
int main(int argc, char **argv)
{
  if (argc < 2)
  {
    ShowArgsHelp();
    exit(EXIT_FAILURE);
  }

  int c = 0, nodeNum = 0;
  std::string configFilePath = "";
  // 解析参数
  while ((c = getopt(argc, argv, "n:f:")) != -1)
  {
    switch (c)
    {
    case 'n':
      nodeNum = atoi(optarg);
      break;
    case 'f':
      configFilePath = optarg;
      break;
    default:
      ShowArgsHelp();
      exit(EXIT_FAILURE);
    }
  }
  // 解析config配置
  const auto &cfg = parseNodeConfig(configFilePath);
  mlog::LOG("解析配置完成：" + configFilePath);
  // 生成随机端口
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(10000, 29999);
  unsigned short startPort = dis(gen);

  // 启动 节点线程，对外提供服务
  for (int i = 0; i < nodeNum; i++)
  {
    short port = startPort + static_cast<short>(i);
    std::ostringstream oss;


    pid_t pid = fork(); // 创建新进程 (注意这里是进程，不是线程)
    if (pid == 0)
    {
      // 如果是子进程
      oss << "start to create raftkv node:" << i << "    port:" << port << " pid:" << getpid() ;
      mlog::LOG(oss.str());
      oss.clear();
      // 子进程的代码: 启动节点

      auto kvServer = new KvServer(i, 500, configFilePath, port);

      pause(); // 子进程进入等待状态，不会执行 return 语句
    }
    else if (pid > 0)
    {
      // 如果是父进程 : 默认启动的进程是父进程
      // 父进程的代码
      sleep(1);
    }
    else
    {
      // 如果创建进程失败
      std::cerr << "Failed to create child process." << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  pause(); // 使进程进入可中断的睡眠状态

  std::cout << "Normal Exit!" << std::endl;
  return 0;
}

void ShowArgsHelp() { std::cout << "format: command -n <nodeNum> -f <configFileName>" << std::endl; }

// 解析 TOML 配置文件
std::unordered_map<std::string, NodeConfig> parseNodeConfig(const std::string &filename)
{
  std::ifstream file(filename);
  if (!file.is_open())
  {
    throw std::runtime_error("无法打开配置文件: " + filename);
  }

  std::unordered_map<std::string, NodeConfig> nodes;
  std::string line;

  while (std::getline(file, line))
  {
    // 跳过空行和注释
    if (line.empty() || line[0] == '#')
      continue;

    // 分割键值对
    size_t pos = line.find('=');
    if (pos == std::string::npos)
      continue;

    std::string key = line.substr(0, pos);
    std::string value = line.substr(pos + 1);

    // 解析节点名称和属性
    if (key.size() > 5)
    {                                          // 至少 "nodeXip" 长度
      std::string nodeName = key.substr(0, 5); // "node0"
      std::string attr = key.substr(5);        // "ip" 或 "port"

      if (attr == "ip")
      {
        nodes[nodeName].ip = value;
      }
      else if (attr == "port")
      {
        try
        {
          nodes[nodeName].port = std::stoi(value);
        }
        catch (...)
        {
          std::cerr << "无效端口: " << value << " for " << nodeName << std::endl;
        }
      }
    }
  }

  return nodes;
}