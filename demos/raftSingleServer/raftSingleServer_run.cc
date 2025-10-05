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
#include <sys/stat.h>
#include "mlog.hpp" //有的时候头文件的检测是有问题
#include "nodeConfig.hpp"
void ShowArgsHelp();

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
  const auto cfg = parseNodeConfig(configFilePath);
  mlog::LOG("解析配置完成：" + configFilePath);

  // 启动 节点线程，对外提供服务
  for (int i = 0; i < 1; i++) //只启动一个节点，不开启多进程
  {
    std::string nodeName = "node" + std::to_string(1);
    int port = cfg.at(nodeName).port;
    std::ostringstream oss;

    // 如果是子进程
    oss << "start to create raftkv node:" << i << "    port:" << port << " pid:" << getpid();
    mlog::LOG(oss.str());
    oss.clear();
    // 子进程的代码: 启动节点

    auto kvServer = new KvServer(i, 500, configFilePath, port);

    std::cout << "Normal Exit!" << std::endl;
    return 0;
  }
}

  void ShowArgsHelp() { std::cout << "format: command -n <nodeNum> -f <configFileName>" << std::endl; }
