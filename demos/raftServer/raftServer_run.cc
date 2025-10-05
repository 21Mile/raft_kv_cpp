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
  for (int i = 0; i < nodeNum; i++)
  {
    std::string nodeName = "node" + std::to_string(i);
    int port = cfg.at(nodeName).port;
    std::ostringstream oss;

    pid_t pid = fork(); // 创建新进程 (注意这里是进程，不是线程)
    if (pid == 0)
    {
      // 重定向日志到输出
      ::mkdir("logs", 0755);
      std::string logPath = "logs/node" + std::to_string(i) + ".log";
      int fd = ::open(logPath.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
      if (fd >= 0)
      {
        ::dup2(fd, STDOUT_FILENO);
        ::dup2(fd, STDERR_FILENO);
        ::close(fd);
        // 关闭缓冲，确保每次 printf/puts 都立即落盘
        setvbuf(stdout, nullptr, _IONBF, 0);
        setvbuf(stderr, nullptr, _IONBF, 0);
      }

      // 如果是子进程
      oss << "start to create raftkv node:" << i << "    port:" << port << " pid:" << getpid();
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
