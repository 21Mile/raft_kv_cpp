#include "rpcProvider.hpp"
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <fstream>
#include <string>
#include <arpa/inet.h> // inet_pton
#include <cstdlib>     // std::getenv
#include <thread>      // std::thread::hardware_concurrency
#include <algorithm>   // std::max, std::min
#include <fcntl.h>
#include "rpcheader.pb.h"
#include "m_utils.hpp"

/*
service_name =>  service描述
                        =》 service* 记录服务对象
                        method_name  =>  method方法对象
json   protobuf
*/
// 这里是框架提供给外部使用的，可以发布rpc方法的函数接口
// 只是简单的把服务描述符和方法描述符全部保存在本地而已
void RpcProvider::NotifyService(google::protobuf::Service *service)
{
  ServiceInfo service_info;
  // 获取服务描述
  const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
  std::string service_name = pserviceDesc->name();
  // 获取服务对象service的方法和数量
  int methodCnt = pserviceDesc->method_count();
  std::cout << "service name:" << service_name << std::endl;

  for (int i = 0; i < methodCnt; ++i)
  {
    const google::protobuf::MethodDescriptor *pmethodDesc = pserviceDesc->method(i);
    std::string method_name = pmethodDesc->name();
    service_info.m_methodMap.insert({method_name, pmethodDesc});
  }
  service_info.m_service = service;
  m_serviceMap.insert({service_name, service_info});
  ;
}
// 使用gprc框架和自定义RPC的区别：grpc是使用现成的代码，而自定义的rpc，是使用自定义的服务器来处理输入、输出
// protobuf在这里只是扮演一个类似于json的角色：对数据进行格式化和反格式化，中间状态为二进制流（便于传输）
// 需要的头文件（通常你的工程里已包含，如无则加上）

void RpcProvider::Run(int nodeIndex, short port)
{
  const std::string ip = "0.0.0.0"; // 直接绑 0.0.0.0，避免主机名解析坑
  muduo::net::InetAddress address(ip, port);
  m_muduo_server = std::make_shared<muduo::net::TcpServer>(&m_eventloop, address, "RpcProvider");

  m_muduo_server->setConnectionCallback(
      std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
  m_muduo_server->setMessageCallback(
      std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
  m_muduo_server->setThreadNum(4);

  // !!! 把任何可能“清 fd / daemonize / fork”的步骤都放在 start() 之前完成 !!!
  // 确保 start() 之后，不再有“清理 fd”的动作

  m_muduo_server->start(); // 真正 bind/listen

  // 自检：本进程回连自身端口；失败则立刻报错退出
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(port);
  sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ::fcntl(fd, F_SETFL, O_NONBLOCK);
  int r = ::connect(fd, (sockaddr *)&sa, sizeof(sa));
  if (!(r == 0 || (r == -1 && errno == EINPROGRESS)))
  {
    fprintf(stderr, "[WARN] self-connect check failed node=%d port=%d errno=%d\n",
            nodeIndex, port, errno);
    // 不 abort；必要时可 sleep 后重试几次
  }
  ::close(fd);

  std::cout << "RpcProvider listen ok at 0.0.0.0:" << port
            << " nodeIndex:" << nodeIndex << " threads:" << 4 << std::endl;

  m_eventloop.loop();
}

// 新的socket连接回调
void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr &conn)
{
  // 如果是新连接就什么都不干，即正常的接收连接即可
  if (!conn->connected())
  {
    // 和rpc client的连接断开了
    conn->shutdown();
  }
}

/*
在框架内部，RpcProvider和RpcConsumer协商好之间通信用的protobuf数据类型
service_name method_name args    定义proto的message类型，进行数据头的序列化和反序列化
                                 service_name method_name args_size
16UserServiceLoginzhang san123456

header_size(4个字节) + header_str + args_str
10 "10"
10000 "1000000"
std::string   insert和copy方法
*/
// 已建立连接用户的读写事件回调 如果远程有一个rpc服务的调用请求，那么OnMessage方法就会响应
// 这里来的肯定是一个远程调用请求
// 因此本函数需要：解析请求，根据服务名，方法名，参数，来调用service的来callmethod来调用本地的业务

void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buffer, muduo::Timestamp)
{
  // 接受RPC字符流
  std::string recv_buf = buffer->retrieveAllAsString();
  // protobuf解析数据流
  google::protobuf::io::ArrayInputStream array_input(recv_buf.data(), recv_buf.size());
  google::protobuf::io::CodedInputStream coded_input(&array_input); // 获取经过解密的数据
  uint32_t header_size{};                                           // 解决粘包问题
  coded_input.ReadVarint32(&header_size);                           // 读取一个uint32的长度，解析为header
  std::string rpc_header_str;
  RPC::RpcHeader rpcHeader;
  std::string service_name;
  std::string method_name;
  // 设置读取限制
  google::protobuf::io::CodedInputStream::Limit msg_limit = coded_input.PushLimit(header_size);
  coded_input.ReadString(&rpc_header_str, header_size); // 读取header_size的数据到rpc_header_str
  coded_input.PopLimit(msg_limit);
  // 读取args
  uint32_t args_size{};
  if (rpcHeader.ParseFromString(rpc_header_str))
  {
    // 数据头反序列化成功
    service_name = rpcHeader.service_name();
    method_name = rpcHeader.method_name();
    args_size = rpcHeader.args_size(); // 参数大小
  }
  else
  {
    // 数据头反序列化失败
    std::cout << "rpc_header_str:" << rpc_header_str << " parse error!" << std::endl;
    return;
  }
  std::string args_str;
  // 读取参数
  if (!coded_input.ReadString(&args_str, args_size))
  {
    // 数据读取失败
    return;
  }
  // 打印调试信息
  //    std::cout << "============================================" << std::endl;
  //    std::cout << "header_size: " << header_size << std::endl;
  //    std::cout << "rpc_header_str: " << rpc_header_str << std::endl;
  //    std::cout << "service_name: " << service_name << std::endl;
  //    std::cout << "method_name: " << method_name << std::endl;
  //    std::cout << "args_str: " << args_str << std::endl;
  //    std::cout << "============================================" << std::endl;

  // -- 获取service对象
  auto it = m_serviceMap.find(service_name);
  if (it == m_serviceMap.end())
  {
    std::cout << "服务：" << service_name << " is not exist!" << std::endl;
    std::cout << "当前已经有的服务列表为:";
    for (auto item : m_serviceMap)
    {
      std::cout << item.first << " ";
    }
    std::cout << std::endl;
    return;
  }
  auto mit = it->second.m_methodMap.find(method_name);
  if (mit == it->second.m_methodMap.end())
  {
    return;
  }
  google::protobuf::Service *service = it->second.m_service;
  const google::protobuf::MethodDescriptor *method = mit->second;
  // 生成rpc方法调用的请求和响应，由于是rpc请求，所以请求需要通过request来序列化
  google::protobuf::Message *request = service->GetRequestPrototype(method).New();
  if (!request->ParseFromString(args_str))
  {
    return;
  }
  google::protobuf::Message *response = service->GetResponsePrototype(method).New();
  // 给下面的method方法调用，绑定一个closure的回调
  // closure是执行完本地方法之后的回调
  google::protobuf::Closure *done = google::protobuf::NewCallback<RpcProvider, const muduo::net::TcpConnectionPtr &, google::protobuf::Message *>(this, &RpcProvider::SendRpcResponse, conn, response);

  // 在框架上根据远端rpc请求，调用当前rpc节点上发布的方法
  // new UserService().Login(controller, request, response, done)
  /*
为什么下面这个service->CallMethod 要这么写？或者说为什么这么写就可以直接调用远程业务方法了
这个service在运行的时候会是注册的service
// 用户注册的service类 继承 .protoc生成的serviceRpc类 继承 google::protobuf::Service
// 用户注册的service类里面没有重写CallMethod方法，是 .protoc生成的serviceRpc类 里面重写了google::protobuf::Service中
的纯虚函数CallMethod，而 .protoc生成的serviceRpc类 会根据传入参数自动调取 生成的xx方法（如Login方法），
由于xx方法被 用户注册的service类 重写了，因此这个方法运行的时候会调用 用户注册的service类 的xx方法
真的是妙呀
*/
  // 真正调用方法
  service->CallMethod(method, nullptr, request, response, done);
}

// Closure的回调操作，用于序列化rpc的响应和网络发送,发送响应回去
void RpcProvider::SendRpcResponse(const muduo::net::TcpConnectionPtr &conn, google::protobuf::Message *response)
{
  std::string response_str;
  if (response->SerializeToString(&response_str))
  {
    // 序列化成功，则将response发送到客户端
    conn->send(response_str);
  }
  else
  {
    std::cout << "serialize response_str error!" << std::endl;
  }
  //    conn->shutdown(); // 模拟http的短链接服务，由rpcprovider主动断开连接  //改为长连接，不主动断开
}
// RAII 自动析构
RpcProvider::~RpcProvider()
{
  std::cout << "[func - RpcProvider::~RpcProvider()]: ip和port信息：" << m_muduo_server->ipPort() << std::endl;
  m_eventloop.quit();
  //    m_muduo_server.   怎么没有stop函数，奇奇怪怪，看csdn上面的教程也没有要停止，甚至上面那个都没有
}