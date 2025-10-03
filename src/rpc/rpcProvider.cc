#include "rpcProvider.hpp"
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <fstream>
#include <string>
#include "rpcheader.pb.h"
#include "utils.hpp"

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
//  protobuf在这里只是扮演一个类似于json的角色：对数据进行格式化和反格式化，中间状态为二进制流（便于传输）
void RpcProvider::Run(int nodeIndex, short port)
{
  char *ipC;
  char hname[128];
  struct hostent *hent;
  gethostname(hname, sizeof(hname)); // 将获取host的名称，写入到hname中
  for (int i = 0; hent->h_addr_list[i]; i++)
  {
    // 进行char数组的转换
    ipC = inet_ntoa(*(struct in_addr *)(hent->h_addr_list[i])); // IP地址
  }
  std::string ip = std::string(ipC); // 将char数组转换为string
  // 添加节点标识符
  std::string node = "node" + std::to_string(nodeIndex);
  std::ofstream outfile;
  outfile.open("test.conf", std::ios::app); // 以append的模式写入
  if (!outfile.is_open())
  {
    std::cout << "打开文件失败！" << std::endl;
    exit(EXIT_FAILURE);
  }
  outfile << node << "IP:" << ip << std::endl;
  outfile << node << "Port:" << port << std::endl;
  outfile.close(); // 关闭文件句柄

  // --- 二阶段，创建muduo服务器 ---
  muduo::net::InetAddress address(ip, port);
  m_muduo_server = std::make_shared<muduo::net::TcpServer>(&m_eventloop, address, "RpcProvider");
  // 绑定连接回调和消息读写回调方法  分离了网络代码和业务代码
  /*
  bind的作用：
  如果不使用std::bind将回调函数和TcpConnection对象绑定起来，那么在回调函数中就无法直接访问和修改TcpConnection对象的状态。因为回调函数是作为一个独立的函数被调用的，它没有当前对象的上下文信息（即this指针），也就无法直接访问当前对象的状态。
  如果要在回调函数中访问和修改TcpConnection对象的状态，需要通过参数的形式将当前对象的指针传递进去，并且保证回调函数在当前对象的上下文环境中被调用。这种方式比较复杂，容易出错，也不便于代码的编写和维护。因此，使用std::bind将回调函数和TcpConnection对象绑定起来，可以更加方便、直观地访问和修改对象的状态，同时也可以避免一些常见的错误。
  */
  // 设置回调（类似于中间件)
  // placeholders占位符，代表回调函数被调用时传入的第一个参数（这里就是 muduo 的 TcpConnectionPtr
  m_muduo_server->setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
  // 处理消息事件
  m_muduo_server->setMessageCallback(std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
  // 设置线程数量
  m_muduo_server->setThreadNum(4); // 一般设置为cpu核心数量
                                   //  rpc服务端准备启动，打印信息
  std::cout << "RpcProvider start service at ip:" << ip << " port:" << port << std::endl;
  m_muduo_server->start();
  m_eventloop.loop();
  /*
   这段代码是在启动网络服务和事件循环，其中server是一个TcpServer对象，m_eventLoop是一个EventLoop对象。
 首先调用server.start()函数启动网络服务。在Muduo库中，TcpServer类封装了底层网络操作，包括TCP连接的建立和关闭、接收客户端数据、发送数据给客户端等等。通过调用TcpServer对象的start函数，可以启动底层网络服务并监听客户端连接的到来。
 接下来调用m_eventLoop.loop()函数启动事件循环。在Muduo库中，EventLoop类封装了事件循环的核心逻辑，包括定时器、IO事件、信号等等。通过调用EventLoop对象的loop函数，可以启动事件循环，等待事件的到来并处理事件。
 在这段代码中，首先启动网络服务，然后进入事件循环阶段，等待并处理各种事件。网络服务和事件循环是两个相对独立的模块，它们的启动顺序和调用方式都是确定的。启动网络服务通常是在事件循环之前，因为网络服务是事件循环的基础。启动事件循环则是整个应用程序的核心，所有的事件都在事件循环中被处理。
   */
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
RpcProvider::~RpcProvider() {
  std::cout << "[func - RpcProvider::~RpcProvider()]: ip和port信息：" << m_muduo_server->ipPort() << std::endl;
  m_eventloop.quit();
  //    m_muduo_server.   怎么没有stop函数，奇奇怪怪，看csdn上面的教程也没有要停止，甚至上面那个都没有
}