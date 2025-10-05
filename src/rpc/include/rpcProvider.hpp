
#ifndef RPCPROVIDER_H
#define RPCPROVIDER_H
#include <google/protobuf/descriptor.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/TcpServer.h>
#include <functional>
#include <string>
#include <unordered_map>
#include "google/protobuf/service.h"

class RpcProvider
{

public:
    void NotifyService(google::protobuf::Service *service);

    // 启动RPC节点
    void Run(int nodeIndex, short port);

private:
    muduo::net::EventLoop m_eventloop;
    std::shared_ptr<muduo::net::TcpServer> m_muduo_server;
    // service服务类信息
    struct ServiceInfo
    {
        google::protobuf::Service *m_service;                                                    // 保存服务对象
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> m_methodMap; // 保存服务方法
    };
    // 保存服务对象和服务方法的所有信息
    std::unordered_map<std::string, ServiceInfo> m_serviceMap;
    // 新得socket
    void OnConnection(const muduo::net::TcpConnectionPtr &);
    // 已经建立连接用户的读写事件回调
    void OnMessage(const muduo::net::TcpConnectionPtr &, muduo::net::Buffer *, muduo::Timestamp);
    // closure的回调操作，用于序列化rpc的响应和网络发送
    void SendRpcResponse(const muduo::net::TcpConnectionPtr &, google::protobuf::Message *);

public:
    ~RpcProvider(); // 析构
};

#endif // !RPCPROVIDER_H
