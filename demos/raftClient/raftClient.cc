#include "raftClient.hpp"

#include "./include/raftServerRpcUtil.hpp"

#include <string>
#include <vector>
#include "m_utils.hpp"
#include "nodeConfig.hpp"
std::string Client::Get(std::string key)
{
    m_requestId++; // 客户端不是并发的
    int server = m_recentLeaderId;
    raftKVRpcProctoc::GetArgs args; // 构造一个参数
    args.set_key(key);
    args.set_clientid(m_clientId);
    args.set_requestid(m_requestId);
    int retry_count = 0;
    // 持续重试
    while (true)
    {
        raftKVRpcProctoc::GetReply reply;
        bool ok = m_servers[server]->Get(&args, &reply);
        if (!ok || reply.err() == ErrWrongLeader)
        { // utils.hpp的错误标识
            (++server) %= m_servers.size();
            continue;
        }
        if (++retry_count > MaxRetryCount)
        {
            return ""; // 超过最大重试次数限制
        }

        if (reply.err() == ErrNoKey)
        {
            return ""; // key不存在
        }

        if (reply.err() == OK)
        {
            m_recentLeaderId = server;
            return reply.value();
        }
    }
    return "";
}

// put 或者 append操作
void Client::PutAppend(std::string key, std::string value, std::string op)
{
    ++m_requestId;
    int retry_count = 0;
    int server = m_recentLeaderId;
    while (true)
    {
        raftKVRpcProctoc::PutAppendArgs args;
        args.set_key(key);
        args.set_value(value);
        args.set_op(op);
        args.set_clientid(m_clientId);
        args.set_requestid(m_requestId);
        raftKVRpcProctoc::PutAppendReply reply;
        bool ok = m_servers[server]->PutAppend(&args, &reply);
        if (!ok || reply.err() == ErrWrongLeader)
        { // utils.hpp的错误标识
            (++server) %= m_servers.size();
            std::cout << "client:putappend 重试" << std::endl;
            continue;
        }
        if (++retry_count > MaxRetryCount)
        {
            std::cout << "client:putappend 达到最大重试次数，请求失败" << std::endl;
            return; // 超过最大重试次数限制
        }

        if (reply.err() == ErrNoKey)
        {
            return; // key不存在
        }
        if (reply.err() == OK)
        {
            m_recentLeaderId = server;
            return;
        }
    }
}
// 封装了两个函数
void Client::Put(std::string key, std::string value) { PutAppend(key, value, "Put"); }

void Client::Append(std::string key, std::string value) { PutAppend(key, value, "Append"); }
Client::Client() : m_clientId(Uuid()), m_requestId(0), m_recentLeaderId(0) {}

// 初始化客户端
void Client::Init(std::string configFileName)
{
    auto cfg = parseNodeConfig(configFileName);
    mlog::LOG("解析配置完成：" + configFileName);
    std::vector<std::pair<std::string, short>> ipPortVt;
    for (int i = 0; i < INT_MAX - 1; ++i)
    {
        std::string nodeName = "node" + std::to_string(i);
        // 构造ip和port
        std::string nodeIp = cfg.at(nodeName).ip;
        std::string nodePort = std::to_string(cfg.at(nodeName).port);
        if (nodeIp.empty() || nodePort.empty())
        {
            break;
        }
        ipPortVt.emplace_back(nodeIp, atoi(nodePort.c_str()));
    }
    // 初始化完成，启动连接
    for (const auto &item : ipPortVt)
    {
        std::string ip = item.first;
        short port = item.second;
        auto *rpc = new raftServerRpcUtil(ip, port);
        m_servers.push_back(std::shared_ptr<raftServerRpcUtil>(rpc));
    }
    // 结束
}