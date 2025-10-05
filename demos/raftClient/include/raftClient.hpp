#ifndef raft_client
#define raft_client

#include <arpa/inet.h>
#include <netinet/in.h>

#include "raftServerRpcUtil.hpp"
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <string>
#include <vector>
#include "kvServerRpc.pb.h"
#include "mpRpcConfig.hpp"


// raft客户端：向状态机发送操作与数据
class Client
{
private:
    std::vector<std::shared_ptr<raftServerRpcUtil>> m_servers;
    std::string m_clientId; // 分配一个客户端id
    int m_requestId;
    int m_recentLeaderId; // 最近连接过的leader节点
    std::string Uuid()
    {
        // 返回一个随机的clientid
        std::string id="";
        for(int i=0;i<4;++i){
            id+=std::to_string(rand());
        }
        return id;
    }
    void PutAppend(std::string key, std::string value, std::string op);

public:
    Client(); // 空构造函数
    void Init(std::string configFileName);
    std::string Get(std::string key); // Get方法
    void Put(std::string key, std::string value);
    void Append(std::string key, std::string value);
};

#endif // !raft_client