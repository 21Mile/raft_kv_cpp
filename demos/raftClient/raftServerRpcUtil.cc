#include "raftServerRpcUtil.hpp"

// kvserver
// 构造连接
raftServerRpcUtil::raftServerRpcUtil(std::string ip, short port)
{
    stub = new raftKVRpcProctoc::kvServiceRpc_Stub(new MprpcChannel(ip, port, false));
}
raftServerRpcUtil::~raftServerRpcUtil()
{
    delete stub; // new 的内存需要自己手动释放
}
bool raftServerRpcUtil::Get(raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply)
{
    MprpcController controller;
    stub->Get(&controller, args, reply, nullptr); // 最后一个参数done，让他自动构造
    return !controller.Failed();
}

bool raftServerRpcUtil::PutAppend(raftKVRpcProctoc::PutAppendArgs * args,raftKVRpcProctoc::PutAppendReply *reply){
    MprpcController controller;
    stub->PutAppend(&controller,args,reply,nullptr);
    if(controller.Failed()){
        std::cout<<controller.ErrorText()<<std::endl;
    }
    return !controller.Failed();
}