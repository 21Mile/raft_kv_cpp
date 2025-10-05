#include "kvServer.hpp"

// 基本的KV服务器实现
// TODO: 在这里添加具体的实现代码

void KvServer::DprintfKVDB()
{
    if (!Debug)
    {
        return;
    }
    std::lock_guard<std::mutex> lg(m_mtx); // 获取互斥锁
    DEFER
    {
        m_skipList.display_list();
    };
}
// 执行追加
void KvServer::ExecuteAppendOpOnKVDB(Op op)
{
    m_mtx.lock();
    m_skipList.insert_set_element(op.Key, op.Value);
    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock(); // 解锁
    DprintfKVDB();  // 打印
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist)
{
    m_mtx.lock();
    *value = "";
    // 查询结果
    *exist = m_skipList.search_element(op.Key, *value);
    m_lastRequestId[op.ClientId] = op.RequestId; // 记录 每个 ClientId 最近处理过的 RequestId。
    m_mtx.unlock();
    if (*exist)
    {
        //                DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, value :%v", op.ClientId,
        //                op.RequestId, op.Key, value)
    }
    else
    {
        //        DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!", op.ClientId,
        //        op.RequestId, op.Key)
    }
    DprintfKVDB();
}
void KvServer::ExecutePutOpOnKVDB(Op op)
{
    m_mtx.lock();
    m_skipList.insert_set_element(op.Key, op.Value);
    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();
    DprintfKVDB();
}
// 处理来自clerk的GET
void KvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply)
{
    Op op;
    op.Operation = "Get";
    op.Key = args->key();
    op.Value = "";
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();
    // 处理请求
    int raftIndex = -1;
    int _ = -1;
    bool isLeader = false;
    // raftIndex：raft预计的logIndex
    // 虽然是预计，但是正确情况下是准确的，op的具体内容对raft来说 是隔离的
    // 这里的Start函数不是远程调用的，二十一个在本地的处理函数
    m_raftNode->Start(op, &raftIndex, &_, &isLeader);
    // 只有 leader 有资格把客户端命令提交到 Raft 日志，保证集群一致性
    if (!isLeader)
    {
        reply->set_err(ErrWrongLeader); // 返回请求（类似于上下文，这里实现业务逻辑）
        return;
    }
    // 并发安全
    m_mtx.lock();
    // 无法找到这个key，则创建
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end())
    {
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    // 取到一个当前节点的等待队列，等待后续吧reply写入这个队列
    auto chForRaftIndex = waitApplyCh[raftIndex];
    m_mtx.unlock(); // 直接解锁，等待任务执行完成，不能一直拿锁等待

    Op raftCommitOp;
    // 通过异步队列等待op
    // 假设 Clerk 向 KvServer 发送一个 GET key="foo" 请求，这个请求先通过 Raft 共识（Start），然后 Raft apply 线程会在共识完成后往对应的 waitApplyCh[raftIndex] 队列里 Push 一条 Op 作为“执行完成”的信号。
    // 如果 timeOutPop 在 CONSENSUS_TIMEOUT 内拿到这个 Op → 说明共识完成，进入 else 分支正常执行返回结果。
    // 如果超时没拿到 → 说明可能共识还没完成、日志被覆盖、或者 leader 变了，就进入 if 分支走超时处理。
    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp))
    {
        // 在等待一定时间内没有收到 Raft 已提交该索引的通知（即日志可能还没被提交或领导权发生变化）
        int _ = -1;
        bool isLeader = false;
        m_raftNode->GetState(&_, &isLeader);
        // 如果 当前仍是 leader 且该请求是重复请求（ifRequestDuplicate(...) && isLeader），说明这个请求之前已经被提交过，可以安全地直接在本地执行并返回结果。
        if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader)
        {
            std::string value;
            bool exist = false;
            // 查找调表，将value保存
            ExecuteGetOpOnKVDB(op, &value, &exist);
            if (exist)
            {
                reply->set_err(OK);
                reply->set_value(value);
            }
            else
            {
                reply->set_err(ErrNoKey);
                reply->set_value("");
            }
        }
        else
        {
            reply->set_err(ErrWrongLeader); // 让clerk换一个节点重试
        }
    }
    else
    {
        // raft已经提交了该op，可以执行了
        if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId)
        {

            std::string value;
            bool exist = false;
            // 查找跳表，将value保存
            ExecuteGetOpOnKVDB(op, &value, &exist);
            if (exist)
            {
                reply->set_err(OK);
                reply->set_value(value);
            }
            else
            {
                reply->set_err(ErrNoKey);
                reply->set_value("");
            }
        }
    }
}

// 从leader节点获取命令
void KvServer::GetCommandFromRaft(ApplyMsg message)
{
    Op op;
    op.parseFromString(message.Command); // 从字符串解析
    DPrintf(
        "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
        "Opreation {%s}, Key :{%s}, Value :{%s}",
        m_me, message.CommandIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

    // 如果这个op已经过期了（执行过了，但是这个操作因为网络延迟才刚刚到达）
    if (message.CommandIndex <= m_lastSnapShotRaftLogIndex)
    {
        return;
    }

    if (!ifRequestDuplicate(op.ClientId, op.RequestId))
    {
        if (op.Operation == "Put")
        {
            ExecutePutOpOnKVDB(op); // 向跳表执行操作
        }
        if (op.Operation == "Append")
        {
            ExecuteAppendOpOnKVDB(op);
        }
    }

    // kvDB的快照
    if (m_maxRaftState != -1)
    {
        // 如果raft的log太大（大于指定的比例）就制作快照
        IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    }

    SendMessageToWaitChan(op, message.CommandIndex);
}

bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId)
{
    std::lock_guard<std::mutex> lg(m_mtx); // 独占锁 : RAII自动释放
    if (m_lastRequestId.find(ClientId) == m_lastRequestId.end())
    {
        return false; // 如果不存在这个键，则创建
    }
    return RequestId <= m_lastRequestId[ClientId];
}

// Put操作或者Append操作
void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply)
{
    Op op;
    // 将字段写入op
    op.Operation = args->op();
    op.Key = args->key();
    op.Value = args->value();
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();
    // 初始化变量
    int raftIndex = -1; // raft节点序号
    int _ = -1;
    bool isleader = false;
    // 启动节点：然后自动触发选举等操作
    m_raftNode->Start(op, &raftIndex, &_, &isleader);

    // start函数内部会修改变量，判断它是不是leader
    if (!isleader)
    {
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , but "
            "not leader",
            m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);
        reply->set_err(ErrWrongLeader); // 当前节点不是leader
        return;
    }
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
        "leader ",
        m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);
    m_mtx.lock();

    if (waitApplyCh.find(raftIndex) == waitApplyCh.end())
    {
        // 如果等待执行的队列当中，不存在当前节点的序号(ID)，那么就创建一个pair，写入map，后续继续使用
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    auto chForRaftIndex = waitApplyCh[raftIndex]; // 拿出异步队列
    m_mtx.unlock();                               // 并发安全结束
    Op raftCommitOp;
    // 执行操作，向其他节点rpc调用
    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp))
    {
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
            "ClientId %s, RequestId %s, Opreation %s Key :%s, Value :%s",
            m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
        if (ifRequestDuplicate(op.ClientId, op.RequestId))
        {
            // 超时了,但因为是重复的请求，返回ok，实际上就算没有超时，在真正执行的时候也要判断是否重复
            reply->set_err(OK);
        }
        else
        {
            reply->set_err(ErrWrongLeader); // leader节点错误
        }
    }
    m_mtx.lock();
    auto tmp = waitApplyCh[raftIndex];
    waitApplyCh.erase(raftIndex); // 删除
    delete tmp;
    m_mtx.unlock();
}

void KvServer::ReadRaftApplyCommandLoop()
{
    while (true)
    {
        // 如果只操作applyChan不用拿锁，因为applyChan自己带锁
        auto message = applyChan->Pop();
        DPrintf("---------------tmp-------------[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息", m_me);

        if (message.CommandValid)
        {
            GetCommandFromRaft(message);
        }
        if (message.SnapshotValid)
        {
            GetSnapShotFromRaft(message); // 向leader节点获取快照
        }
    }
}

// raft会与persist层交互，kvserver层也会，因为kvserver层开始的时候需要恢复kvdb的状态
//  关于快照raft层与persist的交互：保存kvserver传来的snapshot；生成leaderInstallSnapshot RPC的时候也需要读取snapshot；
//  因此snapshot的具体格式是由kvserver层来定的，raft只负责传递这个东西
//  snapShot里面包含kvserver需要维护的persist_lastRequestId 以及kvDB真正保存的数据persist_kvdb
void KvServer::ReadSnapShotToInstall(std::string snapshot)
{
    if (snapshot.empty())
    {
        return;
    }
    parseFromString(snapshot);
}

bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex)
{
    std::lock_guard<std::mutex> lg(m_mtx);
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
        "{%d}, Opreation {%s}, Key :{%s}, Value :{%s}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end())
    {
        return false;
    }
    waitApplyCh[raftIndex]->Push(op);
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
        "{%d}, Opreation {%s}, Key :{%s}, Value :{%s}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    return true;
}
// 如果需要发送snapshot
void KvServer::IfNeedToSendSnapShotCommand(int raftindex, int proportion)
{
    if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0)
    {
        auto snapshot = MakeSnapShot();
        m_raftNode->Snapshot(raftindex, snapshot);
    }
}
// 从leader节点获取snapshot
void KvServer::GetSnapShotFromRaft(ApplyMsg message)
{
    std::lock_guard<std::mutex> lg(m_mtx);
    if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot))
    {
        ReadSnapShotToInstall(message.Snapshot);
        m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
    }
}

// 新建一个snapshot
std::string KvServer::MakeSnapShot()
{
    std::lock_guard<std::mutex> lg(m_mtx);
    std::string snapshotData = getSnapshotData();
    return snapshotData;
}

// ------------------------- KvServer 将rpc的调用封装，------------------------

// 将rpc的操作封装在这一层，内部调用方法
void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request, ::raftKVRpcProctoc::PutAppendReply *repsonse, ::google::protobuf::Closure *done)
{
    KvServer::PutAppend(request, repsonse);
    done->Run(); // 传递标志，表示已经处理完成
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request, ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done)
{
    KvServer::Get(request, response);
    done->Run();
}

// --- kvserver 构造方法 ----
KvServer::KvServer(int me, int maxraftstate, std::string nodeInfoFileName, short port)
    : m_skipList(6), m_me(me), m_maxRaftState(maxraftstate)
{
    m_me = me;
    m_maxRaftState = maxraftstate; // 持久化时间
    applyChan = std::make_shared<LockQueue<ApplyMsg>>();
    m_raftNode = std::make_shared<Raft>(); // 新建一个节点
    std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);

    // 先解析节点配置并准备 peers（确保 init 完成后再对外暴露 RPC）
    MprpcConfig config;
    config.LoadConfigFile(nodeInfoFileName.c_str());

    std::vector<std::pair<std::string, short>> ipPortVt;
    for (int i = 0; i < INT_MAX - 1; ++i)
    {
        const std::string node = "node" + std::to_string(i);

        // 兼容键：支持 "node0.ip"/"node0.port" 和老式 "node0ip"/"node0port"
        std::string nodeIp = config.Load(node + ".ip");
        std::string nodePortStr = config.Load(node + ".port");
        if (nodeIp.empty())        nodeIp = config.Load(node + "ip");
        if (nodePortStr.empty())   nodePortStr = config.Load(node + "port");

        if (nodeIp.empty() && nodePortStr.empty())
        {
            // 你的配置是连续编号，保留 break；如非连续可改成 continue
            break;
        }

        int portNum = 0;
        try { if (!nodePortStr.empty()) portNum = std::stoi(nodePortStr); }
        catch (...) { portNum = 0; }

        if (nodeIp.empty() || portNum <= 0 || portNum > 65535)
        {
            std::cout << "[CONF-ERR] skip invalid node entry: " << node
                      << " ip=" << (nodeIp.empty() ? "<empty>" : nodeIp)
                      << " port=" << nodePortStr << std::endl;
            continue;
        }

        ipPortVt.emplace_back(nodeIp, static_cast<short>(portNum));
    }

    // 启动连接（servers.size() == ipPortVt.size()）
    std::vector<std::shared_ptr<RaftRpcUtil>> servers;
    servers.reserve(ipPortVt.size());

    for (size_t i = 0; i < ipPortVt.size(); ++i)
    {
        if (static_cast<int>(i) == m_me)
        {
            servers.push_back(nullptr); // 自己占位
            continue;
        }
        const std::string &otherNodeIp = ipPortVt[i].first;
        short otherNodePort = ipPortVt[i].second;
        auto *rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);
        servers.emplace_back(std::shared_ptr<RaftRpcUtil>(rpc));
        std::cout << "node" << m_me << " 连接 node" << i
                  << " -> " << otherNodeIp << ":" << otherNodePort << " success!"
                  << std::endl;
    }

    // （可选）给网络层 1s 缓冲时间；删除原来的 sleep(ipPortVt.size() - me)
    sleep(1);

    // ★ 关键：先初始化 Raft，再开启对外 RPC
    m_raftNode->init(servers, m_me, persister, applyChan);

    //////////////// clerk 层面：kvserver 开启 rpc 接受功能（放到 init 之后）
    std::thread t([this, port]() -> void
                  {
                      RpcProvider provider;
                      provider.NotifyService(this);
                      provider.NotifyService(this->m_raftNode.get());
                      // 启动一个 rpc 服务发布节点（内部会阻塞在 loop）
                      provider.Run(m_me, port);
                  });
    t.detach(); // 不阻塞的执行

    // 开启 rpc 远程调用能力，需要注意必须要保证所有节点都开启 rpc 接受功能之后才能开启 rpc 远程调用能力
    // 保留你的等待逻辑（只移动到了 init/RPC 之后，行为与原意一致）
    std::cout << "raftServer node:" << m_me << " start to sleep to wait all ohter raftnode start!!!!" << std::endl;
    sleep(6); // 休眠完成后，自动唤醒，开始和其他节点尝试取得连接
    std::cout << "raftServer node:" << m_me << " wake up!!!! start to connect other raftnode" << std::endl;

    //  kv 的 server 直接与 raft 通信；安装已有快照
    m_lastSnapShotRaftLogIndex = 0;
    auto snapshot = persister->ReadSnapshot(); // 读取快照
    if (!snapshot.empty())
    {
        ReadSnapShotToInstall(snapshot); // 安装快照
    }

    std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this);
    t2.join(); // 阻塞调用
}
