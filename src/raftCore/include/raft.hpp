


#ifndef RAFT_H
#define RAFT_H

#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "boost/any.hpp"
#include "boost/serialization/serialization.hpp"
// 自定义头文件
#include "m_utils.hpp"
#include "ApplyMsg.hpp"
#include "Persister.hpp"
#include "mconfig.hpp"
#include "mlog.hpp"
#include "monsoon.hpp"
#include "raftRpcUtil.hpp"
#include "lockQueue.hpp"




// @var 网络状态表示
constexpr int Disconnected = 0;
constexpr int AppNormal = 1;

// @var 编译时常量表达式
constexpr int Killed = 0;
constexpr int Voted = 1;  // 本轮已经投过票了
constexpr int Expire = 2; // 投票（消息、竞选者）过期
constexpr int Normal = 3;

// @class Raft算法核心，继承自protoc
class Raft : public raftRpcProctoc::raftRpc
{
private:
    std::mutex m_mtx;
    std::vector<std::shared_ptr<RaftRpcUtil>> m_peers; // 跟随者
    std::shared_ptr<Persister> m_persister;            // 持久化
    int m_me;
    int m_currentTerm;
    int m_votedFor;
    // 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号
    std::vector<raftRpcProctoc::LogEntry> m_logs;
    int m_commitIndex;
    int m_lastApplied;            // 已经汇报给状态机（上层应用）的log 的index
    std::vector<int> m_nextIndex; // 这两个状态的下标1开始，因为通常commitIndex和lastApplied从0开始，应该是一个无效的index，因此下标从1开始
    std::vector<int> m_matchIndex;
    enum Status
    {
        Follower,
        Candidate,
        Leader
    }; // 节点状态定义
    Status m_status;                                // 节点状态
    std::shared_ptr<LockQueue<ApplyMsg>> applyChan; // 信号：获取日志，client与raft通信的接口

    // 选举超时时间:经过了这么长的时间后，节点会发起选举，每个节点的选举超时时间是不定的
    std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;
    std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime;

    // 2D中用于传入快照点
    // 储存了快照中的最后一个日志的Index和Term
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;
    // 协程
    std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;

    // @brief 成员方法
public:
    void AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply);
    void applierTicker();
    bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot);
    void doElection();
    // @cond 只有领导者节点才需要发送心跳包
    void doHeartBeat();
    // 每隔一段时间检查睡眠时间内有没有重置定时器，没有则说明超时了
    // 如果有则设置合适睡眠时间：睡眠到重置时间+超时时间
    void electionTimeOutTicker(); // 选举超时触发器
    std::vector<ApplyMsg> getApplyLogs();
    int getNewCommandIndex();                                     // 获取新的命令的序号
    void getPrevLogInfo(int server, int *preIndex, int *preTerm); // 获取前一个日志的信息：用于实现网络分区后的逐步回退
    void GetState(int *term, bool *isLeader);                     // 获取节点状态
    // 安装快照
    void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                         raftRpcProctoc::InstallSnapshotResponse *reply);
    // 领导者节点心跳触发器
    void leaderHearBeatTicker();
    void leaderSendSnapShot(int server); // 领导者节点发送快照
    void leaderUpdateCommitIndex();      // 领导者节点更新提交序号
    bool matchLog(int logIndex, int logTerm);
    void persist(); // 持久化
    // @attention 请求投票函数
    void RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply);
    bool UpToDate(int index, int term);
    // @attention 获取最近一条日志的索引和任期号
    int getLastLogIndex();
    int getLastLogTerm();
    void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
    int getLogTermFromLogIndex(int logIndex);
    int GetRaftStateSize(); // 获取状态size
    // 找到index对应的真实下标位置！！！
    int getSlicesIndexFromLogIndex(int logIndex);
    // 发送投票请求
    bool sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                         std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);
    // 发送追加日志实体请求
    bool sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                           std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums);

    // rf.applyChan <- msg //不拿锁执行  可以单独创建一个线程执行，但是为了同意使用std:thread
    // ，避免使用pthread_create，因此专门写一个函数来执行
    void pushMsgToKvServer(ApplyMsg msg); // 向状态机发送消息
    void readPersist(std::string data);   // 读取持久化数据
    std::string persistData();            // 数据持久化：工具函数
                                          // 启动节点
    void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);

    // Snapshot the service says it has created a snapshot that has
    // all info up to and including index. this means the
    // service no longer needs the log through (and including)
    // that index. Raft should now trim its log as much as possible.
    // index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
    // 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
    // 即服务层主动发起请求raft保存snapshot里面的数据，index是用来表示snapshot快照执行到了哪条命令
    void Snapshot(int index, std::string snapshot);

    // 重写基类方法,因为rpc远程调用真正调用的是这个方法
    // 序列化，反序列化等操作rpc框架都已经做完了，因此这里只需要获取值然后真正调用本地方法即可。
    void AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProctoc::AppendEntriesArgs *request,
                       ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done) override;
    void InstallSnapshot(google::protobuf::RpcController *controller,
                         const ::raftRpcProctoc::InstallSnapshotRequest *request,
                         ::raftRpcProctoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done) override;
    void RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                     ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done) override;
    // 初始化函数，设置跟随着、持久化以及消息传递等组件
    void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
              std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

private:
    // for persist

    class BoostPersistRaftNode
    {
    public:
        friend class boost::serialization::access; // 友元类
        // When the class Archive corresponds to an output archive, the
        // & operator is defined similar to <<.  Likewise, when the class Archive
        // is a type of input archive the & operator is defined similar to >>.
        template <class Archive>
        void serialize(Archive &ar, const unsigned int version)
        {
            ar & m_currentTerm;
            ar & m_votedFor;
            ar & m_lastSnapshotIncludeIndex;
            ar & m_lastSnapshotIncludeTerm;
            ar & m_logs;
        }
        int m_currentTerm;
        int m_votedFor;
        int m_lastSnapshotIncludeIndex;
        int m_lastSnapshotIncludeTerm;
        std::vector<std::string> m_logs;
        std::unordered_map<std::string, int> umap;
    };
};

#endif // !RAFT_H
