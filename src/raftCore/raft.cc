#include "raft.hpp"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory> //智能指针
#include "config.hpp"
#include "../public/include/utils.hpp"

// 当 follower 确认自己与 leader 在 (prevLogIndex, prevLogTerm) 处日志连续后，按 Raft 规则把冲突的后缀删掉并与 leader 对齐。
void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply)
{
    std::lock_guard<std::mutex> locker(m_mtx); // 获取锁
    // 不同节点收到AppendEntries后的反应是不同的:注意检查term
    if (args->term() < m_currentTerm)
    {
        reply->set_success(false); // 如果AppendEntries的请求报文中，发送方的任期号小于当前节点，那么返回false
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(-100);
        DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%v}< rf{%d}.term{%d}\n", m_me, args->leaderid(),
                args->term(), m_me, m_currentTerm);
        return;
    }
    DEFER { persist(); }; // 启动持久化

    if (args->term() > m_currentTerm)
    {
        // 如果发送方的任期号更大
        m_status = Follower; // 三变：身份、任期号、投票能力
        m_currentTerm = args->term();
        m_votedFor = -1;
    }
    myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));
    m_status = Follower;
    m_lastResetElectionTime = now(); // 更新选举超时时间
    // DPrintf("[	AppendEntries-func-rf(%v)		] 重置了选举超时定时器\n", rf.me);
    if (args->prevlogindex() > getLastLogIndex())
    {
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(getLastLogIndex() + 1);
        return;
    }
    else if (args->prevlogindex(), m_lastSnapshotIncludeIndex)
    {
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        // 从后往前进行匹配
        reply->set_updatenextindex(
            m_lastSnapshotIncludeIndex + 1);
    }

    // 截断日志
    if (matchLog(args->prevlogindex(), args->prevlogterm()))
    {
        for (int i = 0; i < args->entries_size(); ++i)
        {
            auto log = args->entries(i);
            if (log.logindex() > getLastLogIndex())
            {
                m_logs.push_back(log);
            }
            else
            {
                // 比较是否匹配
                if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() && m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command())
                {
                    myAssert(false, format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                                           " {%d:%d}却不同！！\n",
                                           m_me, log.logindex(), log.logterm(), m_me,
                                           m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(),
                                           log.command()));
                }
                if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm())
                {
                    // 不匹配就更新
                    m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
                }
            }

            if (args->leadercommit() > m_commitIndex)
            {
                m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
                // 这个地方不能无脑跟上getLastLogIndex()，因为可能存在args->leadercommit()落后于 getLastLogIndex()的情况
            }

            // 领导会一次发送完所有的日志
            myAssert(getLastLogIndex() >= m_commitIndex,
                     format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                            getLastLogIndex(), m_commitIndex));
            reply->set_success(true);
            reply->set_term(m_currentTerm);

            //        DPrintf("[func-AppendEntries-rf{%v}] 接收了来自节点{%v}的log，当前lastLogIndex{%v}，返回值：{%v}\n",
            //        rf.me,
            //                args.LeaderId, rf.getLastLogIndex(), reply)

            return;
        }
        myAssert(
            getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
            format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
                   m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));
        if (args->leadercommit() > m_commitIndex)
        {
            m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
            // 这个地方不能无脑跟上getLastLogIndex()，因为可能存在args->leadercommit()落后于 getLastLogIndex()的情况
        }

        // leader会一次发送完所有的日志
        myAssert(getLastLogIndex() >= m_commitIndex,
                 format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                        getLastLogIndex(), m_commitIndex));
        reply->set_success(true);
        reply->set_term(m_currentTerm);
    }
    else
    { // 不匹配 → 计算一个更聪明的 updateNextIndex 让 leader 快速回退
        reply->set_updatenextindex(args->prevlogindex());

        for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index)
        {
            if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex()))
            {
                reply->set_updatenextindex(index + 1);
                break;
            }
        }
        reply->set_success(false);
        reply->set_term(m_currentTerm);
    }
}
// 触发器工作
void Raft::applierTicker()
{
    while (true)
    {
        m_mtx.lock();
        if (m_status == Leader)
        {
            // 如果当前节点为leader
            DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied,
                    m_commitIndex);
        }
        auto applyMsgs = getApplyLogs();
        m_mtx.unlock();
        if (!applyMsgs.empty())
        {
            DPrintf("[func- Raft::applierTicker()-raft{%d}] 向kvserver報告的applyMsgs長度爲：{%d}", m_me, applyMsgs.size());
        }
        for (auto &message : applyMsgs)
        {
            applyChan->Push(message);
        }
        sleepNMilliseconds(ApplyInterval); // sleep 10 ms
    }
}
bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot)
{
    return true;
}

// 发起选举
void Raft::doElection()
{
    if (m_status == Leader)
    {
        // leader节点
        DPrintf("[       ticker-func-rf(%v)              ] is a Leader,wait the  lock\n", m_me);
    }
    else
    {
        DPrintf("[       ticker-func-rf(%d)              ]  选举定时器到期且不是leader，开始选举 \n", m_me);
        // 重竞选超时，term也会增加的
        m_status = Candidate; // 更新身份为候选人
        /// 开始新一轮的选举
        m_currentTerm += 1;                                       // 自增任期号
        m_votedFor = m_me;                                        // 即是自己给自己投，也避免candidate给同辈的candidate投
        persist();                                                // 持久化
        std::shared_ptr<int> votedNum = std::make_shared<int>(1); // 投票数量，使用智能指针
        m_lastResetElectionTime = now();                          // 上一次发起选举的时间，重设
        for (int i = 0; i < m_peers.size(); ++i)
        {
            if (i == m_me)
            {
                continue;
            }
            int lastLogIndex = -1, lastLogTerm = -1;
            getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm); // 获取最后一个log的term和下标
            std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs = std::make_shared<raftRpcProctoc::RequestVoteArgs>();
            // 发起请求
            requestVoteArgs->set_term(m_currentTerm);
            requestVoteArgs->set_candidateid(m_me);
            requestVoteArgs->set_lastlogindex(lastLogIndex);
            requestVoteArgs->set_lastlogterm(lastLogTerm);

            auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();
            std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply, votedNum); // 创建新线程并执行b函数，传参
            t.detach();
        }
    }
}

void Raft::doHeartBeat()
{
    std::lock_guard<std::mutex> g(m_mtx);
    if (m_status == Leader)
    {
        DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n", m_me);
        auto appendNums = std::make_shared<int>(1); // 正确返回的节点的数量
        for (int i = 0; i < m_peers.size(); i++)    // 向所有follower发送请求
        {
            if (i == m_me)
            {
                continue;
            }
            DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", m_me, i);
            myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
            if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex)
            {
                std::thread t(&Raft::leaderSendSnapShot, this, i); // 创建新线程并执行函数，并传递参数
                t.detach();
                continue;
            }
            // 构造请求:向follower发送心跳包
            int preLogIndex = -1;
            int PrevLogTerm = -1;
            getPrevLogInfo(i, &preLogIndex, &PrevLogTerm); // 拿取follower节点的当前日志号和任期号
            // 构造一个空的参数
            std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs = std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
            appendEntriesArgs->set_term(m_currentTerm);
            appendEntriesArgs->set_leaderid(m_me);
            appendEntriesArgs->set_prevlogindex(preLogIndex);
            appendEntriesArgs->set_prevlogterm(PrevLogTerm);
            appendEntriesArgs->clear_entries(); // 清空内容
            appendEntriesArgs->set_leadercommit(m_commitIndex);
            // 前一步的日志号不等于当前的Snapshot日志号
            if (preLogIndex != m_lastSnapshotIncludeIndex)
            {
                for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j)
                {
                    raftRpcProctoc::LogEntry *sendEntryPtr = appendEntriesArgs->add_entries();
                    *sendEntryPtr = m_logs[j];
                }
            }
            else
            {
                for (const auto &item : m_logs)
                {
                    raftRpcProctoc::LogEntry *sendEntryPtr = appendEntriesArgs->add_entries();
                    *sendEntryPtr = item;
                }
            }
            int lastLogIndex = getLastLogIndex();
            myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
                     format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                            appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));
            const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply = std::make_shared<raftRpcProctoc::AppendEntriesReply>();
            appendEntriesReply->set_appstate(Disconnected);
            std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply);
            t.detach();
        }
        m_lastResetHearBeatTime = now(); // 最近心跳时间
    }
}

// 选举超时触发器
void Raft::electionTimeOutTicker()
{
    // 持续存活
    while (true)
    {
        // 如果是Leader节点，那么无需选举，只需要等待一个心跳周期即可
        while (m_status == Leader)
        {
            // POSIX 睡眠（单位微秒）
            usleep(HeartBeatTimeout); // 心跳超时
        }

        std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
        std::chrono::system_clock::time_point wakeTime{};

        {
            // 这里为什么要先拿锁？
            m_mtx.lock();
            wakeTime = now();
            // 计算合适的休眠时间：m_lastResetElectionTime需要并发安全
            suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
            m_mtx.unlock();
        }
        // 等待选举超时
        if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1)
        {
            // 获取当前时间点
            auto start = std::chrono::steady_clock::now();

            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
            // std::this_thread::sleep_for(suitableSleepTime);

            // 获取函数运行结束后的时间点
            auto end = std::chrono::steady_clock::now();

            // 计算时间差并输出结果（单位为毫秒）
            std::chrono::duration<double, std::milli> duration = end - start;

            // 使用ANSI控制序列将输出颜色修改为紫色
            std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                      << std::endl;
            std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                      << std::endl;
        }
        // 未产生选举超时，无需发动选举
        if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0)
        {
            // 说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
            continue;
        }
        doElection();
    }
}

std::vector<ApplyMsg> Raft::getApplyLogs()
{
    // 把“已提交但尚未应用”的日志，打包成要发给状态机（上层服务）的 ApplyMsg 列表，并顺序推进 m_lastApplied
    std::vector<ApplyMsg> applyMsgs;
    myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                        m_me, m_commitIndex, getLastLogIndex()));
    while (m_lastApplied < m_commitIndex)
    {
        ++m_lastApplied;
        myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
                 format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                        m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
        // 构造一个日志
        ApplyMsg applyMsg;
        applyMsg.CommandValid = true; // 命令合法
        applyMsg.SnapshotValid = false;
        applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
        applyMsg.CommandIndex = m_lastApplied;
        applyMsgs.emplace_back(applyMsg);
    }
    return applyMsgs;
}

// 获取新命令应该分配的Index
int Raft::getNewCommandIndex()
{
    //	如果len(logs)==0,就为快照的index+1，否则为log最后一个日志+1
    auto lastLogIndex = getLastLogIndex();
    return lastLogIndex + 1;
}

// leader调用，传入：服务器index，传出：发送的AE的preLogIndex和PrevLogTerm
void Raft::getPrevLogInfo(int server, int *preIndex, int *preTerm)
{
    // logs长度为0返回0,0，不是0就根据nextIndex数组的数值返回
    if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1)
    {
        // 要发送的日志是第一个日志，因此直接返回m_lastSnapshotIncludeIndex和m_lastSnapshotIncludeTerm
        *preIndex = m_lastSnapshotIncludeIndex;
        *preTerm = m_lastSnapshotIncludeTerm;
        return;
    }
    auto nextIndex = m_nextIndex[server];
    *preIndex = nextIndex - 1;
    *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

// 获取节点状态
void Raft::GetState(int *term, bool *isLeader)
{
    m_mtx.lock();
    DEFER
    {
        // todo 暂时不清楚会不会导致死锁
        m_mtx.unlock();
    };

    // Your code here (2A).
    *term = m_currentTerm;
    *isLeader = (m_status == Leader);
}
// --- 快照相关 ---
void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                           raftRpcProctoc::InstallSnapshotResponse *reply)
{
    m_mtx.lock();
    DEFER { m_mtx.unlock(); };
    if (args->term() < m_currentTerm)
    {
        // 如果发送快照请求的节点，任期号小于当前节点
        reply->set_term(m_currentTerm); // 返回任期号
        return;
    }
    if (args->term() > m_currentTerm)
    {
        // 后面两种情况都要接收日志
        m_currentTerm = args->term();
        m_votedFor = -1;
        m_status = Follower;
        persist();//持久化
    }
}