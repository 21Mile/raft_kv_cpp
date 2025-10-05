#include "raft.hpp"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory> //智能指针
#include "config.hpp"
#include "m_utils.hpp"

// 当 follower 确认自己与 leader 在 (prevLogIndex, prevLogTerm) 处日志连续后，按 Raft 规则把冲突的后缀删掉并与 leader 对齐。
void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply)
{
    std::lock_guard<std::mutex> locker(m_mtx); // 获取锁
    DPrintf("[AE<-] F{%d} 收到来自 L{%d} 的AppendEntries: leaderTerm=%d prev[%d/%d] entries=%d leaderCommit=%d | myTerm=%d myState=%d myLastIdx=%d snap[%d/%d]",
            m_me, args->leaderid(), args->term(), args->prevlogindex(), args->prevlogterm(),
            args->entries_size(), args->leadercommit(),
            m_currentTerm, m_status, getLastLogIndex(),
            m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

    // 1) 任期检查
    if (args->term() < m_currentTerm)
    {
        reply->set_success(false); // 如果对方任期号更小，拒绝
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(-100);
        DPrintf("[AE<-] F{%d} 拒绝：Leader{%d} 的 term=%d 小于我方 term=%d（过期）", m_me, args->leaderid(),
                args->term(), m_currentTerm);
        return;
    }
    DEFER
    {
        persist(); // 启动持久化
        DPrintf("[AE<-] F{%d} 处理完毕：持久化完成（term=%d, votedFor=%d, logLen=%d, commit=%d, lastApplied=%d, snapIdx=%d）",
                m_me, m_currentTerm, m_votedFor, (int)m_logs.size(), m_commitIndex, m_lastApplied, m_lastSnapshotIncludeIndex);
    };

    if (args->term() > m_currentTerm)
    {
        // 如果发送方的任期号更大 -> 立刻转Follower，重置投票
        m_status = Follower;
        m_currentTerm = args->term();
        m_votedFor = -1;
        DPrintf("[AE<-] F{%d} 发现更高任期：降级为Follower，新term=%d，votedFor=%d", m_me, m_currentTerm, m_votedFor);
    }
    myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));

    // 2) 接收心跳/日志视为与Leader保持联系，重置选举超时
    m_status = Follower;
    m_lastResetElectionTime = now();
    DPrintf("[AE<-] F{%d} 重置选举超时计时器（保持和Leader连接）", m_me);

    // 3) prevLogIndex 边界检查
    if (args->prevlogindex() > getLastLogIndex())
    {
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(getLastLogIndex() + 1);
        DPrintf("[AE<-] F{%d} 日志过短：prevIdx=%d > myLastIdx=%d，建议nextIndex=%d",
                m_me, args->prevlogindex(), getLastLogIndex(), getLastLogIndex() + 1);
        return;
    }
    else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) // 注意：这里原代码使用了“逗号运算符”，常见手误；通常期待的是 <= 判断
    {
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        // 从后往前进行匹配
        reply->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);
        DPrintf("[AE<-] F{%d} prevIdx=%d <= snapIdx=%d（受快照影响），建议nextIndex=%d",
                m_me, args->prevlogindex(), m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeIndex + 1);
    }

    // 4) 核心：匹配 (prevIndex, prevTerm)
    if (matchLog(args->prevlogindex(), args->prevlogterm()))
    {
        DPrintf("[AE<-] F{%d} 通过前缀匹配：prev=%d/%d，准备合并 entries=%d（当前 lastIdx=%d）",
                m_me, args->prevlogindex(), args->prevlogterm(), args->entries_size(), getLastLogIndex());

        // 逐条合并（不匹配则覆盖）
        for (int i = 0; i < args->entries_size(); ++i)
        {
            auto log = args->entries(i);
            if (log.logindex() > getLastLogIndex())
            {
                m_logs.push_back(log);
                DPrintf("[AE<-] F{%d} 追加新日志：idx=%d term=%d（扩大日志尾部）", m_me, log.logindex(), log.logterm());
            }
            else
            {
                int si = getSlicesIndexFromLogIndex(log.logindex());
                int oldTerm = m_logs[si].logterm();
                if (oldTerm != log.logterm())
                {
                    DPrintf("[AE<-] F{%d} 覆盖冲突日志：idx=%d oldTerm=%d -> newTerm=%d（以Leader为准）",
                            m_me, log.logindex(), oldTerm, log.logterm());
                    m_logs[si] = log;
                }
                else
                {
                    // term 相同，再次确认 command 是否一致（严格一致性）
                    if (m_logs[si].command() != log.command())
                    {
                        myAssert(false, format(
                                            "[AE<-] F{%d} 致命：相同 idx/term 但 command 不一致！ idx=%d term=%d mine=%s leader=%s",
                                            m_me, log.logindex(), log.logterm(),
                                            m_logs[si].command().c_str(), log.command().c_str()));
                    }
                    DPrintf("[AE<-] F{%d} 跳过：idx=%d term相同且command一致，无需写入", m_me, log.logindex());
                }
            }
        }

        // 5) 跟进提交点（以Leader的commit为上限，注意不能超过我方 lastIdx）
        if (args->leadercommit() > m_commitIndex)
        {
            int newCommit = std::min(args->leadercommit(), getLastLogIndex());
            DPrintf("[AE<-] F{%d} 更新提交点：commit %d -> %d（受Leader影响，不能超过本地lastIdx）",
                    m_me, m_commitIndex, newCommit);
            m_commitIndex = newCommit;
        }

        // 6) 安全性检查
        myAssert(getLastLogIndex() >= m_commitIndex,
                 format("[AppendEntries1 F{%d}] lastIdx{%d} < commitIndex{%d}", m_me, getLastLogIndex(), m_commitIndex));

        reply->set_success(true);
        reply->set_term(m_currentTerm);
        DPrintf("[AE<-] F{%d} 成功处理AE：lastIdx=%d commit=%d（将触发后台applier）",
                m_me, getLastLogIndex(), m_commitIndex);
        return;
    }
    else
    {
        // 不匹配 → 提供“更聪明”的回退点（term 回退到前一个 term 的起始位置）
        int suggestNext = args->prevlogindex();
        for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index)
        {
            if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex()))
            {
                suggestNext = index + 1;
                break;
            }
        }
        reply->set_updatenextindex(suggestNext);
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        DPrintf("[AE<-] F{%d} prev不匹配：建议Leader回退 nextIndex=%d（term 对齐后再发）", m_me, suggestNext);
    }
}

// 触发器工作：从提交点把日志应用到状态机
void Raft::applierTicker()
{
    DPrintf("[APPLY] 线程启动：负责把 commitIndex>lastApplied 的日志推送到上层状态机");
    while (true)
    {
        m_mtx.lock();
        if (m_status == Leader)
        {
            DPrintf("[APPLY] raft{%d} 作为Leader：lastApplied=%d commitIndex=%d", m_me, m_lastApplied, m_commitIndex);
        }
        auto applyMsgs = getApplyLogs();
        m_mtx.unlock();

        if (!applyMsgs.empty())
        {
            DPrintf("[APPLY] raft{%d} 即将上报ApplyMsg条数：%d（按序推送到KV）", m_me, (int)applyMsgs.size());
        }
        for (auto &message : applyMsgs)
        {
            // 可选：这里也可以打印每条消息的 index/命令长度，日志量大时可关掉
            DPrintf("[APPLY] raft{%d} 上报：index=%d cmdLen=%d", m_me, message.CommandIndex, (int)message.Command.size());
            applyChan->Push(message);
        }
        sleepNMilliseconds(ApplyInterval); // sleep 10 ms
    }
}

bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot)
{
    DPrintf("[Snap] CondInstallSnapshot(term=%d, idx=%d) 被调用（占位实现，直接返回true）", lastIncludedTerm, lastIncludedIndex);
    return true;
}

// 发起选举
void Raft::doElection()
{
    if (m_status == Leader)
    {
        DPrintf("[ELECT] raft{%d} 已经是Leader，跳过本轮选举", m_me);
        return;
    }

    DPrintf("[ELECT] raft{%d} 选举超时到期，开始发起选举 | 原term=%d", m_me, m_currentTerm);
    m_status = Candidate;
    m_currentTerm += 1;
    m_votedFor = m_me;
    persist();

    std::shared_ptr<int> votedNum = std::make_shared<int>(1); // 自己的一票
    m_lastResetElectionTime = now();

    int myLastLogIndex = -1, myLastLogTerm = -1;
    getLastLogIndexAndTerm(&myLastLogIndex, &myLastLogTerm);
    DPrintf("[ELECT] raft{%d} 竞选参数：term=%d lastLog=[%d/%d] peers=%d",
            m_me, m_currentTerm, myLastLogIndex, myLastLogTerm, (int)m_peers.size());

    for (int i = 0; i < m_peers.size(); ++i)
    {
        if (i == m_me)
            continue;

        auto args = std::make_shared<raftRpcProctoc::RequestVoteArgs>();
        args->set_term(m_currentTerm);
        args->set_candidateid(m_me);
        args->set_lastlogindex(myLastLogIndex);
        args->set_lastlogterm(myLastLogTerm);

        auto reply = std::make_shared<raftRpcProctoc::RequestVoteReply>();
        DPrintf("[ELECT] raft{%d}->节点{%d} 发送请求投票", m_me, i);
        std::thread t(&Raft::sendRequestVote, this, i, args, reply, votedNum);
        t.detach();
    }
}

// 选举超时触发器
void Raft::electionTimeOutTicker()
{
    DPrintf("[ELECT] 选举超时线程启动");
    while (true)
    {
        while (m_status == Leader)
        {
            usleep(HeartBeatTimeout); // Leader 不需要选举，只保持心跳
        }

        std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
        std::chrono::system_clock::time_point wakeTime{};

        {
            m_mtx.lock();
            wakeTime = now();
            suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
            m_mtx.unlock();
        }
        if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1)
        {
            auto start = std::chrono::steady_clock::now();
            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
            auto end = std::chrono::steady_clock::now();
            std::chrono::duration<double, std::milli> duration = end - start;

            DPrintf("[ELECT] 休眠等待超时：设置=%.1fms 实际=%.1fms",
                    std::chrono::duration<double, std::milli>(suitableSleepTime).count(), duration.count());
        }
        // 如果期间被重置了，就继续下一轮
        if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0)
        {
            DPrintf("[ELECT] 睡眠期间收到心跳/重置定时器，本轮不选举");
            continue;
        }
        doElection();
    }
}

// --- 心跳保活类函数 ---
void Raft::doHeartBeat()
{
    std::lock_guard<std::mutex> g(m_mtx);
    if (m_status == Leader)
    {
        DPrintf("[HB] Leader{%d} 心跳触发：准备向所有Follower发送 AE（commit=%d lastIdx=%d）", m_me, m_commitIndex, getLastLogIndex());
        auto appendNums = std::make_shared<int>(1); // 统计成功副本
        for (int i = 0; i < m_peers.size(); i++)
        {
            if (i == m_me)
                continue;

            myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
            if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex)
            {
                DPrintf("[HB] Leader{%d}->节点{%d}: nextIndex=%d <= snapIdx=%d，改为发送快照",
                        m_me, i, m_nextIndex[i], m_lastSnapshotIncludeIndex);
                std::thread t(&Raft::leaderSendSnapShot, this, i);
                t.detach();
                continue;
            }

            int preLogIndex = -1, prevLogTerm = -1;
            getPrevLogInfo(i, &preLogIndex, &prevLogTerm);

            auto args = std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
            args->set_term(m_currentTerm);
            args->set_leaderid(m_me);
            args->set_prevlogindex(preLogIndex);
            args->set_prevlogterm(prevLogTerm);
            args->clear_entries();
            args->set_leadercommit(m_commitIndex);

            if (preLogIndex != m_lastSnapshotIncludeIndex)
            {
                for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j)
                {
                    auto *e = args->add_entries();
                    *e = m_logs[j];
                }
            }
            else
            {
                for (const auto &item : m_logs)
                {
                    auto *e = args->add_entries();
                    *e = item;
                }
            }

            int lastLogIndex = getLastLogIndex();
            myAssert(args->prevlogindex() + args->entries_size() == lastLogIndex,
                     format("appendEntriesArgs.PrevLogIndex{%d}+len(Entries){%d} != lastLogIndex{%d}",
                            args->prevlogindex(), args->entries_size(), lastLogIndex));

            auto reply = std::make_shared<raftRpcProctoc::AppendEntriesReply>();
            reply->set_appstate(Disconnected);

            DPrintf("[HB] Leader{%d}->节点{%d}: 发AE prev=[%d/%d] entries=%d leaderCommit=%d",
                    m_me, i, preLogIndex, prevLogTerm, args->entries_size(), m_commitIndex);

            auto aux = std::make_shared<int>(0);
            std::thread t(&Raft::sendAppendEntries, this, i, args, reply, aux);
            t.detach();
        }
        m_lastResetHearBeatTime = now();
    }
}

void Raft::leaderHearBeatTicker()
{
    DPrintf("[HB] 心跳线程启动");
    while (true)
    {
        while (m_status != Leader)
        {
            usleep(1000 * HeartBeatTimeout);
        }
        static std::atomic<int32_t> atomicCount{0};
        std::chrono::nanoseconds suitableSleepTime{};
        std::chrono::system_clock::time_point wakeTime{};
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            wakeTime = now();
            suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - wakeTime;
        }
        if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1)
        {
            DPrintf("[HB] 预定心跳间隔：%lldms（可能被写入/选举重置）",
                    (long long)std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count());
            auto start = std::chrono::steady_clock::now();
            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
            auto end = std::chrono::steady_clock::now();
            (void)start;
            (void)end;
            atomicCount.fetch_add(1);
        }
        if (std::chrono::duration<double, std::milli>(m_lastResetHearBeatTime - wakeTime).count() > 0)
        {
            DPrintf("[HB] 期间发生重置（可能是刚刚发过心跳/有新提交），略过一轮");
            continue;
        }
        doHeartBeat();
    }
}

// --- 应用日志类 ----

// 把“已提交但尚未应用”的日志，打包成要发给状态机的 ApplyMsg 列表
std::vector<ApplyMsg> Raft::getApplyLogs()
{
    std::vector<ApplyMsg> applyMsgs;
    myAssert(m_commitIndex <= getLastLogIndex(), format("[getApplyLogs F{%d}] commitIndex{%d} > lastLogIndex{%d}",
                                                        m_me, m_commitIndex, getLastLogIndex()));

    while (m_lastApplied < m_commitIndex)
    {
        ++m_lastApplied;
        myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
                 format("rf.logs[realIdx(lastApplied)].LogIndex{%d} != lastApplied{%d} ",
                        m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));

        ApplyMsg applyMsg;
        applyMsg.CommandValid = true;
        applyMsg.SnapshotValid = false;
        applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
        applyMsg.CommandIndex = m_lastApplied;

        applyMsgs.emplace_back(applyMsg);
        DPrintf("[APPLY] raft{%d} 组装ApplyMsg：index=%d cmdLen=%d（lastApplied推进）",
                m_me, applyMsg.CommandIndex, (int)applyMsg.Command.size());
    }
    return applyMsgs;
}

// 获取新命令应该分配的Index
int Raft::getNewCommandIndex()
{
    auto lastLogIndex = getLastLogIndex();
    DPrintf("[Start] 分配新命令索引：lastLogIndex=%d -> newIndex=%d", lastLogIndex, lastLogIndex + 1);
    return lastLogIndex + 1;
}

// leader调用，传出 follower 的 prev 信息
void Raft::getPrevLogInfo(int server, int *preIndex, int *preTerm)
{
    if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1)
    {
        *preIndex = m_lastSnapshotIncludeIndex;
        *preTerm = m_lastSnapshotIncludeTerm;
        DPrintf("[HB] 计算 prevInfo: 节点{%d} nextIndex==snapIdx+1 -> prev=[%d/%d]",
                server, *preIndex, *preTerm);
        return;
    }
    auto nextIndex = m_nextIndex[server];
    *preIndex = nextIndex - 1;
    *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
    DPrintf("[HB] 计算 prevInfo: 节点{%d} nextIndex=%d -> prev=[%d/%d]", server, nextIndex, *preIndex, *preTerm);
}

// 获取节点状态
void Raft::GetState(int *term, bool *isLeader)
{
    m_mtx.lock();
    DEFER { m_mtx.unlock(); };

    *term = m_currentTerm;
    *isLeader = (m_status == Leader);
    DPrintf("[State] raft{%d} GetState: term=%d isLeader=%d", m_me, *term, *isLeader);
}

// --- 快照相关 ---
void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                           raftRpcProctoc::InstallSnapshotResponse *reply)
{
    m_mtx.lock();
    DEFER { m_mtx.unlock(); };
    DPrintf("[Snap] F{%d} 收到 InstallSnapshot: leader=%d term=%d snapIdx=%d snapTerm=%d",
            m_me, args->leaderid(), args->term(), args->lastsnapshotincludeindex(), args->lastsnapshotincludeterm());

    if (args->term() < m_currentTerm)
    {
        reply->set_term(m_currentTerm);
        DPrintf("[Snap] F{%d} 拒绝快照：对方term=%d < 我方term=%d", m_me, args->term(), m_currentTerm);
        return;
    }
    if (args->term() > m_currentTerm)
    {
        m_currentTerm = args->term();
        m_votedFor = -1;
        m_status = Follower;
        persist();
        DPrintf("[Snap] F{%d} 发现更高任期：更新为 term=%d 并转Follower", m_me, m_currentTerm);
    }
    m_status = Follower;
    m_lastResetElectionTime = now();
    if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex)
    {
        DPrintf("[Snap] F{%d} 忽略：对方snapIdx=%d <= 我方当前snapIdx=%d，无需替换",
                m_me, args->lastsnapshotincludeindex(), m_lastSnapshotIncludeIndex);
        return;
    }

    auto lastLogIndex = getLastLogIndex();
    if (lastLogIndex > args->lastsnapshotincludeindex())
    {
        m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
        DPrintf("[Snap] F{%d} 裁剪本地日志到 snapIdx=%d（保留后缀）", m_me, args->lastsnapshotincludeindex());
    }
    else
    {
        m_logs.clear();
        DPrintf("[Snap] F{%d} 本地日志全部清空（完全由快照接管）", m_me);
    }

    m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
    m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
    m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
    m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();

    reply->set_term(m_currentTerm);

    ApplyMsg msg;
    msg.SnapshotValid = true;
    msg.Snapshot = args->data();
    msg.SnapshotTerm = args->lastsnapshotincludeterm();
    msg.SnapshotIndex = args->lastsnapshotincludeindex();

    std::thread t(&Raft::pushMsgToKvServer, this, msg);
    t.detach();

    m_persister->Save(persistData(), args->data());
    DPrintf("[Snap] F{%d} 接受快照并持久化：snapIdx=%d term=%d commit=%d lastApplied=%d",
            m_me, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm, m_commitIndex, m_lastApplied);
}

// --- leader行为函数 ----

// leader发送快照到follower
void Raft::leaderSendSnapShot(int server)
{
    m_mtx.lock();
    DPrintf("[Snap] Leader{%d}->节点{%d} 准备发送快照：snapIdx=%d snapTerm=%d", m_me, server, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

    raftRpcProctoc::InstallSnapshotRequest args;
    args.set_leaderid(m_me);
    args.set_term(m_currentTerm);
    args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
    args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
    args.set_data(m_persister->ReadSnapshot());

    raftRpcProctoc::InstallSnapshotResponse reply;
    m_mtx.unlock();

    bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
    m_mtx.lock();
    DEFER { m_mtx.unlock(); };

    if (!ok)
    {
        DPrintf("[Snap] Leader{%d}->节点{%d} 发送快照RPC失败（网络/下线）", m_me, server);
        return;
    }
    if (m_status != Leader || m_currentTerm != args.term())
    {
        DPrintf("[Snap] Leader{%d}->节点{%d} 响应抵达但我不再是Leader或term变化，忽略", m_me, server);
        return;
    }
    if (reply.term() > m_currentTerm)
    {
        m_currentTerm = reply.term();
        m_votedFor = -1;
        m_status = Follower;
        persist();
        m_lastResetElectionTime = now();
        DPrintf("[Snap] Leader{%d} 收到更高term的快照响应，降为Follower term=%d", m_me, m_currentTerm);
        return;
    }

    m_matchIndex[server] = args.lastsnapshotincludeindex();
    m_nextIndex[server] = m_matchIndex[server] + 1;
    DPrintf("[Snap] Leader{%d} 更新节点{%d} 复制进度：match=%d next=%d",
            m_me, server, m_matchIndex[server], m_nextIndex[server]);
}

// 领导者节点更新日志提交的序号（并发安全）
void Raft::leaderUpdateCommitIndex()
{
    for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex + 1; index--)
    {
        int quorum = 0;
        for (int i = 0; i < m_peers.size(); ++i)
        {
            if (i == m_me || m_matchIndex[i] >= index)
                quorum++;
        }
        if (quorum >= (int)m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm)
        {
            if (m_commitIndex != index)
            {
                DPrintf("[Commit] Leader{%d} 推进commitIndex：%d -> %d（多数派匹配且属于当前任期）", m_me, m_commitIndex, index);
            }
            m_commitIndex = index;
            break;
        }
    }
}

// --- 工具函数 ----

void Raft::pushMsgToKvServer(ApplyMsg msg)
{
    DPrintf("[APPLY] pushMsgToKvServer: SnapshotValid=%d CommandValid=%d Index=%d",
            msg.SnapshotValid, msg.CommandValid, msg.SnapshotIndex ? msg.SnapshotIndex : msg.CommandIndex);
    applyChan->Push(msg);
}

// 进来前要保证logIndex是存在的，即≥rf.lastSnapshotIncludeIndex，而且≤rf.getLastLogIndex()
bool Raft::matchLog(int logIndex, int logTerm)
{
    myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
             format("不满足：logIndex{%d}>=snapIdx{%d} 且 logIndex{%d}<=lastIdx{%d}",
                    logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
    bool ok = (logTerm == getLogTermFromLogIndex(logIndex));
    DPrintf("[AE<-] matchLog 检查：idx=%d 需要term=%d 实际term=%d 结果=%d",
            logIndex, logTerm, getLogTermFromLogIndex(logIndex), ok);
    return ok;
}

void Raft::persist()
{
    auto data = persistData();
    DPrintf("[Persist] raft{%d} Get persist data:{%s}", m_me, data.c_str());
    m_persister->SaveRaftState(data); // 持久化到磁盘
    DPrintf("[Persist] raft{%d} SaveRaftState：term=%d votedFor=%d logLen=%d snapIdx=%d",
            m_me, m_currentTerm, m_votedFor, (int)m_logs.size(), m_lastSnapshotIncludeIndex);
}

// --- 投票机制 ---
void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply)
{
    std::lock_guard<std::mutex> lg(m_mtx);
    DPrintf("[Vote<-] 收到投票请求：from C{%d} term=%d lastLog=[%d/%d] | myTerm=%d myVotedFor=%d myLastLog=[%d/%d]",
            args->candidateid(), args->term(), args->lastlogindex(), args->lastlogterm(),
            m_currentTerm, m_votedFor, getLastLogIndex(), getLastLogTerm());

    DEFER
    {
        persist(); // 保证任期/投票等关键状态落盘
        DPrintf("[Vote<-] 处理投票请求完成：term=%d votedFor=%d", m_currentTerm, m_votedFor);
    };

    if (args->term() < m_currentTerm)
    {
        reply->set_term(m_currentTerm);
        reply->set_votegranted(false);
        reply->set_votestate(Expire);
        DPrintf("[Vote<-] 拒绝：请求term过期（%d < %d）", args->term(), m_currentTerm);
        return;
    }

    if (args->term() > m_currentTerm)
    {
        m_status = Follower;
        m_currentTerm = args->term();
        m_votedFor = -1;
        DPrintf("[Vote<-] 发现更高任期：转Follower，term=%d，重置votedFor", m_currentTerm);
    }
    myAssert(args->term() == m_currentTerm, format("[Vote<-] term对齐后仍不相等"));

    // 候选者必须“足够新”
    if (!UpToDate(args->lastlogindex(), args->lastlogterm()))
    {
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);
        DPrintf("[Vote<-] 拒绝：候选者日志不够新（对比我方 last=[%d/%d]）", getLastLogIndex(), getLastLogTerm());
        return;
    }

    if (m_votedFor != -1 && m_votedFor != args->candidateid())
    {
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);
        DPrintf("[Vote<-] 拒绝：本任期已投给 %d", m_votedFor);
        return;
    }

    // 同意投票
    m_votedFor = args->candidateid();
    m_lastResetElectionTime = now();
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);
    DPrintf("[Vote<-] 同意投票给 C{%d}（term=%d）", m_votedFor, m_currentTerm);
}

// 工具函数：判断日志是否“新”
bool Raft::UpToDate(int index, int term)
{
    int lastIndex = -1, lastTerm = -1;
    getLastLogIndexAndTerm(&lastIndex, &lastTerm);
    bool ok = (term > lastTerm) || (term == lastTerm && index >= lastIndex);
    DPrintf("[Vote<-] UpToDate? candidate=[%d/%d] my=[%d/%d] -> %d",
            index, term, lastIndex, lastTerm, ok);
    return ok;
}

void Raft::getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm)
{
    if (m_logs.empty())
    {
        *lastLogIndex = m_lastSnapshotIncludeIndex;
        *lastLogTerm = m_lastSnapshotIncludeTerm;
    }
    else
    {
        *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
        *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
    }
}

int Raft::getLastLogIndex()
{
    int lastLogIndex = -1, _;
    getLastLogIndexAndTerm(&lastLogIndex, &_);
    return lastLogIndex;
}

int Raft::getLastLogTerm()
{
    int _, lastLogTerm = -1;
    getLastLogIndexAndTerm(&_, &lastLogTerm);
    return lastLogTerm;
}

int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

// 给定“全局日志下标 logIndex”，返回该日志条目的 term（兼顾快照映射）
int Raft::getLogTermFromLogIndex(int logIndex)
{
    myAssert(logIndex >= m_lastSnapshotIncludeIndex,
             format("[getLogTermFromLogIndex F{%d}] index{%d} < snapIdx{%d}", m_me, logIndex, m_lastSnapshotIncludeIndex));
    int lastLogIndex = getLastLogIndex();
    myAssert(logIndex <= lastLogIndex, format("[getLogTermFromLogIndex F{%d}] logIndex{%d} > lastLogIndex{%d}",
                                              m_me, logIndex, lastLogIndex));
    if (logIndex == m_lastSnapshotIncludeIndex)
        return m_lastSnapshotIncludeTerm;
    return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
}

// 找到index对应的真实下标位置（去掉快照偏移）
int Raft::getSlicesIndexFromLogIndex(int logIndex)
{
    myAssert(logIndex > m_lastSnapshotIncludeIndex,
             format("[getSlicesIndexFromLogIndex F{%d}] index{%d} <= snapIdx{%d}", m_me, logIndex, m_lastSnapshotIncludeIndex));
    int lastLogIndex = getLastLogIndex();
    myAssert(logIndex <= lastLogIndex, format("[getSlicesIndexFromLogIndex F{%d}] logIndex{%d} > lastLogIndex{%d}",
                                              m_me, logIndex, lastLogIndex));
    int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
    return SliceIndex;
}

// 发送投票请求（Candidate->Peers）
bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args, std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum)
{
    auto start = now();
    DPrintf("[ELECT] 向节点{%d} 发送投票RPC：term=%d lastLog=[%d/%d]", server, args->term(), args->lastlogindex(), args->lastlogterm());
    bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
    if (!ok)
    {
        DPrintf("[ELECT] 节点{%d} 投票RPC失败（网络/离线）", server);
        return ok;
    }
    std::lock_guard<std::mutex> lg(m_mtx);
    if (reply->term() > m_currentTerm)
    {
        m_status = Follower;
        m_currentTerm = reply->term();
        m_votedFor = -1;
        persist();
        DPrintf("[ELECT] 收到更高term的投票响应：转Follower term=%d", m_currentTerm);
        return true;
    }
    else if (reply->term() < m_currentTerm)
    {
        DPrintf("[ELECT] 丢弃过期投票响应：reply.term=%d < my.term=%d", reply->term(), m_currentTerm);
        return true;
    }
    myAssert(reply->term() == m_currentTerm, format("assert {reply.Term}==rf.currentTerm fail"));

    if (!reply->votegranted())
    {
        DPrintf("[ELECT] 节点{%d} 拒绝投票", server);
        return true;
    }

    ++(*votedNum);
    DPrintf("[ELECT] 节点{%d} 同意投票，累计票数=%d / 需要>%d", server, *votedNum, (int)m_peers.size() / 2);
    if (*votedNum >= m_peers.size() / 2 + 1)
    {
        *votedNum = 0;
        if (m_status == Leader)
        {
            DPrintf("[ELECT] 已是Leader，忽略重复就任");
            return true;
        }
        m_status = Leader;
        int lastLogIndex = getLastLogIndex();
        for (int i = 0; i < m_nextIndex.size(); ++i)
        {
            m_nextIndex[i] = lastLogIndex + 1;
            m_matchIndex[i] = 0;
        }
        DPrintf("[ELECT] raft{%d} 当选为Leader：term=%d lastIdx=%d，立即广播心跳", m_me, m_currentTerm, lastLogIndex);
        std::thread t(&Raft::doHeartBeat, this);
        t.detach();
        persist();
    }
    return true;
}

// Leader 向 Follower 发送 AppendEntries
bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
                             std::shared_ptr<int> appendNums)
{
    DPrintf("[AE->] Leader{%d}->节点{%d} 发送AE：prev=[%d/%d] entries=%d leaderCommit=%d",
            m_me, server, args->prevlogindex(), args->prevlogterm(), args->entries_size(), args->leadercommit());

    bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());
    if (!ok)
    {
        DPrintf("[AE->] Leader{%d}->节点{%d} AE RPC失败（网络/离线）", m_me, server);
        return ok;
    }
    if (reply->appstate() == Disconnected)
    {
        DPrintf("[AE->] Leader{%d}->节点{%d} 返回 Disconnected（视为失败）", m_me, server);
        return ok;
    }

    std::lock_guard<std::mutex> lg1(m_mtx);

    if (reply->term() > m_currentTerm)
    {
        m_status = Follower;
        m_currentTerm = reply->term();
        m_votedFor = -1;
        DPrintf("[AE->] Leader{%d} 收到更高term的AE回复：降为Follower term=%d", m_me, m_currentTerm);
        return ok;
    }
    else if (reply->term() < m_currentTerm)
    {
        DPrintf("[AE->] Leader{%d}->节点{%d} 回复term过期（%d < %d），忽略", m_me, server, reply->term(), m_currentTerm);
        return ok;
    }

    if (m_status != Leader)
    {
        DPrintf("[AE->] 我不再是Leader，忽略此AE回复");
        return ok;
    }

    myAssert(reply->term() == m_currentTerm,
             format("reply.Term{%d} != rf.currentTerm{%d}", reply->term(), m_currentTerm));

    if (!reply->success())
    {
        if (reply->updatenextindex() != -100)
        {
            DPrintf("[AE->] 节点{%d} 报告不匹配，回退 nextIndex[%d] -> %d",
                    server, server, reply->updatenextindex());
            m_nextIndex[server] = reply->updatenextindex();
        }
    }
    else
    {
        ++(*appendNums);
        m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
        m_nextIndex[server] = m_matchIndex[server] + 1;

        int lastLogIndex = getLastLogIndex();
        myAssert(m_nextIndex[server] <= lastLogIndex + 1,
                 format("rf.nextIndex[%d] > lastLogIndex+1, logLen=%d lastIdx=%d", server, (int)m_logs.size(), lastLogIndex));

        DPrintf("[AE->] 节点{%d} AE成功：match=%d next=%d 本轮成功计数=%d",
                server, m_matchIndex[server], m_nextIndex[server], *appendNums);

        if (*appendNums >= 1 + m_peers.size() / 2)
        {
            *appendNums = 0;
            if (args->entries_size() > 0)
            {
                DPrintf("[AE->] 本轮有日志复制成功：最后一条的term=%d 当前term=%d",
                        args->entries(args->entries_size() - 1).logterm(), m_currentTerm);
            }
            if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm)
            {
                int newCommit = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());
                if (newCommit != m_commitIndex)
                {
                    DPrintf("[Commit] Leader{%d} 推进 commitIndex: %d -> %d（当前任期的日志达到多数）",
                            m_me, m_commitIndex, newCommit);
                }
                m_commitIndex = newCommit;
            }
        }
    }
    return ok;
}

// --- rpc controller层 ---
// 请求层次： appendentries -> appendentries1 -> sendappendentries(leader)
// rpc层次： rpccontroller -> req,resp ->具体的请求执行
void Raft::AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProctoc::AppendEntriesArgs *request, ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done)
{
    DPrintf("[RPC] AppendEntries 入口：来自 L{%d} term=%d prev=[%d/%d] entries=%d leaderCommit=%d",
            request->leaderid(), request->term(), request->prevlogindex(), request->prevlogterm(),
            request->entries_size(), request->leadercommit());
    AppendEntries1(request, response);
    DPrintf("[RPC] AppendEntries 返回：success=%d term=%d adviseNext=%d", response->success(), response->term(), response->updatenextindex());
    done->Run();
}

void Raft::InstallSnapshot(google::protobuf::RpcController *controller, const ::raftRpcProctoc::InstallSnapshotRequest *request, ::raftRpcProctoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done)
{
    DPrintf("[RPC] InstallSnapshot 入口：from L{%d} term=%d snapIdx=%d",
            request->leaderid(), request->term(), request->lastsnapshotincludeindex());
    InstallSnapshot(request, response);
    DPrintf("[RPC] InstallSnapshot 返回：term=%d", response->term());
    done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                       ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done)
{
    DPrintf("[RPC] RequestVote 入口：C{%d} term=%d lastLog=[%d/%d]",
            request->candidateid(), request->term(), request->lastlogindex(), request->lastlogterm());
    RequestVote(request, response);
    DPrintf("[RPC] RequestVote 返回：granted=%d term=%d state=%d",
            response->votegranted(), response->term(), response->votestate());
    done->Run();
}

// raft 节点 处理一个操作的函数（start = 客户端命令注入）
void Raft::Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader)
{
    std::lock_guard<std::mutex> lg(m_mtx);
    if (m_status != Leader)
    {
        DPrintf("[Start] raft{%d} 不是Leader，拒绝接收客户端指令", m_me);
        *newLogIndex = -1;
        *newLogTerm = -1;
        *isLeader = false;
        return;
    }

    raftRpcProctoc::LogEntry newLogEntry;
    newLogEntry.set_command(command.asString());
    newLogEntry.set_logterm(m_currentTerm);
    newLogEntry.set_logindex(getNewCommandIndex());
    m_logs.emplace_back(newLogEntry);

    int lastLogIndex = getLastLogIndex();
    DPrintf("[Start] Leader{%d} 收到新命令：分配logIndex=%d term=%d，当前lastIdx=%d（等待心跳批量复制）",
            m_me, newLogEntry.logindex(), newLogEntry.logterm(), lastLogIndex);

    persist();

    *newLogIndex = newLogEntry.logindex();
    *newLogTerm = newLogEntry.logterm();
    *isLeader = true;
}

// 初始化
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh)
{
    m_peers = peers;
    m_persister = persister;
    m_me = me;

    std::unique_lock<std::mutex> lock(m_mtx);
    this->applyChan = applyCh;
    m_currentTerm = 0;
    m_status = Follower;
    m_commitIndex = 0;
    m_lastApplied = 0;
    m_logs.clear();
    m_matchIndex.clear();
    m_nextIndex.clear();

    for (int i = 0; i < m_peers.size(); ++i)
    {
        m_matchIndex.push_back(0);
        m_nextIndex.push_back(0);
    }
    m_votedFor = -1;

    m_lastSnapshotIncludeIndex = 0;
    m_lastSnapshotIncludeTerm = 0;
    m_lastResetElectionTime = now();
    m_lastResetHearBeatTime = now();

    DPrintf("[INIT] raft{%d} 初始化：peers=%d 初始term=%d state=Follower", m_me, (int)m_peers.size(), m_currentTerm);

    readPersist(m_persister->ReadRaftState());
    if (m_lastSnapshotIncludeIndex > 0)
    {
        m_lastApplied = m_lastSnapshotIncludeIndex;
    }
    DPrintf("[INIT] raft{%d} 载入持久化：term=%d votedFor=%d logLen=%d snapIdx=%d commit=%d lastApplied=%d",
            m_me, m_currentTerm, m_votedFor, (int)m_logs.size(), m_lastSnapshotIncludeIndex, m_commitIndex, m_lastApplied);

    lock.unlock();

    m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);
    m_ioManager->scheduler([this]() -> void
                           { this->leaderHearBeatTicker(); });
    m_ioManager->scheduler([this]() -> void
                           { this->electionTimeOutTicker(); });
    std::thread t3(&Raft::applierTicker, this);
    t3.detach();

    DPrintf("[INIT] raft{%d} 后台线程已启动：心跳、选举超时、应用器", m_me);
}

// 持久化数据打包

std::string Raft::persistData()
{
    BoostPersistRaftNode boostPersistRaftNode;
    boostPersistRaftNode.m_currentTerm = m_currentTerm;
    boostPersistRaftNode.m_votedFor = m_votedFor;
    boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
    boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
    for (auto &item : m_logs)
    {
        boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
    }

    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << boostPersistRaftNode;
    return ss.str();
}
void Raft::readPersist(std::string data)
{
    if (data.empty())
    {
        DPrintf("[Persist] 无历史持久化数据，使用默认初始状态");
        return;
    }
    std::stringstream iss(data);
    boost::archive::text_iarchive ia(iss);
    BoostPersistRaftNode bpr;
    ia >> bpr;

    m_currentTerm = bpr.m_currentTerm;
    m_votedFor = bpr.m_votedFor;
    m_lastSnapshotIncludeIndex = bpr.m_lastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = bpr.m_lastSnapshotIncludeTerm;

    m_logs.clear();
    for (auto &item : bpr.m_logs)
    {
        raftRpcProctoc::LogEntry logEntry;
        logEntry.ParseFromString(item);
        m_logs.emplace_back(logEntry);
    }
    DPrintf("[Persist] 读取完成：term=%d votedFor=%d logLen=%d snapIdx=%d",
            m_currentTerm, m_votedFor, (int)m_logs.size(), m_lastSnapshotIncludeIndex);
}

// 快照机制
void Raft::Snapshot(int index, std::string snapshot)
{
    std::lock_guard<std::mutex> lg(m_mtx);

    DPrintf("[Snap] 请求生成本地快照：index=%d 当前 snapIdx=%d commit=%d lastApplied=%d",
            index, m_lastSnapshotIncludeIndex, m_commitIndex, m_lastApplied);

    if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex)
    {
        DPrintf("[Snap] 拒绝生成：index不在允许范围（必须 snapIdx<index<=commit）");
        return;
    }

    auto lastLogIndex = getLastLogIndex();
    int newIdx = index;
    int newTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();

    std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
    for (int i = index + 1; i <= getLastLogIndex(); ++i)
    {
        trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
    }

    m_lastSnapshotIncludeIndex = newIdx;
    m_lastSnapshotIncludeTerm = newTerm;
    m_logs = trunckedLogs;

    int oldCommit = m_commitIndex, oldApplied = m_lastApplied;
    m_commitIndex = std::max(m_commitIndex, index);
    m_lastApplied = std::max(m_lastApplied, index);

    m_persister->Save(persistData(), snapshot);

    DPrintf("[Snap] 完成：snapIdx=%d snapTerm=%d logLen=%d commit=%d(%d) lastApplied=%d(%d)",
            m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm, (int)m_logs.size(),
            m_commitIndex, oldCommit, m_lastApplied, oldApplied);

    myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
             format("len(logs){%d} + snapIdx{%d} != oldLastIdx{%d}", (int)m_logs.size(),
                    m_lastSnapshotIncludeIndex, lastLogIndex));
}
