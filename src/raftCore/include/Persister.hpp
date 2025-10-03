#pragma once
#ifndef PERSISTER_H
#define PERSISTER_H
#include <fstream>
#include <mutex>

class Persister
{
private:
    std::mutex m_mtx; // 互斥锁：并发安全
    std::string m_raftState;
    std::string m_snapshot;
    /**
     * m_raftStateFileName: raftState文件名
     */
    const std::string m_raftStateFileName;
    /**
     * m_snapshotFileName: snapshot文件名
     */
    const std::string m_snapshotFileName;
    /**
     * 保存raftState的输出流
     */
    std::ofstream m_raftStateOutStream;
    /**
     * 保存snapshot的输出流
     */
    std::ofstream m_snapshotOutStream;
    /**
     * 保存raftStateSize的大小
     * 避免每次都读取文件来获取具体的大小
     */
    long long m_raftStateSize;

public:
    void Save(std::string raftstate, std::string snapshot);
    std::string ReadSnapshot();
    void SaveRaftState(const std::string &data);
    long long RaftStateSize();
    std::string ReadRaftState();
    explicit Persister(int me);
    ~Persister();

private:
    void clearRaftState(); //清空状态
    void clearSnapshot(); //清空快照
    void clearRaftStateAndSnapshot();//清空raft状态和快照
};

#endif // !PERSISTER_H