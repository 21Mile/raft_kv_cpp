#ifndef PERSISTER_H
#define PERSISTER_H

#ifndef PROJECT_ROOT
#define PROJECT_ROOT "."
#endif

#include <fstream>
#include <mutex>
#include <filesystem>
#include <string>
#include <string_view>

class Persister
{
private:
    std::mutex m_mtx;        // 并发保护
    std::string m_raftState; // （未使用：保留字段）
    std::string m_snapshot;  // （未使用：保留字段）

    // 文件名
    const std::string m_raftStateFileName;
    const std::string m_snapshotFileName;

    // 保留但不再用于真正 I/O，防止外部依赖结构崩
    std::ofstream m_raftStateOutStream;
    std::ofstream m_snapshotOutStream;

    long long m_raftStateSize{0}; // 缓存的 raftState 大小（字节）

public:
    // 外部接口保持不变
    void Save(std::string raftstate, std::string snapshot);
    std::string ReadSnapshot();
    void SaveRaftState(const std::string &data);
    long long RaftStateSize();
    std::string ReadRaftState();

    explicit Persister(int me);
    ~Persister();

private:
    void clearRaftState();            // 截断 raftState 文件
    void clearSnapshot();             // 截断 snapshot 文件
    void clearRaftStateAndSnapshot(); // 同时截断两者

    // 工具：原子写入（tmp+rename）
    static bool atomicWriteFile(const std::string &path, const std::string &data, bool *out_sameDirRenameOK = nullptr);
    static std::string readWholeFile(const std::string &path);
    static long long fileSizeOrZero(const std::string &path);
};

// 获取持久化目录
inline std::filesystem::path DataPath(std::string_view filename)
{
    static const std::filesystem::path kDataDir =
        std::filesystem::path(PROJECT_ROOT) / "data";
    std::error_code ec;
    std::filesystem::create_directories(kDataDir, ec);
    return kDataDir / std::string(filename);
}

#endif // PERSISTER_H
