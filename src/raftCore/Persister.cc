#include "Persister.hpp"
#include "m_utils.hpp"

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdio>
#include <cerrno>
#include <cstring>

using std::string;

static inline void log_warn(const char* tag, const std::string& path, const char* what) {
    std::fprintf(stderr, "[Persist][WARN] %s (%s): %s, errno=%d(%s)\n",
                 tag, path.c_str(), what, errno, std::strerror(errno));
}

static inline void log_err(const char* tag, const std::string& path, const char* what) {
    std::fprintf(stderr, "[Persist][ERR ] %s (%s): %s, errno=%d(%s)\n",
                 tag, path.c_str(), what, errno, std::strerror(errno));
}

// ---- 工具：读取整个文件（失败返回空字符串）----
std::string Persister::readWholeFile(const std::string& path) {
    try {
        std::ifstream ifs(path, std::ios::binary);
        if (!ifs.is_open()) return {};
        return std::string((std::istreambuf_iterator<char>(ifs)),
                           std::istreambuf_iterator<char>());
    } catch (...) {
        log_err("readWholeFile", path, "exception");
        return {};
    }
}

// ---- 工具：获取文件大小（失败返回0）----
long long Persister::fileSizeOrZero(const std::string& path) {
    std::error_code ec;
    auto sz = std::filesystem::file_size(path, ec);
    if (ec) return 0;
    return static_cast<long long>(sz);
}

// ---- 工具：原子写入（tmp 文件 -> rename 覆盖）----
bool Persister::atomicWriteFile(const std::string& path, const std::string& data, bool* out_sameDirRenameOK) {
    try {
        std::filesystem::path p(path);
        std::filesystem::path dir = p.parent_path();
        std::filesystem::path tmp = dir / (p.filename().string() + ".tmp");

        // 1) 先写入临时文件
        {
            std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
            if (!ofs.is_open()) {
                log_err("atomicWriteFile", tmp.string(), "open tmp failed");
                return false;
            }
            ofs.write(data.data(), static_cast<std::streamsize>(data.size()));
            if (!ofs) {
                log_err("atomicWriteFile", tmp.string(), "write failed");
                return false;
            }
            ofs.flush();
            if (!ofs) {
                log_err("atomicWriteFile", tmp.string(), "flush failed");
                return false;
            }
        }

        // 2) 原子重命名到目标（同目录 rename 基本是原子操作）
        std::error_code ec;
        std::filesystem::rename(tmp, p, ec);
        if (ec) {
            // 如果 rename 失败，尝试 fallback：先删除目标再 rename
            std::filesystem::remove(p, ec); // 忽略错误
            ec.clear();
            std::filesystem::rename(tmp, p, ec);
            if (ec) {
                log_err("atomicWriteFile", p.string(), "rename failed");
                // 最后尝试直接写目标（非原子），尽量不丢数据
                std::ofstream ofs(p, std::ios::binary | std::ios::trunc);
                if (!ofs.is_open()) {
                    log_err("atomicWriteFile", p.string(), "fallback open failed");
                    // 清理 tmp
                    std::filesystem::remove(tmp, ec);
                    return false;
                }
                ofs.write(data.data(), static_cast<std::streamsize>(data.size()));
                if (!ofs) {
                    log_err("atomicWriteFile", p.string(), "fallback write failed");
                    std::filesystem::remove(tmp, ec);
                    return false;
                }
                ofs.flush();
                if (!ofs) {
                    log_err("atomicWriteFile", p.string(), "fallback flush failed");
                    std::filesystem::remove(tmp, ec);
                    return false;
                }
                std::filesystem::remove(tmp, ec);
                if (out_sameDirRenameOK) *out_sameDirRenameOK = false;
                return true;
            }
        }
        if (out_sameDirRenameOK) *out_sameDirRenameOK = true;
        return true;
    } catch (...) {
        log_err("atomicWriteFile", path, "exception");
        return false;
    }
}

// ---------------- 公共接口实现 ----------------

// 同时保存 raftstate 与 snapshot
void Persister::Save(std::string raftstate, std::string snapshot) {
    std::lock_guard<std::mutex> lg(m_mtx);
    try {
        // 先写入两个 tmp 再分别原子覆盖
        bool ok1 = atomicWriteFile(m_raftStateFileName, raftstate);
        bool ok2 = atomicWriteFile(m_snapshotFileName, snapshot);
        if (!ok1) log_warn("Save", m_raftStateFileName, "write raftstate failed");
        if (!ok2) log_warn("Save", m_snapshotFileName, "write snapshot failed");

        if (ok1) m_raftStateSize = static_cast<long long>(raftstate.size());
        // snapshot 大小如需缓存可另加变量；当前未对外提供 size 接口
    } catch (...) {
        log_err("Save", m_raftStateFileName, "exception");
        // 忽略异常，保证不崩
    }
}

// 读取 snapshot（完整二进制）
std::string Persister::ReadSnapshot() {
    std::lock_guard<std::mutex> lg(m_mtx);
    return readWholeFile(m_snapshotFileName);
}

// 保存 raft 状态（完整二进制）
void Persister::SaveRaftState(const std::string &data) {
    std::lock_guard<std::mutex> lg(m_mtx);
    try {
        bool ok = atomicWriteFile(m_raftStateFileName, data);
        if (!ok) {
            log_warn("SaveRaftState", m_raftStateFileName, "atomic write failed");
            return;
        }
        m_raftStateSize = static_cast<long long>(data.size());
    } catch (...) {
        log_err("SaveRaftState", m_raftStateFileName, "exception");
    }
}

// 获取 raft 状态大小（字节）
long long Persister::RaftStateSize() {
    std::lock_guard<std::mutex> lg(m_mtx);
    // 直接返回缓存；如需绝对准确可改为计算文件大小
    return m_raftStateSize;
}

// 读取 raft 状态（完整二进制）
std::string Persister::ReadRaftState() {
    std::lock_guard<std::mutex> lg(m_mtx);
    return readWholeFile(m_raftStateFileName);
}

// 构造：创建目录，清理/准备文件，并初始化 size
Persister::Persister(const int me)
    : m_raftStateFileName(
          (std::filesystem::path(PROJECT_ROOT) / "data" /
           ("raftstatePersist" + std::to_string(me) + ".bin")).string()),
      m_snapshotFileName(
          (std::filesystem::path(PROJECT_ROOT) / "data" /
           ("snapshotPersist" + std::to_string(me) + ".bin")).string())
{
    // 确保目录存在
    const std::filesystem::path dataDir = std::filesystem::path(PROJECT_ROOT) / "data";
    std::error_code ec;
    std::filesystem::create_directories(dataDir, ec);

    // 打开占位流（不用于实际写），防止外部误依赖崩
    // 这里不用开启 exceptions，避免抛异常
    m_raftStateOutStream.open(m_raftStateFileName, std::ios::binary | std::ios::app);
    if (m_raftStateOutStream.is_open()) {
        m_raftStateOutStream.close();
    }

    m_snapshotOutStream.open(m_snapshotFileName, std::ios::binary | std::ios::app);
    if (m_snapshotOutStream.is_open()) {
        m_snapshotOutStream.close();
    }

    // 初始化 size
    m_raftStateSize = fileSizeOrZero(m_raftStateFileName);
}

Persister::~Persister() {
    // 保守起见：关闭占位流（正常情况下它们应已关闭）
    if (m_raftStateOutStream.is_open()) m_raftStateOutStream.close();
    if (m_snapshotOutStream.is_open()) m_snapshotOutStream.close();
}

// 截断 raftState
void Persister::clearRaftState() {
    try {
        std::ofstream ofs(m_raftStateFileName, std::ios::binary | std::ios::trunc);
        if (!ofs.is_open()) {
            log_warn("clearRaftState", m_raftStateFileName, "open failed");
        }
        m_raftStateSize = 0;
    } catch (...) {
        log_err("clearRaftState", m_raftStateFileName, "exception");
    }
}

// 截断 snapshot
void Persister::clearSnapshot() {
    try {
        std::ofstream ofs(m_snapshotFileName, std::ios::binary | std::ios::trunc);
        if (!ofs.is_open()) {
            log_warn("clearSnapshot", m_snapshotFileName, "open failed");
        }
    } catch (...) {
        log_err("clearSnapshot", m_snapshotFileName, "exception");
    }
}

// 同时截断
void Persister::clearRaftStateAndSnapshot() {
    clearRaftState();
    clearSnapshot();
}
