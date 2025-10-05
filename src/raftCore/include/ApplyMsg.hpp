
#ifndef APPLYMSG_H
#define APPLYMSG_H

#include <string>
// 信号：用于进程间通信，封装数据结构
class ApplyMsg
{
public:
    bool CommandValid;
    std::string Command; //// 命令内容（通常是一个序列化的客户端请求）
    int CommandIndex;    // 命令在日志中的索引位置
    bool SnapshotValid;
    std::string Snapshot; // 快照数据（序列化的状态机状态）
    int SnapshotTerm;     // 快照对应的任期号
    int SnapshotIndex;    // 快照包含的最后一条日志的索引

    ApplyMsg():
        CommandValid(false),
        Command(),
        CommandIndex(-1),
        SnapshotValid(false){
            
        }


};

#endif