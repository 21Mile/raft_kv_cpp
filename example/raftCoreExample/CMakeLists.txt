set(SRC_LIST raftKV_DB.cc)


add_executable(raftCoreRun ${SRC_LIST})
target_link_libraries(raftCoreRun muduo_net muduo_base pthread skip_list_on_raft )
