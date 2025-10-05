set(SRC_LIST raftServer_run.cc)


add_executable(raft_server_run ${SRC_LIST})
target_link_libraries(raft_server_run muduo_net muduo_base pthread skip_list_on_raft )
