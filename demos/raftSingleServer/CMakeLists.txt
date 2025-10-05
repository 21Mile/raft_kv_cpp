set(SRC_LIST raftSingleServer_run.cc)


add_executable(raft_single_server_run ${SRC_LIST})
target_link_libraries(raft_single_server_run muduo_net muduo_base pthread skip_list_on_raft )
