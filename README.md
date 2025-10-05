# 概述
这是一个基于C++17版本实现了raft算法的项目


* **bin/**：最终可执行文件输出目录（运行产物）。
* **build/**：你可能手动创建的构建目录（建议用 out/ 分离）。
* **CMakeLists.txt**：工程顶层 CMake 配置入口。
* **CMakePresets.json**：CMake 预设（构建配置/生成器等）。
* **configs/**：运行配置文件（如 `test.toml`）。
* **data/**：运行时数据或样例数据放这里。
* **demos/**：示例程序（客户端 `raftClient`、服务端 `raftServer` 等）。
* **lib/**：编译生成的静态库/动态库输出目录（如 `libskip_list_on_raft.a`）。
* **Makefile**：顶层 make 入口（通常由 CMake 生成或自备）。
* **out/**：实际使用的构建产物目录（按 preset 生成，包含中间文件）。
* **README.md**：项目说明文档。
* **src/**：项目源码主体与各子模块（raftCore、rpc、public、fiber、…）。
* **tests/**：测试用例与测试工程（CMake 测试入口等）。




# 部署

只需要一行命令即可

```bash
make
```

生成了客户端和服务端，在bin目录下

注意先运行服务端后过一段时间再运行客户端

客户端的操作是写在源码里的，在example目录下（无法通过命令行动态更改）

服务端的多个raft节点，运行在不同的ip+port，并且都有自己独立的进程号


# 测试

运行 `./bin/raft_server_run`

