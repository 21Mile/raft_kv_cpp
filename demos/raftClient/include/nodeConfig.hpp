
#ifndef node_config_client
#define node_config_client

#include <iostream>
// #include "raft.h"
// #include "kvServer.h"
#include <unistd.h>
#include <iostream>
#include <random>
#include <toml.hpp>
#include "mlog.hpp" //有的时候头文件的检测是有问题
// 节点配置结构
struct NodeConfig
{
    std::string nodeName;
    std::string ip;
    int port;
};

std::unordered_map<std::string, NodeConfig> parseNodeConfig(const std::string &filename);

void ShowArgsHelp() { std::cout << "format: command -n <nodeNum> -f <configFileName>" << std::endl; }

// 解析 TOML 配置文件
std::unordered_map<std::string, NodeConfig> parseNodeConfig(const std::string &filename)
{
    std::ifstream in(filename);
    if (!in.is_open())
    {
        throw std::runtime_error("无法打开配置文件: " + filename);
    }

    auto trim = [](std::string &s)
    {
        auto notsp = [](unsigned char ch)
        { return ch != ' ' && ch != '\t' && ch != '\r' && ch != '\n'; };
        auto b = std::find_if(s.begin(), s.end(), notsp);
        auto e = std::find_if(s.rbegin(), s.rend(), notsp).base();
        s = (b < e) ? std::string(b, e) : std::string();
    };
    auto unquote = [](std::string &s)
    {
        if (s.size() >= 2 && ((s.front() == '"' && s.back() == '"') || (s.front() == '\'' && s.back() == '\'')))
        {
            s = s.substr(1, s.size() - 2);
        }
    };

    std::unordered_map<std::string, NodeConfig> nodes;
    std::string line, section;

    while (std::getline(in, line))
    {
        trim(line);
        if (line.empty() || line[0] == '#')
            continue;

        // 段落：[node0]
        if (line.front() == '[' && line.back() == ']')
        {
            section = line.substr(1, line.size() - 2);
            trim(section);
            continue;
        }

        // 键值：ip = "...", port = 7783
        auto pos = line.find('=');
        if (pos == std::string::npos)
            continue;

        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);
        trim(key);
        trim(value);
        unquote(value);

        if (section.empty())
            continue; // 必须在某个 [nodeX] 段里

        NodeConfig &n = nodes[section]; // 以 node 名作为 map 键
        n.nodeName = section;

        if (key == "ip")
        {
            n.ip = value;
        }
        else if (key == "port")
        {
            try
            {
                n.port = std::stoi(value);
            }
            catch (...)
            {
                throw std::runtime_error("无效端口: " + value + " @ [" + section + "]");
            }
        }
    }
    return nodes;
}

#endif // !node_config_client
