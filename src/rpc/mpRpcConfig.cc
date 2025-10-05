#include "mpRpcConfig.hpp"
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <toml.hpp>
#include <type_traits>

// 把 TOML 的层级展平为 "a.b.c" 形式写入 m_configMap
static void FlattenToml(const toml::value &v,
                        const std::string &prefix,
                        std::unordered_map<std::string, std::string> &out)
{
    using kind = toml::value_t;

    const auto put_scalar = [&](const std::string &key, const toml::value &val)
    {
        std::string s;
        switch (val.type())
        {
        case kind::string:
            s = toml::get<std::string>(val);
            break;
        case kind::integer:
            s = std::to_string(toml::get<std::int64_t>(val));
            break;
        case kind::floating:
            s = std::to_string(toml::get<double>(val));
            break;
        case kind::boolean:
            s = toml::get<bool>(val) ? "true" : "false";
            break;
        case kind::local_date:
        case kind::local_time:
        case kind::local_datetime:
        case kind::offset_datetime:
            s = toml::format(val);
            break;
        default:
            return; // 其他复杂类型在外层处理
        }
        out[key] = std::move(s);
    };

    switch (v.type())
    {
    case kind::table:
    {
        const auto &tbl = toml::get<toml::table>(v); // ← 用 get
        for (const auto &kv : tbl)
        {
            const std::string key = prefix.empty() ? kv.first : (prefix + "." + kv.first);
            FlattenToml(kv.second, key, out);
        }
        break;
    }
    case kind::array:
    {
        const auto &arr = toml::get<toml::array>(v); // ← 用 get
        for (std::size_t i = 0; i < arr.size(); ++i)
        {
            const std::string key = prefix + "[" + std::to_string(i) + "]";
            FlattenToml(arr.at(i), key, out);
        }
        break;
    }
    default:
        if (!prefix.empty())
            put_scalar(prefix, v);
        break;
    }
}

void MprpcConfig::LoadConfigFile(const char *config_file)
{
    try
    {
        toml::value root = toml::parse(config_file);
        m_configMap.clear();
        FlattenToml(root, "", m_configMap);

        // 兼容老键：node0.ip/node0.port → node0ip/node0port
        for (const auto &kv : m_configMap)
        {
            auto pos = kv.first.find('.');
            if (pos == std::string::npos)
                continue;
            const std::string left = kv.first.substr(0, pos);   // node0
            const std::string right = kv.first.substr(pos + 1); // ip/port
            if (right == "ip" || right == "port")
            {
                m_configMap[left + right] = kv.second; // node0ip / node0port
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "解析 TOML 配置失败: " << config_file << "\nwhat(): " << e.what() << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

std::string MprpcConfig::Load(const std::string &key)
{
    auto it = m_configMap.find(key);
    if (it == m_configMap.end())
        return "";
    return it->second;
}

void MprpcConfig::Trim(std::string &src_buf)
{
    auto l = src_buf.find_first_not_of(' ');
    if (l != std::string::npos)
        src_buf.erase(0, l);
    auto r = src_buf.find_last_not_of(' ');
    if (r != std::string::npos)
        src_buf.erase(r + 1);
}
