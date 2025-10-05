
#ifndef SKIPLIST_H
#define SKIPLIST_H
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>

#include "mlog.hpp"
#include "m_utils.hpp"
// @brief 并发安全的跳表
#define STORE_FILE "store/dumpFile"
static std::string delimiter = ":";

template <typename K, typename V>
class Node // 跳表的节点类
{
public:
    Node() {}            // 空构造
    Node(K k, V v, int); // 这里的int什么意思?
    ~Node();
    K get_key() const; // const代表不会修改成员变量
    V get_value() const;
    void set_value(V);
    Node<K, V> **forward; // 指向下一个节点
    int node_level;

private:
    K key;
    V value;
};

template <typename K, typename V>
Node<K, V>::Node(K k, V v, int level) // @brief 构建一个节点
{
    this->key = k;
    this->value = v;
    this->node_level = level;
    this->forward = new Node<K, V> *[level + 1];
    // 填充层级为满
    memset(this->forward, 0, sizeof(this->forward) * (level + 1));
}
template <typename K, typename V>
Node<K, V>::~Node()
{
    delete[] forward; // 删除这个二维数组
}

template <typename K, typename V>
K Node<K, V>::get_key() const
{
    return key;
}

template <typename K, typename V>
V Node<K, V>::get_value() const // 这里的const一定要带上
{
    return value;
}

template <typename K, typename V>
void Node<K, V>::set_value(V v)
{
    this->value = v;
}

// --- 跳表dump结构 ---
template <typename K, typename V>
class SkipListDump //"dump" 通常指将内存中的数据​​导出/转储​​到持久化存储（如磁盘文件）的过程
{
public: // SkipListDump 类的目的是​​序列化跳表数据​​以便存储或传输。
    friend class boost::serialization::access;

    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
        ar & keyDumpVt_;
        ar & valDumpVt_;
    }
    std::vector<K> keyDumpVt_;
    std::vector<V> valDumpVt_;

public:
    void insert(const Node<K, V> &node);
};

// --- 跳表结构 ---
// Class template for Skip list
template <typename K, typename V>
class SkipList
{
public:
    SkipList(int);
    ~SkipList();
    int get_random_level();
    Node<K, V> *create_node(K, V, int); // 函数指针
    int insert_element(K, V);
    void display_list();
    bool search_element(K, V &value);
    void delete_element(K);
    void insert_set_element(K &, V &);
    std::string dump_file();
    void load_file(const std::string &dumpStr);
    // 递归删除节点
    void clear(Node<K, V> *);
    int size();

private:
    void get_key_value_from_string(const std::string &str, std::string *key, std::string *value);
    bool is_valid_string(const std::string &str);

private:
    // Maximum level of the skip list
    int _max_level;

    // current level of skip list
    int _skip_list_level;

    // pointer to header node
    Node<K, V> *_header; // 跳表的头节点

    // file operator
    std::ofstream _file_writer;
    std::ifstream _file_reader;

    // skiplist current element count
    int _element_count;
    // 互斥锁并发安全
    std::mutex _mtx; // mutex for critical section
};

// --- 跳表函数部分---
template <typename K, typename V>
Node<K, V> *SkipList<K, V>::create_node(K k, V v, int level)
{
    return new Node<K, V>(k, v, level);
}

// Insert given key and value in skip list
// return 1 means element exists
// return 0 means insert successfully
/*
                           +------------+
                           |  insert 50 |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |                      insert +----+
level 3         1+-------->10+---------------> | 50 |          70       100
                                               |    |
                                               |    |
level 2         1          10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 1         1    4     10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 0         1    4   9 10         30   40  | 50 |  60      70       100
                                               +----+

*/

template <typename K, typename V>
int SkipList<K, V>::insert_element(K key, V v)
{

    // 上锁
    _mtx.lock();
    Node<K, V> *current = this->_header;
    Node<K, V> *update[_max_level + 1]; // 声明要进行插入的

    // 极具跳表特色的查找，优化时间复杂度
    for (int i = _skip_list_level; i >= 0; --i)
    {
        // 按key排序，索引时，按照从小到大的顺序来将节点插入到全部小于自己的节点前面
        while (current->forward[i] != NULL && current->forward[i]->get_key() < key)
        {
            current = current->forward[i];
        }
        update[i] = current;
    }
    current = current->forward[0]; // 跳转到下一个节点的0层级
    // 下一个节点存在，且和当前key相同：不允许存在相同key报错
    if (current != NULL && current->get_key() == key)
    {
        std::cout << "key: " << key << ", exists" << std::endl;
        _mtx.unlock();
        return 1;
    }
    std::stringstream sst;
    // 下一个节点不存在，或者和当前key不相同，正常执行插入
    if (current == NULL || current->get_key() != key)
    {
        int random_level = get_random_level(); // 随机生成节点高度
        if (random_level > _skip_list_level)   // 若生成的随机节点高度，超过当前的高度
        {
            // 将这些新层级上新节点的前驱节点（在 update 数组中）都设置为跳表的头节点 (_header)
            for (int i = _skip_list_level + 1; i < random_level + 1; i++)
            {
                update[i] = _header;
            }
            _skip_list_level = random_level;
        }

        // 执行插入节点
        Node<K, V> *insertNode = create_node(key, v, random_level);
        // 插入节点
        for (int i = 0; i < random_level; ++i)
        {
            insertNode->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = insertNode;
        }
        _element_count++; // 节点个数+1
        sst << "节点插入成功，其层级高度为:" << random_level << ",当前节点数量为:" << _element_count;
    }

    mlog::LOG(sst.str());
    _mtx.unlock();
    return 0;
}

// 显示跳表(打印跳表)
template <typename K, typename V>
void SkipList<K, V>::display_list()
{
    std::stringstream sst;
    sst << "*****Skip List Display*****";
    mlog::LOG(sst.str());
    sst.clear();
    for (int i = 0; i <= _skip_list_level; i++)
    {
        Node<K, V> *node = this->_header->forward[i];
        sst << "Level " << i << ": ";
        while (node != NULL)
        {
            sst << node->get_key() << ":" << node->get_value() << ";";
            node = node->forward[i];
        }
        mlog::LOG(sst.str());
        sst.clear();
    }
}

// todo 对dump 和 load 后面可能要考虑加锁的问题
// Dump data in memory to file
template <typename K, typename V>
std::string SkipList<K, V>::dump_file()
{
    // std::cout << "dump_file-----------------" << std::endl;
    //
    //
    // _file_writer.open(STORE_FILE);
    Node<K, V> *node = this->_header->forward[0];
    SkipListDump<K, V> dumper;
    while (node != nullptr)
    {
        dumper.insert(*node);
        // _file_writer << node->get_key() << ":" << node->get_value() << "\n";
        // std::cout << node->get_key() << ":" << node->get_value() << ";\n";
        node = node->forward[0];
    }
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << dumper;
    return ss.str();
    // _file_writer.flush();
    // _file_writer.close();
}

// Load data from disk
template <typename K, typename V>
void SkipList<K, V>::load_file(const std::string &dumpStr)
{
    // _file_reader.open(STORE_FILE);
    // std::cout << "load_file-----------------" << std::endl;
    // std::string line;
    // std::string* key = new std::string();
    // std::string* value = new std::string();
    // while (getline(_file_reader, line)) {
    //     get_key_value_from_string(line, key, value);
    //     if (key->empty() || value->empty()) {
    //         continue;
    //     }
    //     // Define key as int type
    //     insert_element(stoi(*key), *value);
    //     std::cout << "key:" << *key << "value:" << *value << std::endl;
    // }
    // delete key;
    // delete value;
    // _file_reader.close();

    if (dumpStr.empty())
    {
        return;
    }
    SkipListDump<K, V> dumper;
    std::stringstream iss(dumpStr);
    boost::archive::text_iarchive ia(iss);
    ia >> dumper;
    for (int i = 0; i < dumper.keyDumpVt_.size(); ++i)
    {
        insert_element(dumper.keyDumpVt_[i], dumper.keyDumpVt_[i]);
    }
}

// Get current SkipList size
template <typename K, typename V>
int SkipList<K, V>::size()
{
    return _element_count;
}

template <typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string &str, std::string *key, std::string *value)
{
    if (!is_valid_string(str))
    {
        return;
    }
    *key = str.substr(0, str.find(delimiter));
    *value = str.substr(str.find(delimiter) + 1, str.length());
}

template <typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string &str)
{
    if (str.empty())
    {
        return false;
    }
    if (str.find(delimiter) == std::string::npos)
    {
        return false;
    }
    return true;
}

// Delete element from skip list
template <typename K, typename V>
void SkipList<K, V>::delete_element(K key)
{
    _mtx.lock();
    Node<K, V> *current = this->_header;
    Node<K, V> *update[_max_level + 1];
    memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));

    // start from highest level of skip list
    for (int i = _skip_list_level; i >= 0; i--)
    {
        while (current->forward[i] != NULL && current->forward[i]->get_key() < key)
        {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];
    if (current != NULL && current->get_key() == key)
    {
        // start for lowest level and delete the current node of each level
        for (int i = 0; i <= _skip_list_level; i++)
        {
            // if at level i, next node is not target node, break the loop.
            if (update[i]->forward[i] != current)
                break;

            update[i]->forward[i] = current->forward[i];
        }

        // Remove levels which have no elements
        while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0)
        {
            _skip_list_level--;
        }

        std::cout << "Successfully deleted key " << key << std::endl;
        delete current;
        _element_count--;
    }
    _mtx.unlock();
    return;
}

/**
 * \brief 作用与insert_element相同类似，
 * insert_element是插入新元素，
 * insert_set_element是插入元素，如果元素存在则改变其值
 */
template <typename K, typename V>
void SkipList<K, V>::insert_set_element(K &key, V &value)
{
    V oldValue;
    if (search_element(key, oldValue))
    {
        delete_element(key);
    }
    insert_element(key, value);
}

// Search for element in skip list
/*
                           +------------+
                           |  select 60 |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |
level 3         1+-------->10+------------------>50+           70       100
                                                   |
                                                   |
level 2         1          10         30         50|           70       100
                                                   |
                                                   |
level 1         1    4     10         30         50|           70       100
                                                   |
                                                   |
level 0         1    4   9 10         30   40    50+-->60      70       100
*/
template <typename K, typename V>
bool SkipList<K, V>::search_element(K key, V &value)
{
    std::cout << "search_element-----------------" << std::endl;
    Node<K, V> *current = _header;

    // start from highest level of skip list
    for (int i = _skip_list_level; i >= 0; i--)
    {
        while (current->forward[i] && current->forward[i]->get_key() < key)
        {
            current = current->forward[i];
        }
    }

    // reached level 0 and advance pointer to right node, which we search
    current = current->forward[0];

    // if current node have key equal to searched key, we get it
    if (current and current->get_key() == key)
    {
        value = current->get_value();
        std::cout << "Found key: " << key << ", value: " << current->get_value() << std::endl;
        return true;
    }

    std::cout << "Not Found Key:" << key << std::endl;
    return false;
}

template <typename K, typename V>
void SkipListDump<K, V>::insert(const Node<K, V> &node)
{
    keyDumpVt_.emplace_back(node.get_key());
    valDumpVt_.emplace_back(node.get_value());
}

// construct skip list
template <typename K, typename V>
SkipList<K, V>::SkipList(int max_level)
{
    this->_max_level = max_level;
    this->_skip_list_level = 0;
    this->_element_count = 0;

    // create header node and initialize key and value to null
    K k;
    V v;
    this->_header = new Node<K, V>(k, v, _max_level);
};

template <typename K, typename V>
SkipList<K, V>::~SkipList()
{
    if (_file_writer.is_open())
    {
        _file_writer.close();
    }
    if (_file_reader.is_open())
    {
        _file_reader.close();
    }

    // 递归删除跳表链条
    if (_header->forward[0] != nullptr)
    {
        clear(_header->forward[0]);
    }
    delete (_header);
}
template <typename K, typename V>
void SkipList<K, V>::clear(Node<K, V> *cur)
{
    if (cur->forward[0] != nullptr)
    {
        clear(cur->forward[0]);
    }
    delete (cur);
}

template <typename K, typename V>
int SkipList<K, V>::get_random_level()
{
    int k = 1;
    while (rand() % 2)
    {
        k++;
    }
    k = (k < _max_level) ? k : _max_level;
    return k;
};
#endif // !SKIPLIST_H
