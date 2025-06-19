#include "database.h"
#include "../config.h"
#include "../system/global.h"
#include <unordered_map>
#include <iostream>

InMemoryDB::InMemoryDB()
{
    _dbInstance = "InMemory";
}

int InMemoryDB::Open(const std::string)
{
    db = new std::unordered_map<std::string, dbTable>();
    activeTable = "table1";

    std::cout << std::endl
              << "In-Memory DB configuration OK" << std::endl;

    return 0;
}

#if ISEOV
    void InMemoryDB::Init(const std::string value)
    {
    #if IS_TABLE_DEVIDE
        for (uint64_t table_id = 0; table_id < g_table_num; table_id ++)
        {
            string table = string("table") + to_string(table_id);
            uint64_t acc_num_single_table = g_account_num / g_table_num;
            for(uint64_t i = 0; i < acc_num_single_table + 10; i++){
                (*db)[table][std::to_string(i + table_id * acc_num_single_table)] = value;
            }
        }

    #else
        for(uint64_t i = 0; i < g_account_num + 10; i++){
            (*db)[activeTable][std::to_string(i)] = value;
        }
    #endif

        std::cout << "InMemoryDB::Init DONE" << std::endl;
    }
#endif


std::string InMemoryDB::Get(const std::string key)
{
// #if ISEOV
//     if((*db)[activeTable][key].empty()){
//         (*db)[activeTable][key] = "10000";
//         //return "10000";
//     }
//     return (*db)[activeTable][key];
// #else
//     return (*db)[activeTable][key];
// #endif
#if IS_TABLE_DEVIDE
    //DEBUG_V1("test_table: Get(const std::string key), key = %s\n", key);
    //cout << "test_table: Get(const std::string key), key = " << key << endl;
    uint64_t key_int = stoi(key);
    uint64_t table_id = key_int / (g_account_num / g_table_num);
    string table = string("table") + to_string(table_id);
    return (*db)[table][key];

#else
    return (*db)[activeTable][key];
#endif
}

std::string InMemoryDB::Put(const std::string key, const std::string value)
{
#if IS_TABLE_DEVIDE
    std::string oldValue = Get(key);
    uint64_t key_int = stoi(key);
    uint64_t table_id = key_int / (g_account_num / g_table_num);
    string table = string("table") + to_string(table_id);
    (*db)[table][key] = value;
    return oldValue;
#else
    std::string oldValue = Get(key);
    (*db)[activeTable][key] = value;
    return oldValue;
#endif
}

int InMemoryDB::SelectTable(const std::string tableName)
{
    if (tableName == activeTable)
    {
        return 1;
    }
    activeTable = tableName;
    return 0;
}

int InMemoryDB::Close(const std::string)
{
    delete db;
    return 0;
}
