#ifndef RPL_INFO_H
#define RPL_INFO_H
#include "tinyxml2.h"
#include <string>
#include <iostream>
#include "vector"
#include "unordered_map"

class Rpl_info{
public:
    bool is_remote;
    bool is_single_group;
    std::vector<std::string> files;
    uint32_t parse_pool;
    uint32_t group_num;
    std::vector<uint32_t> group_pool;
    std::unordered_map<std::string, uint32_t> group_map;

    Rpl_info();
};


#endif //FINE_GRAINED_MTS_MASTER_CFG_H
