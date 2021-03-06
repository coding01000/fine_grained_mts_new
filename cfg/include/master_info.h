#ifndef FINE_GRAINED_MTS_MASTER_CFG_H
#define FINE_GRAINED_MTS_MASTER_CFG_H
#include "tinyxml2.h"
#include <string>
#include <iostream>

class Master_info{
public:
    std::string user_name;
    std::string pwd;
    std::string host;
    int port;
    Master_info();
};


#endif //FINE_GRAINED_MTS_MASTER_CFG_H
