#ifndef FINE_GRAINED_MTS_MTS_H
#define FINE_GRAINED_MTS_MTS_H
#include "table.h"

namespace rpl{
    class MTS_Handler{
    private:
        MYSQL *mysql;
        MYSQL_RPL rpl;
        binary_log::Format_description_event *fde;
    public:
        int init();
        int run();
    protected:
        int process();
    };
}

#endif //FINE_GRAINED_MTS_MTS_H
