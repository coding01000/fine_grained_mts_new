#ifndef FINE_GRAINED_MTS_MTS_H
#define FINE_GRAINED_MTS_MTS_H
#include "table.h"
#include "event_handle.h"
#include "thread_pool.h"

namespace rpl{
    class MTS_Handler{
    private:
        MYSQL *mysql;
        MYSQL_RPL rpl;
        binary_log::Format_description_event *fde;
        std::unordered_map<std::string, Table *> tables;
        binary_log::Event_Handler eventHandler;
        ThreadPool *pool;
        std::unordered_map<std::string, SafeQueue<Row *> * > q_map; //未执行的序列
        std::condition_variable q_cv;
    public:
        int init();
        int run();
    protected:
        void process();
        int handle(char *buffer, int length);
        int insert_a_record(Table *table, Row *row);
    };
}

#endif //FINE_GRAINED_MTS_MTS_H
