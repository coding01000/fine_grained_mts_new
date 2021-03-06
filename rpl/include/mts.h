#ifndef FINE_GRAINED_MTS_MTS_H
#define FINE_GRAINED_MTS_MTS_H
#include "table.h"
#include "event_handle.h"
#include "thread_pool.h"
#include "master_info.h"
#include "remote_event_fetcher.h"
#include "binlog_file_event_fetcher.h"

namespace rpl{

    typedef struct event_buffer{
        uint8_t *buffer;
        int length;
    };

    class MTS_Handler{
    private:
        Event_fetcher *eventFetcher;
        binary_log::Format_description_event *fde;
        std::unordered_map<std::string, Table *> tables;
        binary_log::Event_Handler eventHandler;
        ThreadPool *pool;
        ThreadPool* process_submiter;
        std::unordered_map<std::string, SafeQueue<Row *> * > q_map; //未执行的序列
        std::condition_variable q_cv;

    public:
        int init();
        int run();
    protected:
        void process(uint8_t *buffer, int length);
        int handle_(uint8_t *buffer, int length);
        int handle(std::vector<event_buffer> *buffers);
        int insert_a_record(Table *table, Row *row);
    };


}

#endif //FINE_GRAINED_MTS_MTS_H
