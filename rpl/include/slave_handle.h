#ifndef FINE_GRAINED_MTS_SLAVE_HANDLE_H
#define FINE_GRAINED_MTS_SLAVE_HANDLE_H
#include "table.h"
#include "event_handle.h"
#include "thread_pool.h"
#include "master_info.h"
#include "remote_event_fetcher.h"
#include "binlog_file_event_fetcher.h"
#include "table_schema.h"
#include "safe_map.h"

namespace rpl{

    class SlaveHandle{
//    private:
//        Event_fetcher *eventFetcher;
//        binary_log::Format_description_event *fde;
//        binary_log::Event_Handler eventHandler;
//        ThreadPool *pool;
//        std::vector<Commiter *> commiters;
//    public:
//        std::unordered_map<std::string, Table *> tables;
//    public:
//        int init();
//        int run();
//    protected:
//        int process(std::shared_ptr<event_buffer> eb);
//        int handle_(std::shared_ptr<event_buffer> eb, std::vector<Trx_info *> trxBuffer);
//        int handle(std::shared_ptr<std::vector<std::shared_ptr<event_buffer>>> buffers, uint64_t xid);
//        int rows_parser(Trx_info *trxBuffer);

    };
}

#endif //FINE_GRAINED_MTS_SLAVE_HANDLE_H
