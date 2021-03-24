#ifndef FINE_GRAINED_MTS_REPLAYER_H
#define FINE_GRAINED_MTS_REPLAYER_H
#include "table.h"
#include "event_handle.h"
#include "thread_pool.h"
#include "master_info.h"
#include "remote_event_fetcher.h"
#include "binlog_file_event_fetcher.h"
#include "table_schema.h"
#include "safe_map.h"
#include "commiter.h"
#include "boost/asio/thread_pool.hpp"
#include "boost/asio/post.hpp"

namespace rpl{
    class Replayer{
    protected:
        Event_fetcher *eventFetcher;
        binary_log::Format_description_event *fde;
        binary_log::Event_Handler eventHandler;
        std::unordered_map<std::string, Table *> *tables;
//        boost::asio::thread_pool *parse_pool;
        ThreadPool *parse_pool;
        event_buffer ** buffers;

    public:
        virtual uint8_t init()=0;
        virtual uint8_t run()=0;

    };
}

#endif //FINE_GRAINED_MTS_REPLAYER_H
