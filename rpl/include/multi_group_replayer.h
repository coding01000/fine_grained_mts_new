#ifndef FINE_GRAINED_MTS_MULTI_GROUP_REPLAYER_H
#define FINE_GRAINED_MTS_MULTI_GROUP_REPLAYER_H
#include "replayer.h"
#include "commiter.h"
#include "rpl_info.h"
#include "boost/asio/thread_pool.hpp"
#include "boost/asio/post.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fstream>
#include "SQLParser.h"
#include <fstream>
#include <dirent.h>
#include "utility"
#include "algorithm"
#include "query_ferq.h"
#include "algorithm"
#include "chrono"


namespace rpl{
    class MultiGroupReplayer: public Replayer{
    private:
        std::vector<Commiter *> commiters;
//        std::vector<boost::asio::thread_pool *> dispenses_pool;
        std::vector<ThreadPool *> dispenses_pool;
        uint32_t n;
        uint64_t trx =0;
        Rpl_info rplInfo;
        int pipe_fd;
        std::vector<std::pair<uint64_t , std::vector<std::string>>> query_list;


    public:
        MultiGroupReplayer(Rpl_info &rplInfo);
        uint8_t init();
        uint8_t run();
        uint8_t get();
        uint8_t delay_init();
        uint8_t delay();

        std::vector<std::thread> *parsePool;
        std::atomic<int64_t> parse_top, parse_now;
        bool parse_stop;
        struct parse_struct{
            event_buffer **buffer_arr;
            uint64_t xid;
            uint32_t k;
        }parse_queue[1629381+10000];
        void parse_thread();

    private:
        uint8_t parallel_process(event_buffer* eb);
        uint8_t event_handle(Trx_info *trxInfo);
//        uint8_t trx_dispenses(std::vector<event_buffer *> *buffers, uint64_t xid);
        uint8_t trx_dispenses(event_buffer **buffer_arr, uint64_t xid, uint32_t i);
        uint8_t event_dispenses(event_buffer* eb, std::vector<Trx_info *> & trxinfo);
        std::vector<std::string> sql_parser(std::string sql);
        time_t StringToDatetime(std::string str);
    };
}

#endif //FINE_GRAINED_MTS_MULTI_GROUP_REPLAYER_H
