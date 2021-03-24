#ifndef FINE_GRAINED_MTS_MULTI_GROUP_REPLAYER_H
#define FINE_GRAINED_MTS_MULTI_GROUP_REPLAYER_H
#include "replayer.h"
#include "commiter.h"
#include "rpl_info.h"
#include "boost/asio/thread_pool.hpp"
#include "boost/asio/post.hpp"


namespace rpl{
    class MultiGroupReplayer: public Replayer{
    private:
        std::vector<Commiter *> commiters;
//        std::vector<boost::asio::thread_pool *> dispenses_pool;
        std::vector<ThreadPool *> dispenses_pool;
        uint32_t n;
        uint64_t trx =0;
        Rpl_info rplInfo;

    public:
        MultiGroupReplayer(Rpl_info &rplInfo);
        uint8_t init();
        uint8_t run();

    private:
        uint8_t parallel_process(event_buffer* eb);
        uint8_t event_handle(Trx_info *trxInfo);
//        uint8_t trx_dispenses(std::vector<event_buffer *> *buffers, uint64_t xid);
        uint8_t trx_dispenses(event_buffer **buffer_arr, uint64_t xid, uint32_t i);
        uint8_t event_dispenses(event_buffer* eb, std::vector<Trx_info *> trxinfo);
    };
}

#endif //FINE_GRAINED_MTS_MULTI_GROUP_REPLAYER_H
