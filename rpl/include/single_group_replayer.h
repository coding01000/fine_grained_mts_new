#ifndef FINE_GRAINED_MTS_SINGLETHREADREPLAYER_H
#define FINE_GRAINED_MTS_SINGLETHREADREPLAYER_H
#include "replayer.h"
#include "commiter.h"
#include "rpl_info.h"
#include "boost/asio/thread_pool.hpp"
#include "boost/asio/post.hpp"

namespace rpl{
    class SingleGroupReplayer: public Replayer{
    private:
        Commiter *commiter;
        uint64_t trx;

    public:
        SingleGroupReplayer(Rpl_info &rplInfo);
        uint8_t init();
        uint8_t run();

    private:
        uint8_t parallel_process(std::shared_ptr<event_buffer> eb);
        uint8_t event_handle(std::shared_ptr<event_buffer> eb, Trx_info *trxInfo);
        uint8_t trx_handle(std::shared_ptr<std::vector<std::shared_ptr<event_buffer>>> buffers, uint64_t xid);
    };
}


#endif //FINE_GRAINED_MTS_SINGLETHREADREPLAYER_H
