#ifndef FINE_GRAINED_MTS_COORDINATOR_H
#define FINE_GRAINED_MTS_COORDINATOR_H
#include "worker.h"
#include "vector"
#include "concurrentqueue.h"
#include "condition_variable"
#include "table.h"
#include "binary_log.h"
#include "unordered_map"
#include "rpl_info.h"
#include "remote_event_fetcher.h"
#include "binlog_file_event_fetcher.h"
#include "event_handle.h"
#include "commiter.h"
#include "mutex"
#include "condition_variable"
#include "blockingconcurrentqueue.h"
#include "control_events.h"
#include "time.h"
//#include "commiter.cpp"

namespace mysql_mts{
    class Coordinator{
    private:
        std::vector<Worker *> workers;
        moodycamel::BlockingConcurrentQueue<int> free_workers;
        Event_fetcher *eventFetcher;
        Rpl_info rplInfo;
        uint64_t group_max_last_sequence_number;
        uint32_t n;
    public:
        std::mutex mu;
        std::condition_variable cv;
        binary_log::Format_description_event *fde;
        binary_log::Event_Handler *eventHandler;
        std::unordered_map<std::string, rpl::Table *> *tables;
        uint8_t init(Rpl_info &rplInfo);
        uint8_t run();
        uint8_t add_free_work(int id);
        Coordinator(){};
        uint8_t exec(rpl::event_buffer *eb);
        void delay();
    };

}

#endif //FINE_GRAINED_MTS_COORDINATOR_H
