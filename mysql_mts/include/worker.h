//
// Created by ZFY on 2021/4/27.
//

#ifndef FINE_GRAINED_MTS_WORKER_H
#define FINE_GRAINED_MTS_WORKER_H
#include "condition_variable"
#include "mutex"
#include "queue"
#include "binary_log.h"
#include "event_handle.h"
#include "row.h"
#include "table.h"
#include "thread"
#include "functional"
#include "commiter.h"
//#include "coordinator.h"
//#include "lo"

namespace mysql_mts{
    class Coordinator;
    class Worker{
    private:
        Coordinator *coordinator;
//        binary_log::Format_description_event *fde;
//        binary_log::Event_Handler *eventHandler;
//        std::unordered_map<std::string, rpl::Table *> *tables;
        uint32_t id;
    public:
        bool in_use;
        std::queue<rpl::event_buffer *> jobs;
        std::thread thread;
        std::mutex mu;
        std::condition_variable cv;
        bool stop;
        uint64_t last_committed,sequence_number;
        Worker(){};
        Worker(Coordinator *c, uint32_t i);
        void run();
        uint8_t add_job(rpl::event_buffer *eb);
        uint8_t set_trx(uint64_t lc, uint64_t sn);
    };
}

#endif //FINE_GRAINED_MTS_WORKER_H
