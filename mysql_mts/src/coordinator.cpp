//
// Created by ZFY on 2021/4/27.
//
#include "../include/coordinator.h"
#include <sys/time.h>


namespace mysql_mts{
    struct timeval tv;
    time_t get_now(){
        gettimeofday(&tv, NULL);
        return tv.tv_sec * 1000000 + tv.tv_usec;
    }

    uint8_t Coordinator::init(Rpl_info &rif) {
        n = 16;
//      初始化binlog解析相关内容
        rplInfo = rif;
        if (rplInfo.is_remote){
            eventFetcher = new Remote_event_fetcher();
        }else{
            eventFetcher = new Binlog_file_event_fetcher(rplInfo.files);
        }
        eventHandler = new binary_log::Event_Handler();
//        相当于数据库
        tables = new std::unordered_map<std::string, rpl::Table *>();
        eventHandler->init(tables, rplInfo.all_tables);
//        初始化worker
        workers = std::vector<Worker *>(n);
        for (int i = 0; i < n; ++i) {
            workers[i] = new Worker(this, i);
//            workers[i]->run();
            free_workers.enqueue(i);
        }
        return 0;
    }

    uint8_t Coordinator::run() {
        std::unique_lock<std::mutex> lock(mu);
        uint64_t now_worker;
        Worker *worker = nullptr;
        uint64_t last_committed,sequence_number;
        time_t start = get_now();
        uint64_t trx = 0;
        do{
            auto eb = new rpl::event_buffer();
            trx++; //7020112 1119626
            if (eventFetcher->fetch_a_event(eb->buffer, eb->length)){
                break;
            }
            switch ((binary_log::Log_event_type)eb->buffer[EVENT_TYPE_OFFSET]) {
                case binary_log::FORMAT_DESCRIPTION_EVENT:{
                    binary_log::Format_description_event *fde_tmp = new binary_log::Format_description_event(4, "8.017");
                    fde = new binary_log::Format_description_event(reinterpret_cast<const char *>(eb->buffer), fde_tmp);
                    break;
                }
                case binary_log::ANONYMOUS_GTID_LOG_EVENT:{
                    if (worker)
                    {
                        std::cout<<"error"<<std::endl;
                        break;
                    }
                    auto event = new binary_log::Gtid_event(reinterpret_cast<const char *>(eb->buffer),fde);
                    last_committed = event->last_committed;
                    sequence_number = event->sequence_number;
                    for (int i = 0; i < n; ++i) {
                        if (workers[i]->in_use&&workers[i]->sequence_number<=last_committed){
                            while (workers[i]->in_use){
                                cv.wait(lock);
                            }
                        }
                    }
                    auto x = free_workers.size_approx();
                    free_workers.wait_dequeue(now_worker);
                    worker = workers[now_worker];
                    worker->set_trx(last_committed, sequence_number);
                    worker->in_use = true;
//                    worker->add_job(eb->buffer);
                    break;
                }
                default:{
//                    if ((binary_log::Log_event_type)eb->buffer[EVENT_TYPE_OFFSET]==binary_log::XID_EVENT){
//                        std::cout<<"---XID_EVENT"<<std::endl;
//                    }else if ((binary_log::Log_event_type)eb->buffer[EVENT_TYPE_OFFSET]==binary_log::WRITE_ROWS_EVENT){
//                        std::cout<<"----WRITE_ROWS_EVENT"<<std::endl;
//                    }
                    if (worker){
                        worker->add_job(eb);
                        worker->cv.notify_all();
                    }
                    if ((binary_log::Log_event_type)eb->buffer[EVENT_TYPE_OFFSET]==binary_log::XID_EVENT) {
                        worker = nullptr;
                    }
                    break;
                }
            }
        }while (true);
        for (int i = 0; i < n; ++i) {
//            while (!workers[i]->jobs.empty());
            workers[i]->stop = true;
//            worker[i].jobs.push(nullptr);
//            workers[i]->cv.notify_all();
            workers[i]->thread.join();
        }
        time_t end_time = get_now();
        std::cout<<start-end_time<<std::endl;
    }

    uint8_t Coordinator::exec(rpl::event_buffer *eb) {
//        switch ((binary_log::Log_event_type)eb->buffer[EVENT_TYPE_OFFSET]) {
//            case binary_log::ANONYMOUS_GTID_LOG_EVENT:{
//                auto event = new binary_log::Gtid_event(reinterpret_cast<const char *>(eb->buffer),fde);
//                uint64_t last_committed,sequence_number;
//                last_committed = event->last_committed;
//                sequence_number = event->sequence_number;
//                for (int i = 0; i < 16; ++i) {
//                    if (workers[i]->in_use){
//                        while (workers[i]->sequence_number<=last_committed){
//                            cv.wait(lock);
//                        }
//                    }
//                }
//                free_workers.wait_dequeue(worker);
//                worker->set_trx(last_committed, sequence_number);
//                worker->in_use = true;
//                worker->add_job(eb->buffer);
//                break;
//            }
//            default:{
//                if (worker){
//                    worker->add_job(eb->buffer);
//                }
//                break;
//            }
//        }
    }

    uint8_t Coordinator::add_free_work(int id) {
        free_workers.enqueue(id);
        return 0;
    }

}
