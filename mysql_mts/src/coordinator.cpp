//
// Created by ZFY on 2021/4/27.
//
#include "../include/coordinator.h"
#include <sys/time.h>
#include <fstream>


namespace mysql_mts{
    struct timeval tv;
    time_t get_now(){
        gettimeofday(&tv, NULL);
        return tv.tv_sec * 1000000 + tv.tv_usec;
    }
    double delay_time[3000];
    bool stop;
    uint64_t trx;

    uint8_t Coordinator::init(Rpl_info &rif) {
        n = 28;
        trx = 0;
        stop = false;
        memset(delay_time, 0, sizeof(delay_time));
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
        uint64_t last_committed, sequence_number;
        time_t start = get_now();
//        std::thread a = std::thread(&Coordinator::delay, this);
//        uint64_t trx = 0;
        do{
            auto eb = new rpl::event_buffer();
//            trx++; //7020112 1119626
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
                        if (workers[i]->in_use && workers[i]->sequence_number <= last_committed){
                            while (workers[i]->in_use){
                                cv.wait(lock);
                            }
                        }
                    }
//                    auto x = free_workers.size_approx();
                    free_workers.wait_dequeue(now_worker);
                    worker = workers[now_worker];
                    worker->set_trx(last_committed, sequence_number);
                    worker->in_use = true;
//                    worker->add_job(eb->buffer);
                    break;
                }

                default:{
                    if (worker){
                        worker->add_job(eb);
                        worker->cv.notify_all();
                    }
                    if ((binary_log::Log_event_type)eb->buffer[EVENT_TYPE_OFFSET]==binary_log::XID_EVENT ||
                            (binary_log::Log_event_type)eb->buffer[EVENT_TYPE_OFFSET]==binary_log::ROTATE_EVENT) {
                        trx++;
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
        std::cout << (end_time - start)  << std::endl;
        std::cout << trx << std::endl;
        stop = true;
//        a.join();
    }

    void Coordinator::delay() {
        uint64_t total_time=0, total_query = 0, cnt = 0, cnt2 = 0, last_cnt[rplInfo.group_num];
        memset(last_cnt, 0, sizeof(uint64_t)*rplInfo.group_num);
        uint64_t stop_num = 5000;
        uint64_t interval = rplInfo.interval;
        auto &freq = rplInfo.query_fre;
        std::vector<int> query_list;
        time_t time1, time2;
        int fq = freq[0].ferq;
        int freq_ = 0;
        std::ofstream file;
        file.open("/root/project/mts_cp/log");
        for (const auto &f : freq) {
            freq_ += f.ferq;
            fq = std::__gcd(fq,f.ferq);
        }
//        uint64_t interval = rplInfo.interval / freq_;
        for (int i = 0; i < freq.size(); i++) {
            int f = freq[i].ferq / fq;
            for (int j = 0; j < f; ++j) {
                query_list.push_back(i);
            }
        }
        memset(delay_time, 0, sizeof(delay_time));
        time_t start = get_now();
//        time_t st = get_now();
        while (true){
            for (const auto &query : query_list) {
                uint64_t now_trx = trx;
                time_t s = get_now();
//                while ((trx / interval) <= total_query){
                while (trx - now_trx <= 50000 ){
                    if (stop || total_query >= stop_num){
                        std::cout << "delay time: " << total_time * 1.0 / 1e6 / total_query << "s total query: " << total_query << std::endl;
                        std::cout << cnt << "----" << cnt2 << std::endl;
                        for (int i = 1; i <= total_query; i++)
                            file << delay_time[i] / 1e6 << std::endl;
                        return ;
                    }
                    std::this_thread::sleep_for(std::chrono::nanoseconds(1));
                }
                total_query++;
                delay_time[total_query] = get_now() - s;
                total_time += delay_time[total_query];
//                time1 = get_now() - start;
//                time2 = (total_query * 1e6 / freq_);
//                time_t time3 = ((total_query+15) * 1e6 / freq_);
//                if (time1 >= time2 && time1 <= time3)
//                    cnt++;
//                if (time1 >= time2)
//                    delay_time[total_query] = time1 - time2;
//                else
//                {
//                    while (time2 >= time1){
//                        time1 = get_now() - start + 1e4;
//                        std::this_thread::sleep_for(std::chrono::nanoseconds(1));
//                    }
//                    cnt2++;
////                    cnt++;
//                }
//                delay_time[total_query] += 3000;
//                total_time += delay_time[total_query];
//                for (int j = 0; j < rplInfo.group_num; ++j) {
//                    last_cnt[j] = commiters[j]->trx_cnt;
//                }
//                std::this_thread::sleep_for(std::chrono::microseconds (1000000));
                if (stop || total_query >= stop_num){
                    std::cout << "delay time: " << total_time * 1.0 / 1e6 / total_query << "s total query: " << total_query << std::endl;
                    std::cout << cnt << "----" << cnt2 << std::endl;
                    for (int i = 1; i <= total_query; i++)
                        file << delay_time[i] / 1e6 << std::endl;
                    exit(0);
                }
            } // for (const auto &query : query_list)
        } // where true
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
