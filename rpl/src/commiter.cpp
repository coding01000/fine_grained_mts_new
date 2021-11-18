#include "commiter.h"
#include <time.h>
#include <sys/time.h>
#include "algorithm"
#include <condition_variable>
#include "climits"
namespace rpl{
    uint64_t Commiter::no = 0;
    time_t get_now(){
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return tv.tv_sec * 1000000 + tv.tv_usec;
    }

    void Commiter::ff(uint64_t cnt){
        uint64_t tmp = get_now();
        f+=cnt;
        if (((tmp-start)/time_interval)>time_cnt){
            tmp = f;
            f=0;
            freq[time_cnt] = tmp;
//            f = 0;
            time_cnt++;
        }
    }

    int Commiter::commit_(){
        std::unique_lock<std::mutex> lock(commit_mu);

        uint64_t now_xid;
        uint64_t fail = 0ULL;
        start = get_now();
        trx_cnt = 0;
        total_trx = 0;
        time_interval = 1e6;
        time_cnt = 0;
        f = 0;
        idx = 0;
        cnt = 0;
        int64_t tmp = 0;

//        while ((!commit_shut_down)||(!commit_que.empty())){
        while ((!commit_shut_down)||(trx_cnt < total_trx)){
            ++trx_cnt;
//            while ((!commit_shut_down)&&commit_que.empty()){
            while ((!commit_shut_down)&&(trx_cnt >= total_trx)){
                commit_cv.wait(lock);
            }
//            if (commit_shut_down&&commit_que.empty()){
            if (commit_shut_down&&trx_cnt>=total_trx){
                break;
            }
//            now_xid = commit_que.front();
            now_xid = trx_cnt;
            while (!trx_map[now_xid].load()){
                fail++;
                commit_cv.wait(lock);
            }
//            commit_que.pop();
            if (commit_shut_down&&trx_cnt>=total_trx){
                break;
            }

            Trx_rows *trxRows = trx_map[now_xid];
            bool flag = false;
            for (auto it = trxRows->rows.begin(); it != trxRows->rows.end();it++)
            {
                flag = true;
                (*it)->table->insert_row(*it);
                commit_time = std::max(commit_time, (*it)->event_time);
            }
            if (!flag)
                ++cnt;
            trx_times[trx_cnt].finish_time = get_now();
        }
//        commit_time *= 2;
        used_time = get_now() - start;
        std::cout<<name<< " used time: "<< used_time <<" ";
        std::cout<<" freq: [";
        for (int i=0;i<time_cnt;i++) {
            std::cout<<freq[i]<<", ";
        }
        std::cout<<f<<" ]"<<std::endl;
        std::cout << "fail: " << fail << std::endl;
        std::ofstream file1;
        file1.open("/root/project/mts_cp/"+name);
        int64_t t1=0, t2=0, t3=0;
        for (int i = 1; i < trx_cnt; ++i) {
            t1 += trx_times[i].dis_time - trx_times[i].start_time;
            t2 += trx_times[i].parse_time - trx_times[i].dis_time;
            t3 += trx_times[i].finish_time - trx_times[i].parse_time;
//            t1 += trx_times[i].dis_time > trx_times[i].start_time ? trx_times[i].dis_time - trx_times[i].start_time : 10;
//            t2 += trx_times[i].parse_time > trx_times[i].dis_time ? trx_times[i].parse_time - trx_times[i].dis_time : 10;
//            t3 += trx_times[i].finish_time > trx_times[i].parse_time ? trx_times[i].finish_time - trx_times[i].parse_time : 10;
//            file1 << trx_times[i].parse_time - trx_times[i].start_time << " " << trx_times[i].finish_time - trx_times[i].start_time << std::endl;
        }
        std::cout << name << "-- 分组：" << t1 / trx_cnt << "  parse time: " << t2 / trx_cnt << "  finish time: " << t3 / trx_cnt << std::endl;
        return 0;
    }

    int Commiter::commit(){
        memset(trx_map, 0, sizeof(trx_map));
        total_trx = ULLONG_MAX;
        thread = std::thread(&Commiter::commit_, this);
        start = get_now();
        return 0;
    }

    void Commiter::shut_down() {
        commit_shut_down = true;
        commit_cv.notify_all();
//        thread.join();
    }

    int Commiter::push_commit_que(uint64_t xid) {
        commit_que.push(xid);
        return 0;
    }

    int Commiter::push_trx_map(uint64_t xid, Trx_rows *trxRows) {
//        trx_map.insert(xid, trxRows);
        trx_map[xid] = trxRows;
        commit_cv.notify_all();
        return 0;
    }

    Trx_info * Commiter::get_new_Trx_info(uint64_t xid) {
        return new Trx_info(xid, this);
    }

    event_buffer::event_buffer() {
        buffer = nullptr;
        length = 0;
    }
    event_buffer::event_buffer(uint8_t *buf, int len) {
        length = len;
        buffer = buf;
    }

    int Trx_info::commit() {
//        if (commiter->name == "commiter1"){
//            std::cout<<"commiter1 push map: "<<xid <<";"<<std::endl;
//        }
//        if (commiter->name == "commiter0"){
//            std::cout<<"commiter0 push map: "<<xid <<";"<<std::endl;
//        }
//        if (commiter->name == "commiter1" && xid==384){
//            std::cout<<"commiter1 push map: "<<xid <<";"<<std::endl;
//        }
//        commiter->f++;
        commiter->push_trx_map(xid, trxRows);
        return 0;
    }

}
