#include "commiter.h"
#include <time.h>
#include <sys/time.h>
#include "algorithm"
#include <condition_variable>
#include "climits"
namespace rpl{
    uint64_t Commiter::no = 0;
    struct timeval tv;
    time_t get_now(){
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
        time_interval = 1e6;
        time_cnt = 0;
        f = 0;
        idx = 0;
        int64_t tmp = 0;

//        uint64_t ev_cnt = 0;
//        auto thread_fre = std::thread();
        while ((!commit_shut_down)||(!commit_que.empty())){
            ++cnt;
            trx_cnt ++;
            while ((!commit_shut_down)&&commit_que.empty()){
                commit_cv.wait(lock);
//                ff(get_now());
            }
            if (commit_shut_down&&commit_que.empty()){
                break;
            }
//            ff(get_now());
            now_xid = commit_que.front();
//            while (trx_map.is_(now_xid)){
//            tmp = 0;
            while (!trx_map[now_xid].load()){
                fail++;
//                tmp++;
//                if (tmp >= 2)
//                    break;
//                std::this_thread::sleep_for(std::chrono::nanoseconds(1));
                commit_cv.wait(lock);
//                ff(get_now(),start,time_interval,time_cnt,f,freq);
            }
            commit_que.pop();
//            if (tmp >= 2)
//                continue;
//            ff(get_now(),start,time_interval,time_cnt,f,freq);
            if (commit_shut_down&&commit_que.empty()){
                break;
            }
//            Trx_rows *trxRows = trx_map.get(now_xid);
            Trx_rows *trxRows = trx_map[now_xid];
            for (auto it = trxRows->rows.begin(); it != trxRows->rows.end();it++)
            {
//                auto table = tables->find((*it)->full_name)->second;
//                std::cout<<(*it)->full_name<<std::endl;
//                f++;
                (*it)->table->insert_row(*it);
                commit_time = std::max(commit_time, (*it)->event_time);
            }
//            ff(1);
//            uint64_t tmp = get_now();

//            trx_map.erase(now_xid);
//            if(cnt % 100 == 0 && name == "commiter1")[539219, 576459, 2 ]
//                std::cout << name<< " cnt: " << cnt << " "<< trx_map._map.size() << std::endl;
        }
        commit_time *= 2;
        used_time = get_now() - start;
        std::cout<<name<< " used time: "<< used_time <<" ";
        std::cout<<" freq: [";
        for (int i=0;i<time_cnt;i++) {
            std::cout<<freq[i]<<", ";
        }
        std::cout<<f<<" ]"<<std::endl;
        std::cout << "fail: " << fail << std::endl;
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
