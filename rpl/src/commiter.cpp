#include "commiter.h"
#include <time.h>
#include <sys/time.h>
#include <condition_variable>
namespace rpl{
    uint64_t Commiter::no = 0;
    struct timeval tv;
    time_t get_now(){
        gettimeofday(&tv, NULL);
        return tv.tv_sec * 1000000 + tv.tv_usec;
    }
    int Commiter::commit_(){
        std::unique_lock<std::mutex> lock(commit_mu);
        while ((!commit_shut_down)||(!commit_que.empty())){
            ++cnt;
            while ((!commit_shut_down)&&commit_que.empty()){
                commit_cv.wait(lock);
            }
            if (commit_shut_down&&commit_que.empty()){
                break;
            }
            uint64_t now_xid = commit_que.front();
            while (trx_map.is_(now_xid)){
                commit_cv.wait(lock);
            }
            if (commit_shut_down&&commit_que.empty()){
                break;
            }
            Trx_rows *trxRows = trx_map.get(now_xid);
            for (auto it = trxRows->rows.begin(); it != trxRows->rows.end();it++)
            {
                auto table = tables->find((*it)->full_name)->second;
                table->insert_row(*it);
            }
            trx_map.erase(now_xid);
//            if(cnt % 100 == 0 && name == "commiter1")
//                std::cout << name<< " cnt: " << cnt << " "<< trx_map._map.size() << std::endl;
            commit_que.pop();

//            if (cnt==311888){
//                std::cout <<"aa"<< std::endl;
//                break;
//            }
        }
        std::cout<<name<<" "<<(get_now()-start)<<" "<<cnt<<std::endl;
        return 0;
    }

    int Commiter::commit(){
        thread = std::thread(&Commiter::commit_, this);
//        cpu_set_t cpuset;
//        CPU_ZERO(&cpuset);
//        CPU_SET(no++, &cpuset);
//        pthread_setaffinity_np(thread.native_handle(),
//                               sizeof(cpu_set_t), &cpuset);
//        thread
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
        trx_map.insert(xid, trxRows);
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
        commiter->push_trx_map(xid, trxRows);
        return 0;
    }

}
