#include "commiter.h"

namespace rpl{
    uint64_t Commiter::no = 0;

    time_t get_now(){
        std::chrono::system_clock::time_point time_point_now = std::chrono::system_clock::now(); // 获取当前时间点
        std::chrono::system_clock::duration duration_since_epoch
                = time_point_now.time_since_epoch(); // 从1970-01-01 00:00:00到当前时间点的时长
        return std::chrono::duration_cast<std::chrono::microseconds>(duration_since_epoch).count(); // 将时长转换为微秒数
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
        commiter->push_trx_map(xid, trxRows);
        return 0;
    }

}