#ifndef FINE_GRAINED_MTS_COMMITER_H
#define FINE_GRAINED_MTS_COMMITER_H
#include "cstdint"
#include "event_handle.h"
#include "row.h"
#include "safe_map.h"
#include "thread"
#include "queue"
#include "table.h"
#include <condition_variable>
#include "atomic"

namespace rpl{
//    extern time_t get_now();
    class Commiter;

    //event buffer包装
    class event_buffer{
    public:
        uint8_t *buffer;
        int length;
        event_buffer();
        event_buffer(uint8_t* buf, int len);
    };

    //一个事务的所有行变换
    class Trx_rows{
    public:
        std::list<Row *> rows;
        uint64_t xid;
    public:
        Trx_rows(){}
        Trx_rows(uint64_t _xid): xid(_xid){}
    };

    //事务的信息，以及事务要提交的线程commiter
    class Trx_info{
    public:
        std::list<event_buffer*> buffers;
        Trx_rows *trxRows;
        Commiter *commiter;
        uint64_t xid;
    public:
        Trx_info(){}
        Trx_info(uint64_t _xid, Commiter *_commiter): xid(_xid), commiter(_commiter), trxRows(new Trx_rows(_xid)){}
        int commit();
    };

    class Commiter{
    public:
//        SafeMap<uint64_t, Trx_rows*> trx_map;
        std::atomic<Trx_rows *> trx_map[4000000];
        std::queue<uint64_t> commit_que;
//        boost::lockfree::queue<uint64_t> commit_que{500000};
//        std::mutex que_mu;
        std::condition_variable commit_cv;
        std::mutex commit_mu;
        std::unordered_map<std::string, Table *> *tables;
        bool commit_shut_down;
        std::thread thread;
        uint64_t start;
        uint64_t cnt;
        std::string name;
        std::atomic<int64_t> trx_cnt;
        static uint64_t no;
        time_t commit_time;
        time_t used_time;

        std::atomic<uint64_t> f;
//        uint64_t start_time;
        uint64_t time_interval;
        std::atomic<uint64_t> time_cnt;
        std::atomic<uint64_t> freq[100];
        std::atomic<uint64_t> idx;
        uint64_t total_trx;

    public:
//        uint64_t

        int commit();
        int commit_();
        Commiter(std::unordered_map<std::string, Table *> *_tables, std::string str):tables(_tables), cnt(0), name(str), commit_shut_down(false){}
        Commiter(std::unordered_map<std::string, Table *> *_tables):tables(_tables), cnt(0), name("commiter"+std::to_string(no++)), commit_shut_down(false){}
        void shut_down();
        int push_trx_map(uint64_t xid, Trx_rows *trxRows);
        int push_commit_que(uint64_t xid);
        Trx_info* get_new_Trx_info(uint64_t xid);

        void ff(uint64_t);
    };
}

#endif //FINE_GRAINED_MTS_COMMITER_H
