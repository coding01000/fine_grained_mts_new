#ifndef FINE_GRAINED_MTS_COMMITER_H
#define FINE_GRAINED_MTS_COMMITER_H
#include "cstdint"
#include "event_handle.h"
#include "row.h"
#include "safe_map.h"
#include "thread"
#include "queue"
#include "table.h"
#include "boost/lockfree/queue.hpp"

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
        std::list<std::shared_ptr<event_buffer> > buffers;
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
        SafeMap<uint64_t, Trx_rows*> trx_map;
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
        static uint64_t no;

    public:
        int commit();
        int commit_();
        Commiter(std::unordered_map<std::string, Table *> *_tables, std::string str):tables(_tables), cnt(0), name(str), commit_shut_down(false){}
        Commiter(std::unordered_map<std::string, Table *> *_tables):tables(_tables), cnt(0), name("commiter"+std::to_string(no++)), commit_shut_down(false){}
        void shut_down();
        int push_trx_map(uint64_t xid, Trx_rows *trxRows);
        int push_commit_que(uint64_t xid);
        Trx_info* get_new_Trx_info(uint64_t xid);
    };
}

#endif //FINE_GRAINED_MTS_COMMITER_H
