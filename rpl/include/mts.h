#ifndef FINE_GRAINED_MTS_MTS_H
#define FINE_GRAINED_MTS_MTS_H
#include "table.h"
#include "event_handle.h"
#include "thread_pool.h"
#include "master_info.h"
#include "remote_event_fetcher.h"
#include "binlog_file_event_fetcher.h"

namespace rpl{

    class Commiter;

    class event_buffer{
    public:
        uint8_t *buffer;
        int length;
        event_buffer();
        event_buffer(uint8_t* buf, int len);
    };

    class Trx_rows{
    public:
        std::list<Row *> rows;
        uint64_t xid;
        friend bool operator < (const Trx_rows &a, const Trx_rows &b){
            return a.xid > b.xid;
        }
    };

    class MTS_Handler{
    private:
        Event_fetcher *eventFetcher;
        binary_log::Format_description_event *fde;
        binary_log::Event_Handler eventHandler;
        ThreadPool *pool;
        ThreadPool* process_submiter;
        std::unordered_map<std::string, SafeQueue<Row *> * > q_map; //未执行的序列
        Commiter *commiter1;
        Commiter *commiter2;
    public:
        std::unordered_map<std::string, Table *> tables;
//        std::unordered_map<uint64_t, Trx_rows*> trx_map;
    public:
        int init();
        int run();
    protected:
//        void process(uint8_t *buffer, int length);
        int process(std::shared_ptr<event_buffer> eb);
        int handle_(std::shared_ptr<event_buffer> eb, std::vector<Trx_rows *> trxRows);
        int handle(std::shared_ptr<std::vector<std::shared_ptr<event_buffer>>> buffers, uint64_t xid);

    };

    class Commiter{
    private:
        SafeMap<uint64_t, Trx_rows*> trx_map;
        std::queue<uint64_t> commit_que;
        std::condition_variable commit_cv;
        std::mutex commit_mu;
        MTS_Handler *mtsHandler;
        bool commit_shut_down;
        std::thread thread;
        uint64_t start;
        uint64_t cnt;
        std::string name;

    public:
        int commit();
        int commit_();
        Commiter(MTS_Handler *handler, std::string str):mtsHandler(handler), cnt(0), name(str), commit_shut_down(false){}
        void shut_down();
        int push_trx_map(uint64_t xid, Trx_rows *trxRows);
        int push_commit_que(uint64_t xid);
    };

}

#endif //FINE_GRAINED_MTS_MTS_H
