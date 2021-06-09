//#ifndef FINE_GRAINED_MTS_MTS_H
//#define FINE_GRAINED_MTS_MTS_H
//#include "table.h"
//#include "event_handle.h"
//#include "thread_pool.h"
//#include "master_info.h"
//#include "remote_event_fetcher.h"
//#include "binlog_file_event_fetcher.h"
//#include "table_schema.h"
//#include "safe_map.h"
//
//namespace rpl{
//
//    class Commiter;
//    class event_buffer;
//    class Trx_rows;
//    class Trx_info;
//
//    class MTS_Handler{
//    private:
//        Event_fetcher *eventFetcher;
//        binary_log::Format_description_event *fde;
//        binary_log::Event_Handler eventHandler;
//        ThreadPool *pool;
//        std::vector<Commiter *> commiters;
//    public:
//        std::unordered_map<std::string, Table *> tables;
//    public:
//        int init();
//        int run();
//    protected:
//        int process(std::shared_ptr<event_buffer> eb);
//        int handle_(std::shared_ptr<event_buffer> eb, std::vector<Trx_info *> trxBuffer);
//        int handle(std::shared_ptr<std::vector<std::shared_ptr<event_buffer>>> buffers, uint64_t xid);
//        int rows_parser(Trx_info *trxBuffer);
//
//    };
//
//    class Trx_dispenses{
//    public:
//        std::vector<Commiter *> commiters;
//        std::vector<ThreadPool *> pools;
//        uint32_t n;
//    public:
//        Trx_dispenses(){}
//        int init();
//        int shut_down();
//        int dispenses();
//        int commit(uint32_t idx, Trx_info *trxInfo);
//    };
//
//    class Commiter{
//
//    public:
//        SafeMap<uint64_t, Trx_rows*> trx_map;
//        std::queue<uint64_t> commit_que;
//        std::condition_variable commit_cv;
//        std::mutex commit_mu;
//        MTS_Handler *mtsHandler;
//        bool commit_shut_down;
//        std::thread thread;
//        uint64_t start;
//        uint64_t cnt;
//        std::string name;
//        static uint64_t no;
//
//    public:
//        int commit();
//        int commit_();
//        Commiter(MTS_Handler *handler, std::string str):mtsHandler(handler), cnt(0), name(str), commit_shut_down(false){}
//        Commiter(MTS_Handler *handler):mtsHandler(handler), cnt(0), name("commiter"+no), commit_shut_down(false){}
//        void shut_down();
//        int push_trx_map(uint64_t xid, Trx_rows *trxRows);
//        int push_commit_que(uint64_t xid);
//        Trx_info* get_new_Trx_info(uint64_t xid);
//    };
//
//    class event_buffer{
//    public:
//        uint8_t *buffer;
//        int length;
//        event_buffer();
//        event_buffer(uint8_t* buf, int len);
//    };
//
//    class Trx_rows{
//    public:
//        std::list<Row *> rows;
//        uint64_t xid;
//        friend bool operator < (const Trx_rows &a, const Trx_rows &b){
//            return a.xid > b.xid;
//        }
//
//    public:
//        Trx_rows(){}
//        Trx_rows(uint64_t _xid): xid(_xid){}
//    };
//
//    class Trx_info{
//    public:
//        std::list<std::shared_ptr<event_buffer> > buffers;
//        binary_log::TableSchema *tableSchema;
//        Trx_rows *trxRows;
//        Commiter *commiter;
//        uint64_t xid;
//    public:
//        Trx_info(){}
//        Trx_info(uint64_t _xid, Commiter *_commiter): xid(_xid), commiter(_commiter), trxRows(new Trx_rows(_xid)){}
//        int commit();
//    };
//}
//
//#endif //FINE_GRAINED_MTS_MTS_H
