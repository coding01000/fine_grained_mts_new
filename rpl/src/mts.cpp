//#include "../include/mts.h"
//#include "event_handle.h"
//#include <functional>
//#include "ctime"
//#include "chrono"
//
//namespace rpl{
//    time_t get_now(){
//        std::chrono::system_clock::time_point time_point_now = std::chrono::system_clock::now(); // 获取当前时间点
//        std::chrono::system_clock::duration duration_since_epoch
//                = time_point_now.time_since_epoch(); // 从1970-01-01 00:00:00到当前时间点的时长
//        return std::chrono::duration_cast<std::chrono::microseconds>(duration_since_epoch).count(); // 将时长转换为微秒数
//    }
//
//    bool start = true;
//    std::string table_name = "tpcc.ORDER_LINE";
//    time_t pretime = get_now();
//    uint64_t trx = 0;
//    uint64_t event = 0;
//    uint64_t a = 0;
//    int64_t total_time = 0;
//    auto pool1 = new ThreadPool(16);
//    auto pool2 = new ThreadPool(3);
//    std::shared_ptr<std::vector<std::shared_ptr<event_buffer>>> buffers=NULL;
//
//    int MTS_Handler::init() {
//        commiters = std::vector<Commiter *>(2);
//        pool = new ThreadPool(16); // 3 128914
//        pool->init();
//        pool1->init();
//        pool2->init();
//        for (int i = 0; i < 2; ++i) {
//            commiters[i] = new Commiter(this);
//        }
//        eventFetcher = new Binlog_file_event_fetcher("/root/project/mts/mysql-bin.000032");
//        return 0;
//    }
//    int MTS_Handler::run() {
//        pretime = get_now();
//        for (const auto &commiter : commiters) {
//            commiter->commit();
//        }
//        do{
//            std::shared_ptr<event_buffer> eb(new event_buffer());
//            if (eventFetcher->fetch_a_event(eb->buffer, eb->length)){
//                printf("fetch event eof");
//                break;
//            }
//            ++event;
//            process(eb);
//        }while (true);
//        printf("..--------%lld-----%d\n", get_now()-pretime, pool->m_queue.size());
//        printf("total time of map event: %lld\n", total_time);
//        pool->shutdown();
//        pool1->shutdown();
//        pool2->shutdown();
//        printf("--------%lld-----%d\n", get_now()-pretime, pool->m_queue.size());
//        for (const auto &commiter : commiters) {
//            commiter->shut_down();
//        }
//        printf("------%lld-----%lld----%lld\n", get_now()-pretime, trx, event);
//        std::cout<<a<<std::endl;
//        return 0;
//    }
//    int MTS_Handler::process(std::shared_ptr<event_buffer> eb) {
//        uint8_t *buffer = eb->buffer;
//        if (!buffers){
//             buffers = std::make_shared<std::vector<std::shared_ptr<event_buffer>>>();
//        }
//        binary_log::Log_event_type type = (binary_log::Log_event_type)buffer[EVENT_TYPE_OFFSET];
//        switch (type) {
//            case binary_log::FORMAT_DESCRIPTION_EVENT:{
//                binary_log::Format_description_event *fde_tmp = new binary_log::Format_description_event(4, "8.017");
//                fde = new binary_log::Format_description_event(reinterpret_cast<const char *>(buffer), fde_tmp);
//                break;
//            }
//            case binary_log::XID_EVENT:{
//                auto xid_event = new binary_log::Xid_event(reinterpret_cast<const char *>(eb->buffer), fde);
//                uint64_t xid = xid_event->xid;
//                for (const auto &commiter : commiters) {
//                    commiter->push_commit_que(xid);
//                }
////                handle(buffers, xid);
//                pool->submit(std::bind(&MTS_Handler::handle, this, buffers, xid));
//                buffers = NULL;
//                ++trx;
//                break;
//            }
//            default:{
//                buffers->push_back(eb);
//                break;
//            }
//        }
//        return 0;
//    }
//
//    int MTS_Handler::handle(std::shared_ptr<std::vector<std::shared_ptr<event_buffer>>> buffers, uint64_t xid) {
//        Trx_info* trxinfo1 = commiters[0]->get_new_Trx_info(xid);
//        Trx_info* trxinfo2 = commiters[1]->get_new_Trx_info(xid);
//        std::vector<Trx_info *> trxinfo = {trxinfo1, trxinfo2};
//        for (auto it=buffers->begin();it!=buffers->end();it++){
//            handle_(*it, trxinfo);
//        }
//        pool1->submit(std::bind(&MTS_Handler::rows_parser, this, trxinfo1));
//        pool2->submit(std::bind(&MTS_Handler::rows_parser, this, trxinfo2));
//        return 0;
//    }
//    int MTS_Handler::handle_(std::shared_ptr<event_buffer> eb, std::vector<Trx_info *> trxinfo) {
//        uint8_t *buffer = eb->buffer;
//        binary_log::Log_event_type type = (binary_log::Log_event_type)buffer[EVENT_TYPE_OFFSET];
//        switch (type) {
//            case binary_log::TABLE_MAP_EVENT:{
//                auto *ev = new binary_log::Table_map_event(reinterpret_cast<const char *>(buffer), fde);
//                std::string full_table_name = ev->get_db_name()+'.'+ev->get_table_name();
//                auto it = tables.find(full_table_name);
//                auto *table_schema = eventHandler.unpack(ev);
//                //如果没有这个表则创建这个表，并构建schema
//                if (it == tables.end()){
//                    Table *table = new Table(full_table_name);
//                    table->schema = new binary_log::TableSchema(*table_schema);
//                    table->_pk = table_schema->getPrikey();
//                    tables[full_table_name] = table;
//                }
//                break;
//            }
//            case binary_log::WRITE_ROWS_EVENT:
//            case binary_log::DELETE_ROWS_EVENT:
//            case binary_log::UPDATE_ROWS_EVENT:{
//                auto *ev = new binary_log::Rows_event(reinterpret_cast<const char *>(buffer), fde);
//                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
//                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
//                if (table_full_name==table_name){
//                    trxinfo[0]->buffers.push_back(eb);
//                }else{
//                    trxinfo[1]->buffers.push_back(eb);
//                }
//                break;
//            }
//            default:
//                break;
//        }
//        return 0;
//    }
//
//    int MTS_Handler::rows_parser(Trx_info *trxinfo) {
//        trxinfo->trxRows = new Trx_rows(trxinfo->xid);
//        for (const auto &eb : trxinfo->buffers) {
//            uint8_t *buffer = eb->buffer;
//            binary_log::Log_event_type type = (binary_log::Log_event_type)buffer[EVENT_TYPE_OFFSET];
//            switch (type) {
//                case binary_log::WRITE_ROWS_EVENT: {
//                    auto *ev = new binary_log::Write_rows_event(reinterpret_cast<const char *>(buffer), fde);
//                    auto *table_schema = eventHandler.get_schema(ev->get_table_id());
//                    std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
//                    char *buf = new char[ev->row.size()];
//                    std::copy(ev->row.begin(), ev->row.end(), buf);
//                    auto reader = binary_log::Event_reader(buf, ev->row.size());
//                    int idx = table_schema->getIdxByName(table_schema->getPrikey());
//                    auto col = eventHandler.unpack(ev, reader, table_schema);
//                    Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_full_name, table_schema->getTablename());
//                    row->columns = col;
//                    trxinfo->trxRows->rows.push_back(row);
//                    break;
//                }
//                case binary_log::DELETE_ROWS_EVENT:{
//                    auto *ev = new binary_log::Delete_rows_event(reinterpret_cast<const char *>(buffer), fde);
//                    char *buf = new char[ev->row.size()];
//                    std::copy(ev->row.begin(), ev->row.end(), buf);
//                    auto reader = binary_log::Event_reader(buf, ev->row.size());
//                    auto *table_schema = eventHandler.get_schema(ev->get_table_id());
//                    std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
//                    auto it = tables.find(table_full_name);
//                    int idx = table_schema->getIdxByName(table_schema->getPrikey());
//                    auto col = eventHandler.unpack(ev, reader, table_schema);
//                    Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_full_name, table_schema->getTablename());
//                    row->columns = col;
//                    trxinfo->trxRows->rows.push_back(row);
//
//                    break;
//                }
//                case binary_log::UPDATE_ROWS_EVENT:{
//                    auto *ev = new binary_log::Update_rows_event(reinterpret_cast<const char *>(buffer), fde);
//                    char *buf = new char[ev->row.size()];
//                    std::copy(ev->row.begin(), ev->row.end(), buf);
//                    auto reader = binary_log::Event_reader(buf, ev->row.size());
//                    auto *table_schema = eventHandler.get_schema(ev->get_table_id());
//                    std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
//                    auto it = tables.find(table_full_name);
//                    int idx = table_schema->getIdxByName(table_schema->getPrikey());
//                    auto col = eventHandler.unpack(ev, reader, table_schema);
//                    Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_full_name, table_schema->getTablename());
//                    row->columns = col;
//                    trxinfo->trxRows->rows.push_back(row);
//                    break;
//                }
//                default:
//                    break;
//            }
//        }
//        trxinfo->commit();
//        return 0;
//    }
//
//    uint64_t Commiter::no = 0;
//
//    int Commiter::commit_(){
//        std::unique_lock<std::mutex> lock(commit_mu);
//        while (!commit_shut_down){
//            ++cnt;
//            while (commit_que.empty()){
//                commit_cv.wait(lock);
//            }
//
//            if (commit_shut_down&&commit_que.empty()){
//                break;
//            }
//            uint64_t now_xid = commit_que.front();
//            while (trx_map.is_(now_xid)){
//                commit_cv.wait(lock);
//            }
//            if (commit_shut_down&&commit_que.empty()){
//                break;
//            }
//            Trx_rows *trxRows = trx_map.get(now_xid);
//            for (auto it = trxRows->rows.begin(); it != trxRows->rows.end();it++)
//            {
//                auto table = mtsHandler->tables.find((*it)->full_name)->second;
//                table->insert_row(*it);
//            }
//            ++a;
//            trx_map.erase(now_xid);
//            commit_que.pop();
//
//            if (cnt==129293){
//                std::cout <<"aa"<< std::endl;
//                break;
//            }
////            delete trxRows;
//        }
//        std::cout<<name<<" "<<(get_now()-start)<<std::endl;
//        return 0;
//    }
//
//    int Commiter::commit(){
//        thread = std::thread(&Commiter::commit_, this);
//        start = get_now();
//        return 0;
//    }
//
//    void Commiter::shut_down() {
//        commit_shut_down = true;
//        commit_cv.notify_all();
//        thread.join();
//    }
//
//    int Commiter::push_commit_que(uint64_t xid) {
//        commit_que.push(xid);
//        return 0;
//    }
//
//    int Commiter::push_trx_map(uint64_t xid, Trx_rows *trxRows) {
//        trx_map.insert(xid, trxRows);
//        commit_cv.notify_all();
//        return 0;
//    }
//
//    Trx_info * Commiter::get_new_Trx_info(uint64_t xid) {
//        return new Trx_info(xid, this);
//    }
//
//    event_buffer::event_buffer() {
//        buffer = nullptr;
//        length = 0;
//    }
//    event_buffer::event_buffer(uint8_t *buf, int len) {
//        length = len;
//        buffer = buf;
//    }
//
//    int Trx_info::commit() {
//        commiter->push_trx_map(xid, trxRows);
//        return 0;
//    }
//
//    int Trx_dispenses::init() {
//        for (const auto &pool : pools) {
//            pool->init();
//        }
//        for (const auto &commiter : commiters) {
//            commiter->commit();
//        }
//        return 0;
//    }
//
//    int Trx_dispenses::shut_down() {
//        for (const auto &pool : pools) {
//            pool->shutdown();
//        }
//        for (const auto &commiter : commiters) {
//            commiter->shut_down();
//        }
//        return 0;
//    }
//
//    int Trx_dispenses::commit(uint32_t idx, Trx_info *trxInfo) {
////        pools[idx].
//    }
//}