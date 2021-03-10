#include "../include/mts.h"
#include "event_handle.h"
#include <functional>
#include "ctime"
#include "chrono"

namespace rpl{
    time_t get_now(){
        std::chrono::system_clock::time_point time_point_now = std::chrono::system_clock::now(); // 获取当前时间点
        std::chrono::system_clock::duration duration_since_epoch
                = time_point_now.time_since_epoch(); // 从1970-01-01 00:00:00到当前时间点的时长
        return std::chrono::duration_cast<std::chrono::microseconds>(duration_since_epoch).count(); // 将时长转换为微秒数
    }

    bool start = true;
    std::string table_name = "tpcc.ORDER_LINE";
    time_t pretime = get_now();
    uint64_t trx = 0;
    uint64_t event = 0;
    uint64_t a = 0;
//    std::vector<std::shared_ptr<event_buffer>>* buffers=NULL;129145
    std::shared_ptr<std::vector<std::shared_ptr<event_buffer>>> buffers=NULL;

    int MTS_Handler::init() {
        pool = new ThreadPool(6); // 3 128914
        pool->init();
        commiter1 = new Commiter(this, "com1");
        commiter2 = new Commiter(this, "com2");
        eventFetcher = new Binlog_file_event_fetcher("/Users/mac/CLionProjects/fine_grained_mts/mysql-bin.000032");
//        eventFetcher = new Remote_event_fetcher();
        return 0;
    }
    int MTS_Handler::run() {
        commiter1->commit();
        commiter2->commit();
        do{
            std::shared_ptr<event_buffer> eb(new event_buffer());
            if (eventFetcher->fetch_a_event(eb->buffer, eb->length)){
                printf("fetch event eof");
                break;
            }
            ++event;
            process(eb);
        }while (true);
        printf("--------%lld-----%d\n", get_now()-pretime, pool->m_queue.size());
        pool->shutdown();
        printf("--------%lld-----%d\n", get_now()-pretime, pool->m_queue.size());
        commiter1->shut_down();
        commiter2->shut_down();
        printf("------%lld-----%lld----%lld\n", get_now()-pretime, trx, event);
        std::cout<<a<<std::endl;
//        std::cout<<commit_que.size()<<" "<<trx_que.size()<<std::endl;
//        std::cout<<trx_que.q.top().xid<<std::endl;
//        trx_que.q.pop();
//        std::cout<<trx_que.q.top().xid<<std::endl;
        return 0;
    }
    int n = 1;
    int cnt = 0;
    int MTS_Handler::process(std::shared_ptr<event_buffer> eb) {
        uint8_t *buffer = eb->buffer;
        if (!buffers){
             buffers = std::make_shared<std::vector<std::shared_ptr<event_buffer>>>();
        }
        binary_log::Log_event_type type = (binary_log::Log_event_type)buffer[EVENT_TYPE_OFFSET];
        switch (type) {
            case binary_log::FORMAT_DESCRIPTION_EVENT:{
                binary_log::Format_description_event *fde_tmp = new binary_log::Format_description_event(4, "8.017");
                fde = new binary_log::Format_description_event(reinterpret_cast<const char *>(buffer), fde_tmp);
                break;
            }
            case binary_log::XID_EVENT:{
//                auto xid_event = new binary_log::Xid_event(reinterpret_cast<const char *>(eb->buffer), fde);
                uint64_t xid = n++;
                commiter1->push_commit_que(xid);
                commiter2->push_commit_que(xid);
//                handle(buffers, xid);
                pool->submit(std::bind(&MTS_Handler::handle, this, buffers, xid));
                buffers = NULL;
                ++trx;
                break;
            }
            case binary_log::TABLE_MAP_EVENT:{
                auto *ev = new binary_log::Table_map_event(reinterpret_cast<const char *>(buffer), fde);
 //               std::string full_table_name = ev->get_db_name()+'.'+ev->get_table_name();
//                auto it = tables.find(full_table_name);
//                auto *table_schema = eventHandler.unpack(ev);
//                //如果没有这个表则创建这个表，并构建schema
//                if (it == tables.end()){
//                    std::cout << ++cnt<< std::endl;
//                    Table *table = new Table(full_table_name);
//                    table->schema = new binary_log::TableSchema(*table_schema);
//                    table->_pk = table_schema->getPrikey();
//                    tables[full_table_name] = table;
//                }
//                break;
            }
            default:{
                buffers->push_back(eb);
                break;
            }
        }
        return 0;
    }

    int MTS_Handler::handle(std::shared_ptr<std::vector<std::shared_ptr<event_buffer>>> buffers, uint64_t xid) {
        Trx_rows* trxRows1 = new Trx_rows();
        Trx_rows* trxRows2 = new Trx_rows();
        trxRows1->xid = xid;
        trxRows2->xid = xid;
        std::vector<Trx_rows *> trxRows = {trxRows1, trxRows2};
        for (auto it=buffers->begin();it!=buffers->end();it++){
            handle_(*it, trxRows);
        }
//        if (!trxRows1->rows.empty())
        commiter1->push_trx_map(xid, trxRows1);
//        if (!trxRows2->rows.empty())
        commiter2->push_trx_map(xid, trxRows2);
        return 0;
    }
    int MTS_Handler::handle_(std::shared_ptr<event_buffer> eb, std::vector<Trx_rows *> trxRows) {
        uint8_t *buffer = eb->buffer;
        int length = eb->length;

        binary_log::Log_event_type type = (binary_log::Log_event_type)buffer[EVENT_TYPE_OFFSET];
        switch (type) {
            case binary_log::ANONYMOUS_GTID_LOG_EVENT: {
                start = false;
                break;
            }
            case binary_log::XID_EVENT: {
                start = true;
                break;
            }
            case binary_log::FORMAT_DESCRIPTION_EVENT: {
//                printf("FORMAT_DESCRIPTION_EVENT!\n");
                binary_log::Format_description_event *fde_tmp = new binary_log::Format_description_event(4, "8.017");
                char *buf = (char *) malloc(sizeof(char *) * (length + 1));
                memcpy(buf, buffer, length);
                fde = new binary_log::Format_description_event(reinterpret_cast<const char *>(buffer), fde_tmp);
//                fde->print_event_info(std::cout);
                break;
            }
            case binary_log::TABLE_MAP_EVENT:{
//                printf("TABLE_MAP_EVENT!\n");
//                uint64_t start = get_now();
                auto *ev = new binary_log::Table_map_event(reinterpret_cast<const char *>(buffer), fde);
                std::string full_table_name = ev->get_db_name()+'.'+ev->get_table_name();
                auto it = tables.find(full_table_name);
                auto *table_schema = eventHandler.unpack(ev);
                //如果没有这个表则创建这个表，并构建schema
                if (it == tables.end()){
                    Table *table = new Table(full_table_name);
                    table->schema = new binary_log::TableSchema(*table_schema);
                    table->_pk = table_schema->getPrikey();
                    tables[full_table_name] = table;
                }
//                delete ev;
                break;
            }
//            下面三个rows event完全可以使用一套代码，主要处理逻辑的差别在Event_Handler中的unpack中
//             但是考虑到后面可能会加不一样的处理逻辑，在这是分开处理的
            case binary_log::WRITE_ROWS_EVENT: {
//                printf("WRITE_ROWS_EVENTS!\n");
                auto *ev = new binary_log::Write_rows_event(reinterpret_cast<const char *>(buffer), fde);
                char *buf = new char[ev->row.size()];
                std::copy(ev->row.begin(), ev->row.end(), buf);
                auto reader = binary_log::Event_reader(buf, ev->row.size());
                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                auto it = tables.find(table_full_name);
                if (it==tables.end()){
                    printf("No Match Table!\n");
                }
//                std::cout<<table_full_name<<std::endl;
                int idx = table_schema->getIdxByName(table_schema->getPrikey());
                auto col = eventHandler.unpack(ev, reader, table_schema);
                Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_full_name, table_schema->getTablename());
//                Row *row = new Row(col[idx], now, false, table_schema->getDBname(), table_schema->getTablename());
                row->columns = col;
                if (table_full_name==table_name){
                    trxRows[0]->rows.push_back(row);

                }else{
                    trxRows[1]->rows.push_back(row);
                }
//                table->insert_row(row);
//                delete ev;
//                commit_cv.notify_all();
//                insert_a_record(table, row);
//                pool->submit(std::bind(&MTS_Handler::insert_a_record, this, table, row));
                break;
            }
            case binary_log::DELETE_ROWS_EVENT:{
//                printf("DELETE_ROWS_EVENT!\n");
                auto *ev = new binary_log::Delete_rows_event(reinterpret_cast<const char *>(buffer), fde);
                char *buf = new char[ev->row.size()];
                std::copy(ev->row.begin(), ev->row.end(), buf);
                auto reader = binary_log::Event_reader(buf, ev->row.size());
                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                auto it = tables.find(table_full_name);
                if (it==tables.end()){
                    printf("No Match Table!\n");
                }
                int idx = table_schema->getIdxByName(table_schema->getPrikey());
                auto col = eventHandler.unpack(ev, reader, table_schema);
                Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_full_name, table_schema->getTablename());
//                Row *row = new Row(col[idx], now, false, table_schema->getDBname(), table_schema->getTablename());
                row->columns = col;
                if (table_full_name==table_name){
                    trxRows[0]->rows.push_back(row);

                }else{
                    trxRows[1]->rows.push_back(row);
                }
//                trxRows->rows.push_back(row);
//                delete ev;
//                commit_cv.notify_all();
//                table->insert_row(row);
//                insert_a_record(table, row);
//                pool->submit(std::bind(&MTS_Handler::insert_a_record, this, table, row));
                break;
            }
            case binary_log::UPDATE_ROWS_EVENT:{
//                printf("UPDATE_ROWS_EVE NT!\n");
                auto *ev = new binary_log::Update_rows_event(reinterpret_cast<const char *>(buffer), fde);
                char *buf = new char[ev->row.size()];
                std::copy(ev->row.begin(), ev->row.end(), buf);
                auto reader = binary_log::Event_reader(buf, ev->row.size());
                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                auto it = tables.find(table_full_name);
                if (it==tables.end()){
                    printf("No Match Table!\n");
                }
                int idx = table_schema->getIdxByName(table_schema->getPrikey());
                auto col = eventHandler.unpack(ev, reader, table_schema);
                Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_full_name, table_schema->getTablename());
//                Row *row = new Row(col[idx], now, false, table_schema->getDBname(), table_schema->getTablename());
                row->columns = col;
                if (table_full_name==table_name){
                    trxRows[0]->rows.push_back(row);

                }else{
                    trxRows[1]->rows.push_back(row);
                }
//                row->full_name = table_full_name;
//                trxRows->rows.push_back(row);
//                delete ev;
//                commit_cv.notify_all();
//                table->insert_row(row);
//                insert_a_record(table, row);
//                pool->submit(std::bind(&MTS_Handler::insert_a_record, this, table, row));
                break;
            }
            default:
                break;
        }
        return 0;
    }


    int Commiter::commit_(){
        std::unique_lock<std::mutex> lock(commit_mu);
        while (!commit_shut_down){
            ++cnt;
            while ((!commit_shut_down)&&(commit_que.empty())){
                commit_cv.wait(lock);
            }

            if (commit_shut_down&&commit_que.empty()){
                break;
            }
            uint64_t now_xid = commit_que.front();
            while ((!commit_shut_down)&&(!trx_map.find(now_xid))){
                commit_cv.wait(lock);
            }
            if (commit_shut_down&&commit_que.empty()){
                break;
            }
            Trx_rows *trxRows = trx_map.get(now_xid);
            for (auto it = trxRows->rows.begin(); it != trxRows->rows.end();it++)
            {
                auto table = mtsHandler->tables.find((*it)->full_name)->second;
//                std::cout<<(*it)->table_name<<" "<<(*it)->columns[0]<<" "<<(*it)->columns[1]<<std::endl;
                table->insert_row(*it);
            }
            ++a;
            trx_map.erase(now_xid);
            commit_que.pop();
            if (cnt==129293){
                std::cout <<"aa"<< std::endl;
                break;
            }
//            delete trxRows;
        }
        std::cout<<name<<" "<<(get_now()-start)<<std::endl;
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
        thread.join();
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

    event_buffer::event_buffer() {
        buffer = nullptr;
        length = 0;
    }
    event_buffer::event_buffer(uint8_t *buf, int len) {
        length = len;
        buffer = buf;
    }
}