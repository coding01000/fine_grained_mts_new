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
    time_t pretime = get_now();
    std::vector<event_buffer>* buffers=NULL;

    int MTS_Handler::init() {
        pool = new ThreadPool(5);
        process_submiter = new ThreadPool(1);
        process_submiter->init();
        pool->init();
        eventFetcher = new Binlog_file_event_fetcher("/Users/mac/CLionProjects/fine_grained_mts/mysql-bin.000032");
//        eventFetcher = new Remote_event_fetcher();
        return 0;
    }
//    auto process_submiter = new ThreadPool(1);
    int MTS_Handler::run() {
        uint8_t *buf;
        int length;
        do{
            if (eventFetcher->fetch_a_event(buf, length)){
                printf("fetch event eof");
                break;
            }
//            if (start){
//                pretime = get_now();
//            }
//            process_submiter->submit(std::bind(&MTS_Handler::handle_, this, buf, length));
            process_submiter->submit(std::bind(&MTS_Handler::process, this, buf, length));
//            handle_(buf, length);
//            handle(buf, length);
        }while (true);
        process_submiter->shutdown();
        pool->shutdown();
        printf("------%lld\n", get_now()-pretime);
    }

    void MTS_Handler::process(uint8_t *buffer, int length) {
        if (!buffers){
             buffers = new std::vector<event_buffer>();
        }
        binary_log::Log_event_type type = (binary_log::Log_event_type)buffer[EVENT_TYPE_OFFSET];
        switch (type) {
            case binary_log::FORMAT_DESCRIPTION_EVENT:{
                printf("FORMAT_DESCRIPTION_EVENT!\n");
                binary_log::Format_description_event *fde_tmp = new binary_log::Format_description_event(4, "8.017");
                char *buf = (char *) malloc(sizeof(char *) * (length + 1));
                memcpy(buf, buffer, length);
                fde = new binary_log::Format_description_event(reinterpret_cast<const char *>(buffer), fde_tmp);
                break;
            }
            case binary_log::XID_EVENT:{
                pool->submit(std::bind(&MTS_Handler::handle, this, buffers));
                buffers = NULL;
                break;
            }
            default:{
                event_buffer tmp;
                tmp.buffer = new uint8_t[length];
                memcpy(tmp.buffer, buffer, length);
                tmp.length = length;
                buffers->push_back(tmp);
                break;
            }
        }
    }

    int MTS_Handler::handle(std::vector<event_buffer> *buffers) {

        for (auto it=buffers->begin();it!=buffers->end();it++){
            uint8_t *buffer = it->buffer;
            int length = it->length;
            handle_(buffer, length);
        }
    }
    int MTS_Handler::handle_(uint8_t *buffer, int length) {

        time_t now = get_now(); // 将时长转换为微秒数

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
                auto *ev = new binary_log::Table_map_event(reinterpret_cast<const char *>(buffer), fde);
                std::string full_table_name = ev->get_db_name()+'.'+ev->get_table_name();
                auto it = tables.find(full_table_name);
                //如果没有这个表则创建这个表，并构建schema
                if (it == tables.end()){
                    auto *table_schema = eventHandler.unpack(ev);
                    Table *table = new Table(full_table_name);
                    table->schema = new binary_log::TableSchema(*table_schema);
                    table->_pk = table_schema->getPrikey();
                    tables[full_table_name] = table;
                }
                break;
            }
//            下面三个rows event完全可以使用一套代码，主要处理逻辑的差别在Event_Handler中的unpack中
//             但是考虑到后面可能会加不一样的处理逻辑，在这是分开处理的
            case binary_log::WRITE_ROWS_EVENT: {
                printf("WRITE_ROWS_EVENTS!\n");
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
                Table *table = it->second;
                int idx = table_schema->getIdxByName(table_schema->getPrikey());
                auto col = eventHandler.unpack(ev, reader, table_schema);
                Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_schema->getDBname(), table_schema->getTablename());
//                Row *row = new Row(col[idx], now, false, table_schema->getDBname(), table_schema->getTablename());
                row->process_time = now;
                row->columns = col;
//                table->insert_row(row);
                insert_a_record(table, row);
//                pool->submit(std::bind(&MTS_Handler::insert_a_record, this, table, row));
                break;
            }
            case binary_log::DELETE_ROWS_EVENT:{
                printf("DELETE_ROWS_EVENT!\n");
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
                Table *table = it->second;
                int idx = table_schema->getIdxByName(table_schema->getPrikey());
                auto col = eventHandler.unpack(ev, reader, table_schema);
                Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_schema->getDBname(), table_schema->getTablename());
//                Row *row = new Row(col[idx], now, false, table_schema->getDBname(), table_schema->getTablename());
                row->process_time = now;
                row->columns = col;
                insert_a_record(table, row);
//                pool->submit(std::bind(&MTS_Handler::insert_a_record, this, table, row));
                break;
            }
            case binary_log::UPDATE_ROWS_EVENT:{
                printf("UPDATE_ROWS_EVE NT!\n");
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
                Table *table = it->second;
                int idx = table_schema->getIdxByName(table_schema->getPrikey());
                auto col = eventHandler.unpack(ev, reader, table_schema);
                Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_schema->getDBname(), table_schema->getTablename());
//                Row *row = new Row(col[idx], now, false, table_schema->getDBname(), table_schema->getTablename());
                row->process_time = now;
                row->columns = col;
                insert_a_record(table, row);
//                pool->submit(std::bind(&MTS_Handler::insert_a_record, this, table, row));
                break;
            }
            default:
//                printf("Other Events!\n");
//                for (auto k=tables.begin();k!=tables.end();k++){4047690 4459052

//                    std::cout<<"table:"<<k->first<<std::endl;
//                    auto r= k->second->rows;
//                    for (auto it=r.begin();it!=r.end();it++){
//                        std::cout<<it->first<<" ";
//                        if (it->second->next){
//                            std::cout<<it->second->next->columns[0]<<it->second->next->columns[1]<<it->second->next->columns[2];
//                        }
//                        std::cout<<std::endl;
//                    }
//                }
                break;
        }
    }

    int MTS_Handler::insert_a_record(Table *table, Row *row) {

        //可以把入队代码放到解析事务的代码中一起加锁，保证入队顺序的严格一致
        auto it = q_map.find(row->primary_key);
        SafeQueue<Row *> * q;
        if (it==q_map.end()){
            q = new SafeQueue<Row *>();
            q_map[row->primary_key] = q;
        }else{
            q = it->second;
        }
        q->enqueue(row);
        std::unique_lock<std::mutex> lock(q->mu);
        while (q->q.front()->event_time!=row->event_time){
            q_cv.wait(lock);
        }
        table->insert_row(row);

        auto now = get_now();
        row->now = get_now();
//        std::cout<<row->table_name<<" "<<row->event_time<<" "<<row->now<<std::endl;
        std::cout<<row->table_name<<" "<<row->columns[0]<<" "<<row->columns[1]<<std::endl;
//        printf("%s,%lld,%lld\n",row->table_name.c_str(),now - row->event_time,now - pretime);
//        pretime = row->event_time;
//        table->g 140898802 54 311052349
        q->q.pop();
        q_cv.notify_all();
    }
}