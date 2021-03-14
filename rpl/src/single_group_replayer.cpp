#include "single_group_replayer.h"

namespace rpl{

    SingleGroupReplayer::SingleGroupReplayer(Rpl_info &rplInfo) {
        trx= 0;
        if (rplInfo.is_remote){
            eventFetcher = new Remote_event_fetcher();
        }else{
            eventFetcher = new Binlog_file_event_fetcher(rplInfo.files);
        }
        parse_pool = new boost::asio::thread_pool(rplInfo.parse_pool);
        tables = new std::unordered_map<std::string, Table *>();
        commiter = new Commiter(tables);
        buffers = nullptr;
    }

    uint8_t SingleGroupReplayer::init() {
        commiter->commit();
        buffers = std::make_shared<std::vector<std::shared_ptr<event_buffer>>>();
        return 0;
    }

    uint8_t SingleGroupReplayer::run() {
        do{
            std::shared_ptr<event_buffer> eb(new event_buffer());
            if (eventFetcher->fetch_a_event(eb->buffer, eb->length)){
                printf("fetch event eof");
                break;
            }
            parallel_process(eb);
        }while (true);
        std::cout<<trx<<std::endl;
        parse_pool->join();
        commiter->shut_down();
        commiter->thread.join();
        return 0;
    }

    uint8_t SingleGroupReplayer::parallel_process(std::shared_ptr<event_buffer> eb){
        switch ((binary_log::Log_event_type)eb->buffer[EVENT_TYPE_OFFSET]) {
            case binary_log::FORMAT_DESCRIPTION_EVENT:{
                binary_log::Format_description_event *fde_tmp = new binary_log::Format_description_event(4, "8.017");
                fde = new binary_log::Format_description_event(reinterpret_cast<const char *>(eb->buffer), fde_tmp);
                break;
            }
            case binary_log::XID_EVENT:{
                auto xid_event = new binary_log::Xid_event(reinterpret_cast<const char *>(eb->buffer), fde);
                uint64_t xid = xid_event->xid;
                commiter->push_commit_que(xid);
                ++trx;
//                trx_handle(buffers, xid);
//                parse_pool->submit(std::bind(&SingleGroupReplayer::trx_handle, this, buffers, xid));
                boost::asio::post(*parse_pool, std::bind(&SingleGroupReplayer::trx_handle, this, buffers, xid));
                buffers = std::make_shared<std::vector<std::shared_ptr<event_buffer>>>();
                break;
            }
            default:{
                buffers->push_back(eb);
                break;
            }
        }
        return 0;
    }

    uint8_t SingleGroupReplayer::trx_handle(std::shared_ptr<std::vector<std::shared_ptr<event_buffer>>> buffers,
                                        uint64_t xid) {
        Trx_info *trxInfo = commiter->get_new_Trx_info(xid);
        for (auto &buffer : *buffers) {
            event_handle(buffer, trxInfo);
        }
        trxInfo->commit();
        return 0;
    }

    uint8_t SingleGroupReplayer::event_handle(std::shared_ptr<event_buffer> eb, Trx_info *trxInfo) {
        uint8_t *buffer = eb->buffer;
        binary_log::Log_event_type type = (binary_log::Log_event_type)buffer[EVENT_TYPE_OFFSET];
        switch (type) {
            case binary_log::TABLE_MAP_EVENT:{
                auto *ev = new binary_log::Table_map_event(reinterpret_cast<const char *>(buffer), fde);
                std::string full_table_name = ev->get_db_name()+'.'+ev->get_table_name();
                auto it = tables->find(full_table_name);
                auto *table_schema = eventHandler.unpack(ev);
                //如果没有这个表则创建这个表，并构建schema
                if (it == tables->end()){
                    Table *table = new Table(full_table_name);
                    table->schema = new binary_log::TableSchema(*table_schema);
                    table->_pk = table_schema->getPrikey();
                    (*tables)[full_table_name] = table;
                }
                break;
            }
            case binary_log::WRITE_ROWS_EVENT:{
                auto *ev = new binary_log::Write_rows_event(reinterpret_cast<const char *>(buffer), fde);
                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                char *buf = new char[ev->row.size()];
                std::copy(ev->row.begin(), ev->row.end(), buf);
                auto reader = binary_log::Event_reader(buf, ev->row.size());
                int idx = table_schema->getIdxByName(table_schema->getPrikey());
                auto col = eventHandler.unpack(ev, reader, table_schema);
                Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_full_name, table_schema->getTablename());
                row->columns = col;
                trxInfo->trxRows->rows.push_back(row);
                break;
            }
            case binary_log::DELETE_ROWS_EVENT:{
                auto *ev = new binary_log::Delete_rows_event(reinterpret_cast<const char *>(buffer), fde);
                char *buf = new char[ev->row.size()];
                std::copy(ev->row.begin(), ev->row.end(), buf);
                auto reader = binary_log::Event_reader(buf, ev->row.size());
                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                auto it = tables->find(table_full_name);
                int idx = table_schema->getIdxByName(table_schema->getPrikey());
                auto col = eventHandler.unpack(ev, reader, table_schema);
                Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_full_name, table_schema->getTablename());
                row->columns = col;
                trxInfo->trxRows->rows.push_back(row);
                break;
            }
            case binary_log::UPDATE_ROWS_EVENT:{
                auto *ev = new binary_log::Update_rows_event(reinterpret_cast<const char *>(buffer), fde);
                char *buf = new char[ev->row.size()];
                std::copy(ev->row.begin(), ev->row.end(), buf);
                auto reader = binary_log::Event_reader(buf, ev->row.size());
                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                auto it = tables->find(table_full_name);
                int idx = table_schema->getIdxByName(table_schema->getPrikey());
                auto col = eventHandler.unpack(ev, reader, table_schema);
                Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_full_name, table_schema->getTablename());
                row->columns = col;
                trxInfo->trxRows->rows.push_back(row);
                break;
            }
            default:
                break;
        }
        return 0;
    }

}