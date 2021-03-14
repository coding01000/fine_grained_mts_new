#include "multi_group_replayer.h"

namespace rpl{

    extern uint64_t get_now();
//    std::string table_name="tpcc.ORDERLINE";
    std::string table_name="tpcc.WAREHOUSE";
    MultiGroupReplayer::MultiGroupReplayer(Rpl_info &rplInfo) {
        trx = 0;
        n = rplInfo.group_num;
        if (rplInfo.is_remote){
            eventFetcher = new Remote_event_fetcher();
        }else{
            eventFetcher = new Binlog_file_event_fetcher(rplInfo.files);
        }
        parse_pool = new boost::asio::thread_pool(rplInfo.parse_pool);
        tables = new std::unordered_map<std::string, Table *>();
        buffers = nullptr;
        commiters = std::vector<Commiter *>(n);
        dispenses_pool = std::vector<boost::asio::thread_pool *>(n);
        for (int i = 0; i < n; ++i) {
            commiters[i] = new Commiter(tables);
            dispenses_pool[i] = new boost::asio::thread_pool(rplInfo.group_pool[i]);
        }
    }

    uint8_t MultiGroupReplayer::init() {
        buffers = std::make_shared<std::vector<std::shared_ptr<event_buffer>>>();
        for (const auto &commiter : commiters) {
            commiter->commit();
        }
        return 0;
    }

    uint8_t MultiGroupReplayer::run() {
        time_t start = get_now();
        do{
            std::shared_ptr<event_buffer> eb = std::make_shared<event_buffer>();
            if (eventFetcher->fetch_a_event(eb->buffer, eb->length)){
                printf("fetch event eof");
                break;
            }
            parallel_process(eb);
        }while (true);
        std::cout<<trx<<" "<<(get_now()-start)<<std::endl;
        parse_pool->join();
        for (const auto &commiter : commiters) {
            commiter->shut_down();
        }
        for (const auto &pool : dispenses_pool) {
            pool->join();
        }
        for (const auto &commiter : commiters) {
            commiter->thread.join();
        }
        return 0;
    }

    uint8_t MultiGroupReplayer::parallel_process(std::shared_ptr<event_buffer> eb){
        switch ((binary_log::Log_event_type)eb->buffer[EVENT_TYPE_OFFSET]) {
            case binary_log::XID_EVENT:{
                ++trx;
                for (auto &commiter : commiters) {
                    commiter->push_commit_que(trx);
                }
                boost::asio::post(*parse_pool, std::bind(&MultiGroupReplayer::trx_dispenses, this, buffers, trx));
                buffers = std::make_shared<std::vector<std::shared_ptr<event_buffer>>>();
                break;
            }
            case binary_log::FORMAT_DESCRIPTION_EVENT:{
                binary_log::Format_description_event *fde_tmp = new binary_log::Format_description_event(4, "8.017");
                fde = new binary_log::Format_description_event(reinterpret_cast<const char *>(eb->buffer), fde_tmp);
                break;
            }
            default:{
                buffers->push_back(eb);
                break;
            }
        }
        return 0;
    }


    uint8_t MultiGroupReplayer::trx_dispenses(std::shared_ptr<std::vector<std::shared_ptr<event_buffer>>> buffers,
                                              uint64_t xid) {
        std::vector<Trx_info *> trxinfos;
        for (int i = 0; i < n; ++i) {
            trxinfos.push_back(commiters[i]->get_new_Trx_info(xid));
        }
        for (auto &buffer : *buffers) {
            event_dispenses(buffer, trxinfos);
        }
        for (int i = 0; i < n; ++i) {
            boost::asio::post(*dispenses_pool[i], std::bind(&MultiGroupReplayer::event_handle, this, trxinfos[i]));
        }
        return 0;
    }

    uint8_t MultiGroupReplayer::event_dispenses(std::shared_ptr<event_buffer> eb, std::vector<Trx_info *> trxinfo) {
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
            case binary_log::WRITE_ROWS_EVENT:
            case binary_log::DELETE_ROWS_EVENT:
            case binary_log::UPDATE_ROWS_EVENT:{
                auto *ev = new binary_log::Rows_event(reinterpret_cast<const char *>(buffer), fde);
                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                if (table_full_name==table_name){
                    trxinfo[0]->buffers.push_back(eb);
                }else{
                    trxinfo[1]->buffers.push_back(eb);
                }
                break;
            }
            default:
                break;
        }
        return 0;
    }

    uint8_t MultiGroupReplayer::event_handle(Trx_info *trxinfo) {
//        trxinfo->trxRows = new Trx_rows(trxinfo->xid);
        for (const auto &eb : trxinfo->buffers) {
            uint8_t *buffer = eb->buffer;
            binary_log::Log_event_type type = (binary_log::Log_event_type)buffer[EVENT_TYPE_OFFSET];
            switch (type) {
                case binary_log::WRITE_ROWS_EVENT: {
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
                    trxinfo->trxRows->rows.push_back(row);
                    break;
                }
                case binary_log::DELETE_ROWS_EVENT:{
                    auto *ev = new binary_log::Delete_rows_event(reinterpret_cast<const char *>(buffer), fde);
                    char *buf = new char[ev->row.size()];
                    std::copy(ev->row.begin(), ev->row.end(), buf);
                    auto reader = binary_log::Event_reader(buf, ev->row.size());
                    auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                    std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                    int idx = table_schema->getIdxByName(table_schema->getPrikey());
                    auto col = eventHandler.unpack(ev, reader, table_schema);
                    Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_full_name, table_schema->getTablename());
                    row->columns = col;
                    trxinfo->trxRows->rows.push_back(row);

                    break;
                }
                case binary_log::UPDATE_ROWS_EVENT:{
                    auto *ev = new binary_log::Update_rows_event(reinterpret_cast<const char *>(buffer), fde);
                    char *buf = new char[ev->row.size()];
                    std::copy(ev->row.begin(), ev->row.end(), buf);
                    auto reader = binary_log::Event_reader(buf, ev->row.size());
                    auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                    std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                    int idx = table_schema->getIdxByName(table_schema->getPrikey());
                    auto col = eventHandler.unpack(ev, reader, table_schema);
                    Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_full_name, table_schema->getTablename());
                    row->columns = col;
                    trxinfo->trxRows->rows.push_back(row);
                    break;
                }
                default:
                    break;
            }
        }
        trxinfo->commit();
        return 0;
    }
}