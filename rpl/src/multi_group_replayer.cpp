#include "multi_group_replayer.h"
#include "math.h"
#include "boost/asio/thread_pool.hpp"

namespace rpl{

    extern uint64_t get_now();
//    std::string table_name="tpcc.ORDERLINE";
    std::string table_name="tpcc.WAREHOUSE";
    MultiGroupReplayer::MultiGroupReplayer(Rpl_info &rplinfo) {
        trx = 0;
        rplInfo = rplinfo;
        n = rplInfo.group_num;
        if (rplInfo.is_remote){
            eventFetcher = new Remote_event_fetcher();
        }else{
            eventFetcher = new Binlog_file_event_fetcher(rplInfo.files);
        }
//        parse_pool = new boost::asio::thread_pool(rplInfo.parse_pool);
        parse_pool = new ThreadPool(rplInfo.parse_pool);
        parse_pool->init(false);
//        boost::this_thread::bind_to_processor( unsigned int n);
        std::cout<<"p1: "<<rplInfo.group_pool[0] << " p2: " <<rplInfo.group_pool[1]<<std::endl;
        tables = new std::unordered_map<std::string, Table *>();
        buffers = nullptr;
        commiters = std::vector<Commiter *>(n);
//        dispenses_pool = std::vector<s>
        dispenses_pool = std::vector<ThreadPool *>(n);
        for (int i = 0; i < n; ++i) {
            commiters[i] = new Commiter(tables);
//            dispenses_pool[i] = new boost::asio::thread_pool(rplInfo.group_pool[i]);
            dispenses_pool[i] = new ThreadPool(rplInfo.group_pool[i]);
            dispenses_pool[i]->init(false);
        }
    }
//    ThreadPool pool1(2);
//    ThreadPool pool2(8);
    uint8_t MultiGroupReplayer::init() {
        buffers =  new event_buffer*[100];
        eventHandler.init(tables);
//        pool1.init(false);
//        pool2.init(false);
        return 0;
    }
    time_t time_fetch = 0;
    time_t time_process = 0;
    time_t time_post = 0;
    time_t time_push_back = 0;
    time_t time_format = 0;
//    time_t time_push_commit = 0;
    uint64_t trx1=0, trx2=0;
    uint8_t MultiGroupReplayer::run() {
//        std::cout<<"start! "<<((Binlog_file_event_fetcher*)eventFetcher)->ringBuffer->size()<<std::endl;
        time_t start = get_now();
//        std::shared_ptr<event_buffer> eb = std::make_shared<event_buffer>();
//        cpu_set_t mask;
//        CPU_ZERO(&mask);
//        CPU_SET(1, &mask);//将cpu1绑定
//        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &mask);
        for (const auto &commiter : commiters) {
            commiter->commit();
        }
        do{
//            time_t start1 = get_now();
            auto eb = new event_buffer();
            if (eventFetcher->fetch_a_event(eb->buffer, eb->length)){
                printf("fetch event eof\n");
//                time_fetch += (get_now() - start1);
                break;
            }
//            time_fetch += (get_now() - start1);
            parallel_process(eb);
        }while (true);
        delete eventFetcher;
        std::cout<<"total txns: " << trx<<" "<<(get_now()-start) << std::endl;
//        parse_pool->join();
        parse_pool->shutdown();
        std::cout<<"total txns: " << trx <<" "<<(get_now()-start) << std::endl;
        std::cout << "fetch: " << time_fetch << " time_process: " << time_process << " time_post: " << time_post << " time_push_back: " << time_push_back << std::endl;
//        std::cout<<"time_push_commit: "<<time_push_commit<<std::endl;
        for (const auto &commiter : commiters) {
            std::cout<<commiter->commit_que.size()<<" "<<commiter->trx_map._map.size()<<std::endl;
            commiter->shut_down();
        }
        for (const auto &pool : dispenses_pool) {
//            pool->join();
            pool->shutdown();
        }
//        pool1.shutdown();
//        pool2.shutdown();
        for (const auto &commiter : commiters) {
            commiter->thread.join();
        }
        std::cout<<"commiter1: "<<trx1<<" commiter2: "<<trx2<<std::endl;

        return 0;
    }

    uint32_t i = 0;
    uint8_t MultiGroupReplayer::parallel_process(event_buffer* eb){
//        time_t start = get_now();
        switch ((binary_log::Log_event_type)eb->buffer[EVENT_TYPE_OFFSET]) {
            case binary_log::XID_EVENT:{
                ++trx;
//                time_t start3 = get_now();
                for (auto &commiter : commiters) {
                    commiter->push_commit_que(trx);
                }
//                boost::asio::post(*parse_pool, std::bind(&MultiGroupReplayer::trx_dispenses, this, buffers, trx));
//                parse_pool->submit(std::bind(&MultiGroupReplayer::trx_dispenses, this, buffers, trx));
                parse_pool->submit(std::bind(&MultiGroupReplayer::trx_dispenses, this, buffers, trx, i));
                buffers = new event_buffer*[100];
//                time_post += get_now() - start3;

//                buffers = new std::vector<event_buffer *>(50, nullptr);
                i = 0;
                break;
            }
            case binary_log::FORMAT_DESCRIPTION_EVENT:{
//                time_t start6 = get_now();
                binary_log::Format_description_event *fde_tmp = new binary_log::Format_description_event(4, "8.017");
                fde = new binary_log::Format_description_event(reinterpret_cast<const char *>(eb->buffer), fde_tmp);
//                time_format += get_now() - start6;
                break;
            }
            default:{
//                time_t start4 = get_now();
//                buffers->insert(buffers->begin()+i++, eb);
//                buffers->push_back(eb);
//                buffers->emplace_back(eb);
                buffers[i++] = eb;
//                time_push_back += get_now() - start4;
                break;
            }
        }
//        time_process += (get_now() - start);
        return 0;
    }


    uint8_t MultiGroupReplayer::trx_dispenses(event_buffer **buffer_arr,
                                              uint64_t xid, uint32_t k) {
//        sleep(1);
        std::vector<Trx_info *> trxinfos;
        for (int i = 0; i < n; ++i) {
            trxinfos.push_back(commiters[i]->get_new_Trx_info(xid));
        }
//        for (auto &buffer : *buffers) {
        for (int i = 0; i < k; ++i) {
            event_dispenses(buffer_arr[i], trxinfos);
        }

//        if (trxinfos[0]->buffers.size())
//            trx1++;
//        if (trxinfos[1]->buffers.size())
//            trx2++;

//        std::cout <<"txn_id: " << xid << " event_size: " << trxinfos[0]->buffers.size() << " " << trxinfos[1]->buffers.size() << std::endl;

        for (int i = 0; i < n; ++i) {
//            boost::asio::post(*dispenses_pool[i], std::bind(&MultiGroupReplayer::event_handle, this, trxinfos[i]));
            dispenses_pool[i]->submit(std::bind(&MultiGroupReplayer::event_handle, this, trxinfos[i]));
        }
        delete buffer_arr;
        return 0;
    }

    uint8_t MultiGroupReplayer::event_dispenses(event_buffer* eb, std::vector<Trx_info *> trxinfo) {
        uint8_t *buffer = eb->buffer;
        binary_log::Log_event_type type = (binary_log::Log_event_type)buffer[EVENT_TYPE_OFFSET];
        switch (type) {
            case binary_log::TABLE_MAP_EVENT:{
                auto *ev = new binary_log::Table_map_event(reinterpret_cast<const char *>(buffer), fde);
//                std::string full_table_name = ev->get_db_name()+'.'+ev->get_table_name();
//                auto it = tables->find(full_table_name);
//                auto *table_schema = eventHandler.unpack(ev);
                eventHandler.unpack(ev);
                //如果没有这个表则创建这个表，并构建schema
//                if (it == tables->end()){
//                    Table *table = new Table(full_table_name);
//                    table->schema = new binary_log::TableSchema(*table_schema);
//                    table->_pk = table_schema->getPrikey();
//                    (*tables)[full_table_name] = table;
//                }
                delete ev;
                break;
            }
            case binary_log::WRITE_ROWS_EVENT:
            case binary_log::DELETE_ROWS_EVENT:
            case binary_log::UPDATE_ROWS_EVENT:{
                auto *ev = new binary_log::Rows_event(reinterpret_cast<const char *>(buffer), fde);
                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                trxinfo[rplInfo.group_map[table_full_name]]->buffers.push_back(eb);
//                if (table_full_name==table_name){
//                    trxinfo[0]->buffers.push_back(eb);
//                }else{
//                    trxinfo[1]->buffers.push_back(eb);
//                }
                delete ev;
                break;
            }
            default:
                break;
        }
        return 0;
    }

    uint8_t MultiGroupReplayer::event_handle(Trx_info *trxinfo) {
//        trxinfo->trxRows = new Trx_rows(trxinfo->xid);
        for (auto &eb : trxinfo->buffers) {
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
                    delete ev;
                    delete[] buf;
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
                    delete ev;
                    delete[] buf;
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
                    delete ev;
                    delete[] buf;
                    break;
                }
                default:
                    break;
            }
//            delete[] eb->buffer;
        }
        trxinfo->commit();
        delete trxinfo;
        return 0;
    }
}

