#include <sstream>
#include "multi_group_replayer.h"
#include "string.h"
#include <fstream>

#include <atomic>

namespace rpl{

    const uint32_t buffer_size = 100;
    std::atomic<int64_t> max_xid;
    extern uint64_t get_now();

    MultiGroupReplayer::MultiGroupReplayer(Rpl_info &rplinfo) {
        trx = 0;
        rplInfo = rplinfo;
        n = rplInfo.group_num;
        if (rplInfo.is_remote){
            eventFetcher = new Remote_event_fetcher();
        }else{
            eventFetcher = new Binlog_file_event_fetcher(rplInfo.files);
        }
        parse_pool = new boost::asio::thread_pool(rplinfo.parse_pool);
//        parse_pool = new ThreadPool(rplInfo.parse_pool);
//        bool bind = true;
//        parse_pool->init(bind);

        std::cout << "parse pool(#): " << rplinfo.parse_pool << std::endl;
        for(int i = 0; i < n; i++){
            std::cout << "group " << i << ": workers(#): " << rplInfo.group_pool[i] << std::endl;
        }
        tables = new std::unordered_map<std::string, Table *>();
        buffers = nullptr;
        commiters = std::vector<Commiter *>(n);
//        dispenses_pool = std::vector<ThreadPool *>(n);
        dispenses_pool = std::vector<boost::asio::thread_pool *>(n);
        for (int i = 0; i < n; ++i) {
            commiters[i] = new Commiter(tables);
            dispenses_pool[i] = new boost::asio::thread_pool(rplInfo.group_pool[i]);
//            dispenses_pool[i] = new ThreadPool(rplInfo.group_pool[i]);
//            dispenses_pool[i]->init(bind);
        }
//        pipe_fd = open("/root/mts/QueryBot5000/pipe_test", O_WRONLY);
    }
    uint8_t MultiGroupReplayer::init() {
        buffers =  new event_buffer*[buffer_size];
        eventHandler.init(tables, rplInfo.all_tables);
        delay_init();
        max_xid = 0;
        do{
            auto eb = new event_buffer();
            if (eventFetcher->fetch_a_event(eb->buffer, eb->length)){
                break;
            }
            switch ((binary_log::Log_event_type)eb->buffer[EVENT_TYPE_OFFSET]){
                case binary_log::FORMAT_DESCRIPTION_EVENT:{
                    binary_log::Format_description_event *fde_tmp = new binary_log::Format_description_event(4, "8.017");
                    fde = new binary_log::Format_description_event(reinterpret_cast<const char *>(eb->buffer), fde_tmp);
                    break;
                }
                case binary_log::TABLE_MAP_EVENT:{
                    auto *ev = new binary_log::Table_map_event(reinterpret_cast<const char *>(eb->buffer), fde);
                    eventHandler.unpack(ev);
                    delete ev;
                    break;
                }
                default:
                    break;
            }
        }while (true);
        eventFetcher->init();
        return 0;
    }
    std::atomic<uint64_t> trx_sum[10];
    std::atomic<uint64_t> trx_size[10];
    bool stop = false;
    time_t end_time;
    uint8_t MultiGroupReplayer::run() {
        memset(trx_sum, 0 , sizeof(trx_sum));
        time_t start = get_now();
        for (const auto &commiter : commiters) {
            commiter->commit();
        }
        uint64_t en = 0;
        auto a = std::thread(&MultiGroupReplayer::delay, this);
        do{
            en++;
            auto eb = new event_buffer();
            if (eventFetcher->fetch_a_event(eb->buffer, eb->length)){
                break;
            }
            parallel_process(eb);
        }while (true);
        delete eventFetcher;
//        stop = true;
//        parse_pool->shutdown();
        parse_pool->join();
        std::cout<<"total number of txns: " << trx << std::endl;
        std::cout << "total parse used time: "<< (get_now()-start) << " us" << std::endl;
        for (int i = 0; i < n; ++i) {
            std::cout<<" log events(#) in group "<< i <<" : "<<trx_sum[i]<<std::endl;
        }
        for (int i = 0; i < n; ++i) {
            std::cout<<" log size(#) in group "<< i <<" : "<<trx_size[i] / 1e8<<std::endl;
        }
        for (const auto &commiter : commiters) {
            commiter->shut_down();
        }
        for (const auto &pool : dispenses_pool) {
            pool->join();
//            pool->shutdown();
        }
        for (int i=0;i<n;i++) {
            commiters[i]->thread.join();
            std::cout << commiters[i]->name << " - " << commiters[i]->trx_cnt * 1.0 / (commiters[i]->used_time/1e6) << std::endl;
        }
        end_time = get_now();
        stop = true;
        a.join();
        return 0;
    }

    uint32_t i = 0;
    double delay_time[3000];
    uint8_t MultiGroupReplayer::parallel_process(event_buffer* eb){
        switch ((binary_log::Log_event_type)eb->buffer[EVENT_TYPE_OFFSET]) {
//            case binary_log::ANONYMOUS_GTID_LOG_EVENT:{
//                auto event = new binary_log::Gtid_event(reinterpret_cast<const char *>(eb->buffer),fde);
//                std::cout<<event->last_committed<<" "<<event->sequence_number<<std::endl;
//                std::cout<<std::endl;
//                break;
//            }
            case binary_log::XID_EVENT:{
                ++trx;
//                auto event = new binary_log::Xid_event(reinterpret_cast<const char *>(eb->buffer), fde);
//                time_t start3 = get_now();
                for (auto &commiter : commiters) {
                    commiter->total_trx = trx;
//                    commiter->push_commit_que(trx);
                }
                boost::asio::post(*parse_pool, std::bind(&MultiGroupReplayer::trx_dispenses, this, buffers, trx, i));
//                parse_pool->submit(std::bind(&MultiGroupReplayer::trx_dispenses, this, buffers, trx, i));
                buffers = new event_buffer*[buffer_size];
//                time_post += get_now() - start3;
                i = 0;
                break;
            }
            case binary_log::FORMAT_DESCRIPTION_EVENT:{
                binary_log::Format_description_event *fde_tmp = new binary_log::Format_description_event(4, "8.017");
                fde = new binary_log::Format_description_event(reinterpret_cast<const char *>(eb->buffer), fde_tmp);
                break;
            }
            default:{
                buffers[i++] = eb;
                break;
            }
        }
        return 0;
    }

    uint8_t MultiGroupReplayer::trx_dispenses(event_buffer **buffer_arr,
                                              uint64_t xid, uint32_t k) {
        std::vector<Trx_info *> trxinfos;
        for (int i = 0; i < n; ++i) {
            trxinfos.push_back(commiters[i]->get_new_Trx_info(xid));
        }
        time_t start1 = get_now();
        for (int i = 0; i < k; ++i) {
            event_dispenses(buffer_arr[i], trxinfos);
        }
        for (int j = 0; j < n;  ++j) {
            trxinfos[j]->commiter->trx_times[xid].start_time = start1;
            if (trxinfos[j]->buffers.size() > 0){
                trx_sum[j] += trxinfos[j]->buffers.size();
                for (auto i: trxinfos[j]->buffers){
                    trx_size[j] += i->length;
                }
            }
        }
        time_t dis_time = get_now();
        for (int i = 0; i < n; ++i) {
            trxinfos[i]->commiter->trx_times[xid].dis_time = dis_time;
//            dispenses_pool[i]->submit(std::bind(&MultiGroupReplayer::event_handle, this, trxinfos[i]));
            boost::asio::post(*dispenses_pool[i],std::bind(&MultiGroupReplayer::event_handle, this, trxinfos[i]));
        }
        delete [] buffer_arr;
        return 0;
    }



    uint8_t MultiGroupReplayer::event_dispenses(event_buffer* eb, std::vector<Trx_info *> & trxinfo) {
        uint8_t *buffer = eb->buffer;
        binary_log::Log_event_type type = (binary_log::Log_event_type)buffer[EVENT_TYPE_OFFSET];
        switch (type) {
//            case binary_log::TABLE_MAP_EVENT:{
//                auto *ev = new binary_log::Table_map_event(reinterpret_cast<const char *>(buffer), fde);
////                std::string full_table_name = ev->get_db_name()+'.'+ev->get_table_name();
////                auto it = tables->find(full_table_name);
////                auto *table_schema = eventHandler.unpack(ev);
//                eventHandler.unpack(ev);
//                //如果没有这个表则创建这个表，并构建schema
////                if (it == tables->end()){
////                    Table *table = new Table(full_table_name);
////                    table->schema = new binary_log::TableSchema(*table_schema);
////                    table->_pk = table_schema->getPrikey();
////                    (*tables)[full_table_name] = table;
////                }
//                delete ev;
//                break;
//            }
            case binary_log::WRITE_ROWS_EVENT:
            case binary_log::DELETE_ROWS_EVENT:
            case binary_log::UPDATE_ROWS_EVENT:{
                auto *ev = new binary_log::Rows_event(reinterpret_cast<const char *>(buffer), fde);
                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                while (!table_schema){
                    table_schema = eventHandler.get_schema(ev->get_table_id());
                }
//                std::ostringstream oss;
                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();

//                oss<<ev->header()->when.tv_sec;
//                std::string write_buf = table_full_name + "--" + oss.str()+"\n";
//                write(pipe_fd, write_buf.c_str(), write_buf.size());
                delete ev;
                if (rplInfo.group_map.find(table_full_name) == rplInfo.group_map.end())
                    break;
                trxinfo[rplInfo.group_map[table_full_name]]->buffers.push_back(eb);
                break;
            }
            default:
                break;
        }
        return 0;
    }

    uint8_t MultiGroupReplayer::event_handle(Trx_info *trxinfo) {
//        trxinfo->trxRows = new Trx_rows(trxinfo->xid);
//        trxinfo->commiter->trx_times[trxinfo->xid].start_time = get_now();
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
                    Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000, false, table_full_name, table_schema->getTablename());
                    row->columns = col;
                    row->table = tables->find(row->full_name)->second;
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
                    Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000, false, table_full_name, table_schema->getTablename());
                    row->columns = col;
                    row->table = tables->find(row->full_name)->second;
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
                    Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000, false, table_full_name, table_schema->getTablename());
                    row->columns = col;
                    row->table = tables->find(row->full_name)->second;
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
        trxinfo->commiter->trx_times[trxinfo->xid].parse_time = get_now();
        trxinfo->commit();
        delete trxinfo;
        return 0;
    }

//    uint8_t MultiGroupReplayer::delay_init() {
//        std::string path = "/root/mts/QueryBot5000/tpcc";
//        std::map<uint64_t , std::vector<std::string>> mp;
//        std::vector<std::string> files;
//        struct dirent *ptr;
//        DIR *dir;
//        dir = opendir(path.c_str());
//        uint64_t query_cnt[10];
//        memset(query_cnt,0,sizeof(query_cnt));
//        while (ptr=readdir(dir)){
//            if (ptr->d_name[0] == '.'){
//                continue;
//            }
//            files.push_back(ptr->d_name);
//        }
////        std::cout<<"----"<<std::endl;
//        for (auto &file : files) {
//            std::string table_name = file.substr(0, file.find("."));
//            table_name = "tpcc." + table_name;
////            table_name = table_name.replace(table_name.find("_"), 1, ".");
//            file = path + "/" + file;
//            std::ifstream fin(file.c_str());
//            std::string write_buf;
//            getline(fin, write_buf);
//            while (getline(fin, write_buf)){
//                int pos = write_buf.find(",");
//                std::string time_buf = write_buf.substr(0,pos);
//                uint64_t time;
//                std::istringstream is(write_buf);
//                is >> time;
//                if (mp.find(time) == mp.end()){
//                    mp[time] = std::vector<std::string>();
//                }
//                mp[time].push_back(table_name);
//                query_cnt[rplInfo.group_map[table_name]]++;
//            }
//        }
//        long long last_time = -1;
//        for (const auto &item : mp) {
//            if (last_time == -1){
//                last_time = item.first;
//            }
////            query_list.push_back({(item.first-last_time)*1000, item.second});
//            query_list.push_back({(item.first - last_time) * 1000, item.second});
//            last_time = item.first;
//        }
//        for (int j = 0; j < 3; ++j) {
//            std::cout << j << "  " << query_cnt[j] << std::endl;
//        }
////        for (const auto &item : query_list) {
////            std::cout<<item.first;
////            for (const auto &list : item.second) {
////                std::cout<<" "<<list;
////            }
////            std::cout<<std::endl;
////        }
//        return 0;
//    }
//

    uint8_t MultiGroupReplayer::delay_init() {
        std::string path = "/root/project/mts2/query_log";
        struct dirent *ptr;
        DIR *dir;
        std::ifstream fin(path.c_str());
        std::string write_buf;
        while (getline(fin, write_buf)){
            int pos = write_buf.find(",");
            std::string time_buf = write_buf.substr(0,pos);
            uint64_t time;
            std::istringstream is(write_buf);
            is >> time;

            std::vector<std::string> tables;
            std::string table_buf = write_buf.substr(pos+1);
            while (true){
                pos = table_buf.find(",");
                if (pos == table_buf.npos)
                    break;
                std::string table = table_buf.substr(0, pos);
                table_buf = table_buf.substr(pos+1);
                tables.push_back(table);
            }
            query_list.push_back({time, tables});
        }
        return 0;
    }


//    uint8_t MultiGroupReplayer::delay() {
//        uint64_t total_time=0;
//        uint64_t query_cnt = 0, index, tmp_time;
//        float freq = 14.4;
////        std::cout << "query list length: " << query_list.size() << std::endl;
//        for (const auto &item : query_list) {
//            for (const auto &table_name : item.second) {
//                index = rplInfo.group_map[table_name];
//                while ((commiters[index]->trx_cnt/freq)<=query_cnt){
//                    usleep(1);
//                }
//            }
//            query_cnt++;
//            tmp_time = get_now() - commiters[index]->start;
//            if (tmp_time > item.first)
//                tmp_time -= item.first;
//            else
//                tmp_time = 0;
//            total_time += tmp_time;
//        }
//        std::cout << "delay time: " << total_time / query_list.size() << std::endl;
//        return 0;
//    }

    uint8_t MultiGroupReplayer::delay() {
        uint64_t total_time=0, total_query = 0, cnt = 0, cnt2 = 0, last_cnt[rplInfo.group_num];
        memset(last_cnt, 0, sizeof(uint64_t)*rplInfo.group_num);
        uint64_t stop_num = 5000;
        uint64_t interval = rplInfo.interval;
        auto &freq = rplInfo.query_fre;
        std::vector<int> query_list;
        time_t time1, time2;
        int fq = freq[0].ferq;
        int freq_ = 0;
        std::ofstream file;
        file.open("/root/project/mts_cp/log");
        for (const auto &f : freq) {
            freq_ += f.ferq;
            fq = std::__gcd(fq,f.ferq);
        }
//        uint64_t interval = rplInfo.interval / freq_;
        for (int i = 0; i < freq.size(); i++) {
            int f = freq[i].ferq / fq;
            for (int j = 0; j < f; ++j) {
                query_list.push_back(i);
            }
        }
        memset(delay_time, 0, sizeof(delay_time));
        time_t start = get_now();
//        time_t st = get_now();
        while (true){
            for (const auto &query : query_list) {
                time_t s = get_now();
                for (auto group : freq[query].query_group){
                    uint64_t t = commiters[group]->trx_cnt;
                    while (commiters[group]->trx_cnt - t <= 50000){
//                    while ((commiters[group]->trx_cnt / interval) <= total_query){
//                    while (commiters[group]->trx_cnt - last_cnt[group] <= interval){
                        if (stop || total_query >= stop_num){
                            std::cout << "delay time: " << total_time * 1.0 / 1e6 / total_query << "s total query: " << total_query << std::endl;
                            std::cout << cnt << "----" << cnt2 << std::endl;
                            for (int i = 1; i <= total_query; i++)
                                file << delay_time[i] / 1e6 << std::endl;
                            return 0;
//                            exit(0);
                        }
                        std::this_thread::sleep_for(std::chrono::nanoseconds(1));
//                        if (get_now() - start - (total_query * 1e6 / freq_) > 450000)
//                            break;
                    }
                }
                total_query++;
                delay_time[total_query] = get_now() - s + 3000;
                total_time += delay_time[total_query];
//                time1 = get_now() - start;
//                time2 = (total_query * 1e6 / freq_);
//                time_t time3 = ((total_query+15) * 1e6 / freq_);
//                if (time1 >= time2 && time1 <= time3)
//                    cnt++;
//                if (time1 >= time2)
//                    delay_time[total_query] = time1 - time2;
//                else
//                {
//                    while (time2 >= time1){
//                        time1 = get_now() - start + 1e4;
//                        std::this_thread::sleep_for(std::chrono::nanoseconds(1));
//                    }
//                    cnt2++;
////                    cnt++;
//                }
//                delay_time[total_query] += 3000;
//                total_time += delay_time[total_query];
                if (stop || total_query >= stop_num){
                    std::cout << "delay time: " << total_time * 1.0 / 1e6 / total_query << "s total query: " << total_query << std::endl;
                    std::cout << cnt << "----" << cnt2 << std::endl;
                    for (int i = 1; i <= total_query; i++)
                        file << delay_time[i] / 1e6 << std::endl;
                    return 0;
//                            exit(0);
                }
////                for (int j = 0; j < rplInfo.group_num; ++j) {
////                    last_cnt[j] = commiters[j]->trx_cnt;
////                }
////                std::this_thread::sleep_for(std::chrono::microseconds (1000000));
//                if (stop || total_query >= stop_num){
//                    std::cout << "delay time: " << total_time * 1.0 / 1e6 / total_query << "s total query: " << total_query << std::endl;
//                    std::cout << cnt << "----" << cnt2 << std::endl;
//                    for (int i = 1; i <= total_query; i++)
//                        file << delay_time[i] / 1e6 << std::endl;
//                    exit(0);
//                }
            } // for (const auto &query : query_list)
        } // where true
    }

    uint8_t MultiGroupReplayer::get() {
        std::ifstream fin("/root/project/mts2/aa.csv");
//        auto table = tables[]
        std::string write_buf;
        while (getline(fin, write_buf)){
            int pos = write_buf.find(",");
            std::string query = write_buf.substr(pos+1);
            std::string time_buf = write_buf.substr(0,pos);
            time_t time = StringToDatetime(time_buf);
            auto query_tables = sql_parser(query);
            std::string table_string = "[";
            for (const auto &table_name : query_tables) {
                auto commiter = commiters[rplInfo.group_map[table_name]];
                while (commiter->commit_time >= time);
                table_string += "'" + table_name + "'" + ",";
            }
            table_string += "]";
            std::cout<<time<<" "<<table_string<<std::endl;
            table_string += "--" + std::to_string(time);
            write(pipe_fd, table_string.c_str(), table_string.size());
        }
    }
    std::vector<std::string> MultiGroupReplayer::sql_parser(std::string query) {
//        std::string query = "SELECT DISTINCT agency_timezone FROM m.agency, aa WHERE agency_id = 1";
        std::vector<std::string> tables;
        hsql::SQLParserResult result;
        hsql::SQLParser::parse(query, &result);
        if (result.isValid()) {
            for (auto i = 0u; i < result.size(); ++i) {
                const hsql::SQLStatement* statement = result.getStatement(i);
                if (statement->isType(hsql::kStmtSelect)) {
                    const auto* select = static_cast<const hsql::SelectStatement*>(statement);
                    if (select->fromTable->list) {
                        for (const auto &item : *select->fromTable->list) {
//                            std::cout<<item->name<<std::endl;
                            tables.push_back(std::string(item->schema)+"."+item->name);
                        }
                    }
                    else {
                        tables.push_back(std::string(select->fromTable->schema)+"."+select->fromTable->name);
//                        std::cout<<(select->fromTable->name)<<std::endl;
                    }
                }
            }
        }

        return tables;
    }
    time_t MultiGroupReplayer::StringToDatetime(std::string str) {
//        char *cha = (char*)str.data();             // 将string转换成char*。
        tm tm_;                                    // 定义tm结构体。
        int year, month, day, hour, minute, second;// 定义时间的各个int临时变量。
        sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &minute, &second);// 将string存储的日期时间，转换为int临时变量。
        tm_.tm_year = year - 1900;                 // 年，由于tm结构体存储的是从1900年开始的时间，所以tm_year为int临时变量减去1900。
        tm_.tm_mon = month - 1;                    // 月，由于tm结构体的月份存储范围为0-11，所以tm_mon为int临时变量减去1。
        tm_.tm_mday = day;                         // 日。
        tm_.tm_hour = hour;                        // 时。
        tm_.tm_min = minute;                       // 分。
        tm_.tm_sec = second;                       // 秒。
        tm_.tm_isdst = 0;                          // 非夏令时。
        time_t t_ = mktime(&tm_);                  // 将tm结构体转换成time_t格式。
        return t_;                                 // 返回值。
    }
}

