//
// Created by ZFY on 2021/4/27.
//
#include "../include/worker.h"
#include "coordinator.h"
namespace mysql_mts{

    Worker::Worker(Coordinator *c, uint32_t i){
        coordinator = c;
//        eventHandler = c->eventHandler;
//        fde = c->fde;
//        tables = c->tables;
        id = i;
        in_use = false;
        stop = false;
        thread = std::thread(std::bind(&Worker::run,this));
    }

    void Worker::run() {
        std::unique_lock<std::mutex> lock(mu);
        auto &eventHandler = coordinator->eventHandler;
        auto &fde = coordinator->fde;
        auto &tables = coordinator->tables;
        while (!stop||jobs.size_approx() > 0){
//        如果为空 worker进行等待
            while (jobs.size_approx()==0&&!stop){
                usleep(1);
//                cv.wait(lock);
            }
//            if (jobs.empty()){
            if (jobs.size_approx() == 0){
                break;
            }
//            获取job队列中的buffer
//            auto eb = jobs.front();
            rpl::event_buffer *eb;
            jobs.try_dequeue(eb);
            uint8_t * buffer = eb->buffer;
//            jobs.pop();
//            进行解析
            switch ((binary_log::Log_event_type)buffer[EVENT_TYPE_OFFSET]){
                case binary_log::TABLE_MAP_EVENT:{
                    auto *ev = new binary_log::Table_map_event(reinterpret_cast<const char *>(buffer), coordinator->fde);
                    eventHandler->unpack(ev);
                    delete ev;
                    break;
                }
                case binary_log::WRITE_ROWS_EVENT: {
                    auto *ev = new binary_log::Write_rows_event(reinterpret_cast<const char *>(buffer), fde);
                    auto *table_schema = eventHandler->get_schema(ev->get_table_id());
                    std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                    char *buf = new char[ev->row.size()];
                    std::copy(ev->row.begin(), ev->row.end(), buf);
                    auto reader = binary_log::Event_reader(buf, ev->row.size());
                    int idx = table_schema->getIdxByName(table_schema->getPrikey());
                    auto col = eventHandler->unpack(ev, reader, table_schema);
                    rpl::Row *row = new rpl::Row(col[idx], ev->header()->when.tv_sec, false, table_full_name, table_schema->getTablename());
                    row->columns = col;
                    row->table = tables->find(row->full_name)->second;
                    row->table->insert_row(row);
//                    trxinfo->trxRows->rows.push_back(row);
                    delete ev;
                    delete[] buf;
                    break;
                }
                case binary_log::DELETE_ROWS_EVENT:{
                    auto *ev = new binary_log::Delete_rows_event(reinterpret_cast<const char *>(buffer), fde);
                    char *buf = new char[ev->row.size()];
                    std::copy(ev->row.begin(), ev->row.end(), buf);
                    auto reader = binary_log::Event_reader(buf, ev->row.size());
                    auto *table_schema = eventHandler->get_schema(ev->get_table_id());
                    std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                    int idx = table_schema->getIdxByName(table_schema->getPrikey());
                    auto col = eventHandler->unpack(ev, reader, table_schema);
                    rpl::Row *row = new rpl::Row(col[idx], ev->header()->when.tv_sec, false, table_full_name, table_schema->getTablename());
                    row->columns = col;
                    row->table = tables->find(row->full_name)->second;
                    row->table->insert_row(row);
//                    trxinfo->trxRows->rows.push_back(row);
                    delete ev;
                    delete[] buf;
                    break;
                }
                case binary_log::UPDATE_ROWS_EVENT:{
                    auto *ev = new binary_log::Update_rows_event(reinterpret_cast<const char *>(buffer), fde);
                    char *buf = new char[ev->row.size()];
                    std::copy(ev->row.begin(), ev->row.end(), buf);
                    auto reader = binary_log::Event_reader(buf, ev->row.size());
                    auto *table_schema = eventHandler->get_schema(ev->get_table_id());
                    std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                    int idx = table_schema->getIdxByName(table_schema->getPrikey());
                    auto col = eventHandler->unpack(ev, reader, table_schema);
                    rpl::Row *row = new rpl::Row(col[idx], ev->header()->when.tv_sec, false, table_full_name, table_schema->getTablename());
                    row->columns = col;
                    row->table = tables->find(row->full_name)->second;
                    row->table->insert_row(row);
//                    trxinfo->trxRows->rows.push_back(row);
                    delete ev;
                    delete[] buf;
                    break;
                }
                case binary_log::ROTATE_EVENT:
                case binary_log::XID_EVENT:{
//                  如果job队列是空的了，就将work的id加入到空闲队列
//                    while (!jobs.empty())
                    while (jobs.size_approx() > 0)
                        jobs.try_dequeue(eb);
//                    if (jobs.empty()){
//                    if (jobs.size_approx() == 0){
                    coordinator->add_free_work(id);
                    in_use = false;
                    coordinator->cv.notify_all();
//                    }
                    break;
                }
                default:
                    break;
            }//switch
        }//while (!stop||jobs.empty())
    }//run

    uint8_t Worker::add_job(rpl::event_buffer *eb) {
        jobs.enqueue(eb);
//        jobs.push(eb);
        return 0;
    }

    uint8_t Worker::set_trx(uint64_t lc, uint64_t sn) {
        last_committed = lc;
        sequence_number = sn;
    }
}
