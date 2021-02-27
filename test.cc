#include "binary_log.h"
#include "event_handle.h"
#include <iostream>
#include <string.h>
#include <string>
#include "mts.h"

//#include <mysql/m>
//#include "mysql/mysql_com.h"
//MYSQL *mysql;
//MYSQL_RPL rpl;
//binary_log::Format_description_event *fde1;
//binary_log::TableSchema *table;
//
//void f(){
//    binary_log::Log_event_type type = (binary_log::Log_event_type)rpl.buffer[1 + EVENT_TYPE_OFFSET];
//    if (type == binary_log::WRITE_ROWS_EVENT){
//        printf("WRITE_ROWS_EVENTS!\n");
//        auto *ev = new binary_log::Write_rows_event(reinterpret_cast<const char *>(rpl.buffer + 1), fde1);
//        ev->reader().read<uint8_t>();
//        printf("-----%d---%d\n",(ev->get_event_type()==binary_log::WRITE_ROWS_EVENT),
//               ev->reader().read<uint64_t>());
//    }else if (type == binary_log::FORMAT_DESCRIPTION_EVENT){
//        printf("FORMAT_DESCRIPTION_EVENT!\n");
//        binary_log::Format_description_event fde = binary_log::Format_description_event(4, "8.017");
//        char * buf = ( char *)malloc(sizeof(char *)*(rpl.size+1));
//        memcpy(buf, rpl.buffer + 1, rpl.size-1);
//        fde1 = new binary_log::Format_description_event(reinterpret_cast<const char *>(rpl.buffer + 1), &fde);
//        fde1->print_event_info(std::cout);
//        printf("-----%d---%d\n",(fde1->get_event_type()==binary_log::FORMAT_DESCRIPTION_EVENT),
//               fde1->header()->when);
//    }
//    else if (type == binary_log::TABLE_MAP_EVENT){
//        printf("TABLE_MAP_EVENT!\n");
//        auto *ev = new binary_log::Table_map_event(reinterpret_cast<const char *>(rpl.buffer + 1), fde1);
//        ev->reader().read<uint8_t>();
//        printf("-----%d---%d\n",(ev->get_event_type()==binary_log::TABLE_MAP_EVENT),
//               ev->reader().read<uint64_t>());
//        binary_log::Event_Handler eh;
//        table = eh.unpack(ev);
//    }
//    else if (type == binary_log::DELETE_ROWS_EVENT){
//        printf("DELETE_ROWS_EVENT!\n");
//
//        auto *ev = new binary_log::Delete_rows_event(reinterpret_cast<const char *>(rpl.buffer + 1), fde1);
//        ev->reader().read<uint8_t>();
//        printf("-----%d---%d\n",(ev->get_event_type()==binary_log::DELETE_ROWS_EVENT),
//               ev->reader().read<uint64_t>());
//    }
//    else if (type == binary_log::UPDATE_ROWS_EVENT){
//        printf("UPDATE_ROWS_EVENT!\n");
//
//        auto *ev = new binary_log::Update_rows_event(reinterpret_cast<const char *>(rpl.buffer + 1), fde1);
//        char *buf = new char[ev->row.size()];
//        std::copy(ev->row.begin(), ev->row.end(), buf);
//        auto reader = binary_log::Event_reader(buf, ev->row.size());
//        binary_log::Event_Handler eh;
//        auto it = eh.unpack(ev, reader, table);
//        int a;
//    }
//
//}

int main()
{
    rpl::MTS_Handler mtsHandler;
    mtsHandler.init();
    mtsHandler.run();

//    rpl.start_position = 4U;
//    std::string file_name;
//
//    mysql = mysql_init(nullptr);
//    mysql->reconnect = 1;
//    mysql = mysql_real_connect(mysql, "10.24.10.113", "ssh", "ssh",
//                               NULL, 3306, NULL, 0);
//    mysql_query(mysql,"SHOW MASTER STATUS;");
//    MYSQL_RES *res = mysql_store_result(mysql);
//    MYSQL_ROW row = mysql_fetch_row(res);
//    std::cout<<row[0]<<" "<<row[1]<<std::endl;
//
//    rpl.server_id =1;
//    rpl.file_name = row[0];
//    rpl.file_name_length = strlen(rpl.file_name);
//    rpl.start_position = atol(row[1]);
//
//    if (!mysql){
//        printf("Error connecting to database:%s\n",mysql_error(mysql));
//    }else{
//        printf("Connect!\n");
//    }
//
//
//    {
//        if (mysql_binlog_open(mysql, &rpl))
//        {
//            fprintf(stderr, "mysql_binlog_open() failed\n");
//            fprintf(stderr, "Error %u: %s\n",
//                    mysql_errno(mysql), mysql_error(mysql));
//            exit(1);
//        }
//        for (;;)  /* read events until error or EOF */
//        {
//            if (mysql_binlog_fetch(mysql, &rpl)) {
//                fprintf(stderr, "mysql_binlog_fetch() failed\n");
//                fprintf(stderr, "Error %u: %s\n",
//                        mysql_errno(mysql), mysql_error(mysql));
//                break;
//            }
//            if (rpl.size == 0)  /* EOF */
//            {
//                fprintf(stderr, "EOF event received\n");
//                break;
//            }
//            fprintf(stderr, "Event received of size %lu.\n", rpl.size);
//
//            f();
////        printf("lll\n");
//        }
//    }while (false);
//    mysql_binlog_close(mysql, &rpl);
//    mysql_close(mysql);
}

