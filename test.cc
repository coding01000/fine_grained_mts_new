#include <mysql/mysql.h>
#include <iostream>
#include <string.h>
#include <string>
#include "binary_log.h"
//#include "mysql/mysql_com.h"


int main()
{
    MYSQL *mysql;
    MYSQL_RPL rpl;

    rpl.start_position = 4U;
    std::string file_name;
    binary_log::Format_description_event *fde1;
    mysql = mysql_init(nullptr);

    mysql = mysql_real_connect(mysql, "10.24.10.113", "ssh", "ssh",
                               NULL, 3306, NULL, 0);
    mysql_query(mysql,"SHOW MASTER STATUS;");
    MYSQL_RES *res = mysql_store_result(mysql);
    MYSQL_ROW row = mysql_fetch_row(res);
    std::cout<<row[0]<<" "<<row[1]<<std::endl;

    rpl.server_id =1;
    rpl.file_name = row[0];
    rpl.file_name_length = strlen(rpl.file_name);
    rpl.start_position = atol(row[1]);

    if (!mysql){
        printf("Error connecting to database:%s\n",mysql_error(mysql));
    }else{
        printf("Connect!\n");
    }

    if (mysql_binlog_open(mysql, &rpl))
    {
        fprintf(stderr, "mysql_binlog_open() failed\n");
        fprintf(stderr, "Error %u: %s\n",
                mysql_errno(mysql), mysql_error(mysql));
        exit(1);
    }

    for (;;)  /* read events until error or EOF */
    {
        if (mysql_binlog_fetch(mysql, &rpl))
        {
            fprintf(stderr, "mysql_binlog_fetch() failed\n");
            fprintf(stderr, "Error %u: %s\n",
                    mysql_errno(mysql), mysql_error(mysql));
             break;
        }
        if (rpl.size == 0)  /* EOF */
        {
            fprintf(stderr, "EOF event received\n");
            break;
        }
        fprintf(stderr, "Event received of size %lu.\n", rpl.size);
        printf("lll\n");
        binary_log::Log_event_type type = (binary_log::Log_event_type)rpl.buffer[1 + EVENT_TYPE_OFFSET];
        if (type == binary_log::WRITE_ROWS_EVENT){
            printf("WRITE_ROWS_EVENTS!\n");
            printf("-----%d---%d\n",(fde1->get_event_type()==binary_log::FORMAT_DESCRIPTION_EVENT),
                   fde1->header()->when);
            auto *ev = new binary_log::Write_rows_event(reinterpret_cast<const char *>(rpl.buffer + 1), fde1);
            printf("-----%d---%d\n",(ev->get_event_type()==binary_log::WRITE_ROWS_EVENT),
                   ev->header()->when);
        }else if (type == binary_log::FORMAT_DESCRIPTION_EVENT){
            printf("FORMAT_DESCRIPTION_EVENT!\n");
            binary_log::Format_description_event fde = binary_log::Format_description_event(4, "8.017");
            char * buf = ( char *)malloc(sizeof(char *)*(rpl.size+1));
            memcpy(buf, rpl.buffer + 1, rpl.size-1);
            fde1 = new binary_log::Format_description_event(reinterpret_cast<const char *>(rpl.buffer + 1), &fde);
            printf("-----%d---%d\n",(fde1->get_event_type()==binary_log::FORMAT_DESCRIPTION_EVENT),
                   fde1->header()->when);
//            fde1.print_event_info(std::cout);
//            std::cout<<(fde1.get_event_type()==binary_log::FORMAT_DESCRIPTION_EVENT)<<std::endl;
        }
    }

    mysql_binlog_close(mysql, &rpl);
    mysql_close(mysql);
}

