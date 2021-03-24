#include "remote_event_fetcher.h"

Remote_event_fetcher::Remote_event_fetcher() {
//    Master_info masterInfo;
//    mysql = mysql_init(nullptr);
//    mysql->reconnect = 1;
//    mysql = mysql_real_connect(mysql, masterInfo.host.c_str(), masterInfo.user_name.c_str(), masterInfo.pwd.c_str(),
//                               NULL, masterInfo.port, NULL, 0);
//    if (!mysql){
//        printf("Error connecting to database:%s\n",mysql_error(mysql));
//        return ;
//    }
//    mysql_query(mysql,"SHOW MASTER STATUS;");
//    MYSQL_RES *res = mysql_store_result(mysql);
//    MYSQL_ROW row = mysql_fetch_row(res);
//    rpl.start_position = 4U;
//    rpl.server_id =1;
//    rpl.file_name = row[0];
//    rpl.file_name_length = strlen(rpl.file_name);
//    rpl.start_position = atol(row[1]);
//    rpl.flags = MYSQL_RPL_SKIP_HEARTBEAT;
//    is_connect = false;
}

int Remote_event_fetcher::fetch_a_event(uint8_t* &buffer, int &length) {
//    if (!is_connect){
//        if (mysql_binlog_open(mysql, &rpl))
//        {
//            fprintf(stderr, "mysql_binlog_open() failed\n");
//            fprintf(stderr, "Error %u: %s\n",
//                    mysql_errno(mysql), mysql_error(mysql));
//            exit(1);
//        }
//        is_connect = true;
//    }
//    if (mysql_binlog_fetch(mysql, &rpl)) {
//        fprintf(stderr, "mysql_binlog_fetch() failed\n");
//        fprintf(stderr, "Error %u: %s\n",
//                mysql_errno(mysql), mysql_error(mysql));
//        return 2;
//    }
//    if (rpl.size == 0)  /* EOF */
//    {
//        fprintf(stderr, "EOF event received\n");
//        return 1;
//    }
//    buffer = (uint8_t *)malloc(sizeof(uint8_t)*rpl.size);
//    memset(buffer, 0, sizeof(uint8_t)*rpl.size);
//    length = rpl.size - 1;
//    memcpy(buffer, rpl.buffer+1, length);
//    return 0;
}