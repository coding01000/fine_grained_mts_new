#include <mysql/mysql.h>
#include <iostream>


MYSQL mysql;

int main()
{
    mysql_init(&mysql);
//    if ((mysql_real_connect(&mysql, "192.168.1.96", "zhangfy", "ZfengY!0910",
//                            "db_ods", 3306, NULL, 0))){
    if ((mysql_real_connect(&mysql, "10.24.10.147", "root", "root",
                            NULL, 3306, NULL, 0))!=NULL){
        printf("Error connecting to database:%s\n",mysql_error(&mysql));
    }else{
        printf("connect!\n");
    }
    mysql_close(&mysql);
}

