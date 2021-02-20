#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<stdint.h>
#include <string.h>
#include <thread>
#include <mutex>
#include <functional>
#include<stdio.h>
#include <unordered_map>
#include <unistd.h>
#include "include/event_header.h"
#include "include/safe_queue.h"
#include "include/table.h"
#include <iostream>
#include <random>
#include "include/thread_pool.h"
//#include "include/thread_pool.h"
static MAP_TAB my_tab;
static QUERY_EVENT my_query;
static uint64_t GTID_GNO_;
static uint32_t event_time = 0;

static int GT = 1;
static Table table;
unsigned int FORCE = 0;
ThreadPool pool(6);
//std::vector<ThreadPool *> pool;

std::unordered_map<uint64_t, SafeQueue<Row *> * > q_map; //未执行的序列
std::condition_variable q_cv;

//还未考虑优先级的问题
void insert_a_record(Row * row){
    //可以把入队代码放到解析事务的代码中一起加锁，保证入队顺序的严格一致
    auto it = q_map.find(row->primary_ley);
    SafeQueue<Row *> * q;
    if (it==q_map.end()){
        q = new SafeQueue<Row *>();
        q_map[row->primary_ley] = q;
    }else{
        q = it->second;
    }
    q->enqueue(row);
//    sleep(2);
    std::unique_lock<std::mutex> lock(q->mu);
    while (q->q.front()->event_time!=row->event_time){
        q_cv.wait(lock);
    }
    table.insert_row(row);
    q->q.pop();
    q_cv.notify_all();
}

int check_asii_num(const char* num)
{
    int i;
    int ret = 0;

    for(i=0;i<(int)strlen(num);i++)
    {
        if(*(num+i)<0X30 || *(num+i)>0x39 )
        {
            ret = -1;
            break;
        }
    }
    if(ret == -1 )
    {
        printf("parameter must number type %s %d \n",ERR_POST);
        return ret;
    }

    return ret;
}

int xfree(void* p)
{
    if( p == NULL)
    {
        return 0;
    }
    free(p);
    p = NULL;
    return 0;
}

//1: Big_endian
//0: Little_endian
//2: Unkown
int check_lit(void)
{
    char* test = NULL;
    short* m = NULL;

    test =(char* )calloc(2,1);

    strcpy(test,"lb");
    m = (short* )test;

    if(*m == 0x626c)
    {
        printf("Check is Little_endian\n");
        return 0;
    }
    else if(*m == 0x6c62)
    {
        printf("Check is Big_endian\n");
        return 1;
    }
    else
    {
        printf("Check is Unkown\n");
        return 2;
    }

}

int check_bin_format(FILE* fd)
{
    int ret = 0;
    FILE* fds = NULL;
    char bin_maico[4];
    uint16_t binlog_format = 0;
    char mysql_format[51];
    uint32_t event_header_size = 0;
    int32_t  *bin_maico_nu = NULL;
    uint16_t flags;
    uint64_t totalsize;

    memset(bin_maico,0,4);
    memset(mysql_format,0,51);
    fds = fd;
    if(fd == NULL)
    {
        ret = 1;
        printf("check_bin_format == NULL? %s %d \n",ERR_POST);
        return ret;
    }

    if(fread(bin_maico,BIN_LOG_HEADER_SIZE,O_MEM,fds) != O_MEM)
    {
        ret = 2;
        printf("check_bin_format fread ERROR %s %d \n",ERR_POST);
        return ret;
    }

    bin_maico_nu = (int32_t *)bin_maico;



    //fseek(fds,LOG_EVENT_HEADER_LEN,SEEK_CUR);
    //add
    fseek(fds,LOG_POS_OFFSET,SEEK_CUR);
    if(fread(&flags,FLAGS_OFFSET-LOG_POS_OFFSET,O_MEM,fds) != O_MEM)
    {
        ret = 20;
        printf("check_bin_format fread ERROR %s %d \n",ERR_POST);
        return ret;
    }

    //end
    if(fread(&binlog_format,FORMAT_V-FLAGS_OFFSET,O_MEM,fds) != O_MEM)
    {
        ret = 3;
        printf("check_bin_format fread ERROR %s %d \n",ERR_POST);
        return ret;
    }

    //fseek(fds,1,SEEK_CUR);

    if(fread(mysql_format,FED_MYSQL_FORMAT-FED_BINLOG_FORMAT,O_MEM,fds) != O_MEM)
    {
        ret = 4;
        printf("check_bin_format fread ERROR %s %d \n",ERR_POST);
        return ret;
    }
    fseek(fds,FED_USED - FED_MYSQL_FORMAT,SEEK_CUR);

    if(fread(&event_header_size,FED_EVENT_HEADER-FED_USED,O_MEM,fds)!=O_MEM)
    {
        ret = 5;
        printf("check_bin_format fread ERROR %s %d \n",ERR_POST);
        return ret;
    }

    if(*bin_maico_nu != BINLOG_MAICO )
    {
        ret = 6;
        printf("check_bin_format:not binlogfile? ERROR  %s %d \n",ERR_POST);
        return ret;
    }

    if(event_header_size != BINLOG_HEADER_LENGTH || binlog_format != BINLOG_VERSION )
    {
        ret = 7;
        printf("check_bin_format:binlog_format not v4? ERROR %s %d \n",ERR_POST);
        return ret;
    }

    fseek(fds,0,SEEK_END);
    totalsize = ftell(fds);

    printf("Check Mysql Version is:%s\n",mysql_format);
    printf("Check Mysql binlog format ver is:V%u\n",binlog_format);
    if(flags == (uint16_t)1)
    {
        printf("Warning:Check This binlog is not closed!\n" );
    }
    else
    {
        printf("Check This binlog is closed!\n" );
    }
    printf("Check This binlog total size:%lu(bytes)\n",totalsize);
    printf("Note:load data infile not check!\n");
    return ret;

}

int gtid_handdle(unsigned char* gtid_buf)
{
    int i = 0;
    int ret = 0;
    uint64_t *gno = NULL;


    memset(&GTID_GNO_,0,sizeof(uint64_t));

    if(gtid_buf == NULL)
    {
        ret = 1;
        printf("gtid_handdle:gtid_buf == NULL ? ERROR %s %d \n",ERR_POST);
        return ret;
    }
    if(GT){
        printf("Gtid:");
    }
    for(i = 0;i<16;i++)
    {
        if(i == 4)
        {
            if(GT){
                printf("-");
            }
        }
        if(i == 6)
        {
            if(GT){
                printf("-");
            }
        }
        if(i == 8)
        {
            if(GT){
                printf("-");
            }
        }
        if(i == 10)
        {
            if(GT){
                printf("-");
            }
        }
        if(GT){
            printf("%x",gtid_buf[i+1]);
        }
    }

    gno = (uint64_t*)(gtid_buf+17);
    GTID_GNO_ = *gno;
    if(GT){
        printf(":%lu",*gno);
    }
    return ret;
}

int gtid_com_handdle(unsigned char* gtid_co_seq)
{
    int ret = 0;
    uint64_t* data;


    if(gtid_co_seq == NULL)
    {
        ret = 1;
        printf("gtid_com_handdle:gtid_com_seq == NULL ? ERROR %s %d \n",ERR_POST);
        return ret;
    }

    data = (uint64_t*)gtid_co_seq;

    if(GT){
        printf(" last_committed=%ld  sequence_number=%ld\n",*(data),*(data+1));
    }

    return ret;
}

int map_handle(FILE* fds)
{
    int ret = 0 ;
    uint8_t tab_id[6];
    uint8_t db_len = 0;
    uint8_t tab_len = 0;


    if(fds == NULL)
    {
        ret = 10;
        printf("map_handle:fds == NULL? ERROR %s %d \n",ERR_POST);
        return ret;
    }
    memset(&my_tab,0,sizeof(MAP_TAB));
    memset(tab_id,0,6);
    if(fread(tab_id,MAP_TABLE_ID,O_MEM,fds) != O_MEM)
    {
        ret = 1;
        printf("map_handle:fread ERROR %s %d \n",ERR_POST);
        return ret;
    }
    memcpy(&my_tab.table_id,tab_id,6);
    if(GT){
        printf("TABLE_ID:%lu ",my_tab.table_id);
    }

    fseek(fds,MAP_UNSUED-MAP_TABLE_ID,SEEK_CUR);

    if( fread(&db_len,MAP_DB_LENGTH,O_MEM,fds) != O_MEM ||
        fread(my_tab.db_name,db_len,O_MEM,fds) != O_MEM)
    {
        ret = 2;
        printf("map_handle:fread ERROR %s %d \n",ERR_POST);
        return ret;
    }
    if(GT){
        printf("DB_NAME:%s ",my_tab.db_name);
    }
    fseek(fds,1,SEEK_CUR); //0X00

    if(fread(&tab_len,MAP_TABLE_LENGTH,O_MEM,fds) != O_MEM ||
       fread(my_tab.tab_name,tab_len,O_MEM,fds) != O_MEM)
    {
        ret = 3;
        printf("map_handle:fread ERROR %s %d \n",ERR_POST);
        return ret;
    }
    if(GT){
        printf("TABLE_NAME:%s Gno:%lu\n",my_tab.tab_name,GTID_GNO_);
    }
    return ret;

}

int delete_handle(FILE* fds)
{
    int ret = 0;
    uint64_t table_id;
    uint64_t *num;
    uint8_t *str;
    std::vector<void *> column_values;
    fread(&table_id,DML_TABLE_ID,O_MEM,fds);
    fseek(fds, 7, SEEK_CUR);
    for(int i=0;i<table.column_width.size();i++){
        int width = table.column_width[i];
        switch (width) {
            case 4:
                num = new uint64_t();
                fread(num, 4, O_MEM, fds);
                column_values.push_back(num);
                printf(" %d", (*num));
                break;
            case 0:
                uint8_t len;
                str = new uint8_t[512]();
                memset(str,0,512);
                fread(&len, 1, O_MEM, fds);
                fread(str, len, O_MEM, fds);
                column_values.push_back(str);
                printf(" %s", str);
                break;
        }
    }
    Row *row = new Row(*(uint64_t *)column_values[0], column_values, event_time, true);
//    table.insert_row(row);
    pool.submit(insert_a_record, row);
    printf("\n");
    return ret;
}

int update_handle(FILE* fds)
{
    int ret = 0;
    uint64_t table_id;
    uint64_t *num;
    uint8_t *str;
    std::vector<void *> column_values;
    fread(&table_id,DML_TABLE_ID,O_MEM,fds);
    fseek(fds, 7, SEEK_CUR);
    fseek(fds, 1, SEEK_CUR);
    for(int i=0;i<table.column_width.size();i++){
        int width = table.column_width[i];
        switch (width) {
            case 4:
                fseek(fds, 4, SEEK_CUR);
                break;
            case 0:
                uint8_t len;
                fread(&len, 1, O_MEM, fds);
                fseek(fds, len, SEEK_CUR);
                break;
        }
    }
    fseek(fds, 1, SEEK_CUR);
    for(int i=0;i<table.column_width.size();i++){
        int width = table.column_width[i];
        switch (width) {
            case 4:
                num = new uint64_t();
                fread(num, 4, O_MEM, fds);
                column_values.push_back(num);
                printf(" %d", (*num));
                break;
            case 0:
                uint8_t len;
                str = new uint8_t[512]();
                memset(str,0,512);
                fread(&len, 1, O_MEM, fds);
                fread(str, len, O_MEM, fds);
                column_values.push_back(str);
                printf(" %s", str);
                break;
        }
    }
    Row *row = new Row(*(uint64_t *)column_values[0], column_values, event_time, 0);
//    table.insert_row(row);
    pool.submit(insert_a_record, row);
    printf("\n");
    return ret;
}

int insert_handle(FILE* fds)
{
    int ret = 0;
    uint64_t table_id;
    uint64_t *num;
    uint8_t *str;
    std::vector<void *> column_values;
    fread(&table_id,DML_TABLE_ID,O_MEM,fds);
    fseek(fds, 7, SEEK_CUR);
    for(int i=0;i<table.column_width.size();i++){
        int width = table.column_width[i];
        switch (width) {
            case 4:
                num = new uint64_t();
                fread(num, 4, O_MEM, fds);
                column_values.push_back(num);
                printf(" %d", (*num));
                break;
            case 0:
                uint8_t len;
                str = new uint8_t[512]();
                memset(str,0,512);
                fread(&len, 1, O_MEM, fds);
                fread(str, len, O_MEM, fds);
                column_values.push_back(str);
                printf(" %s", str);
                break;
        }
    }
    Row *row = new Row(*(uint64_t *)column_values[0], column_values, event_time, 0);
//    table.insert_row(row);
//    insert_a_record(row);
    pool.submit(insert_a_record, row);
    printf("\n");
    return ret;
}

int dml_handle(FILE* fds)
{
    int ret = 0 ;
    uint8_t tab_id[6];
    uint64_t table_id = 0;


    if(fds == NULL)
    {
        ret = 10;
        printf("dml_handle:fds == NULL? ERROR %s %d \n",ERR_POST);
        return ret;
    }
    memset(tab_id,0,6);
    if(fread(tab_id,DML_TABLE_ID,O_MEM,fds) != O_MEM)
    {
        ret = 1;
        printf("dml_handle:fread ERROR %s %d \n",ERR_POST);
        return ret;
    }

    memcpy(&table_id,tab_id,6);

    if(table_id == my_tab.table_id)
    {
        if(GT)
        {
            printf("Dml on table: %s.%s  table_id:%lu Gno:%lu \n",my_tab.db_name,my_tab.tab_name, my_tab.table_id,GTID_GNO_);
        }
    }
    else
    {
        if(FORCE == 0)
        {
            ret = 2;
            printf("dml_handle: table_id cmp ERROR %s %d \n",ERR_POST);
            return ret;
        }
    }

    return ret;
}

int query_handle(FILE* fds,uint32_t event_size,uint32_t event_pos)
{
    int ret = 0;
    uint8_t db_len = 0;
    uint16_t meta_len = 0;
    uint32_t sta_len = 0;
    char  sta[36];

    if( fds == NULL)
    {
        ;
    }
    memset(&my_query,0,sizeof(QUERY_EVENT));
    memset(sta,0,35);

    fseek(fds,QUERY_T_ID,SEEK_CUR);//skip thread_id 4 bytes
    if(fread(&my_query.exe_time,QUERY_EXE_T-QUERY_T_ID,O_MEM,fds) != O_MEM)
    {
        ret = 1;
        printf("query_handle:fread ERROR %s %d \n",ERR_POST);
        return ret;
    }
    if(fread(&db_len,QUERY_DEFAULT_DB_LEN - QUERY_EXE_T,O_MEM,fds) != O_MEM)
    {
        ret = 2;
        printf("query_handle:fread ERROR %s %d \n",ERR_POST);
        return ret;
    }
    fseek(fds,QUERY_ERR_CODE-QUERY_DEFAULT_DB_LEN,SEEK_CUR);//skip error_code

    if(fread(&meta_len,QUERY_META_BLOCK_LEN-QUERY_ERR_CODE,O_MEM,fds) != O_MEM)
    {
        ret = 3;
        printf("query_handle:fread ERROR %s %d \n",ERR_POST);
        return ret;
    }

    fseek(fds,meta_len,SEEK_CUR);
    if(db_len != 0)
    {
        if(fread(my_query.db_name,db_len,O_MEM,fds) != O_MEM)
        {
            ret = 4;
            printf("query_handle:fread ERROR %s %d \n",ERR_POST);
            return ret;
        }
    }
    else
    {
        strcpy(reinterpret_cast<char *>(my_query.db_name), "!<slave?nodbname>");
    }

    fseek(fds,O_MEM,SEEK_CUR);

    sta_len = event_size - LOG_EVENT_HEADER_LEN - QUERY_META_BLOCK_LEN - meta_len - db_len - O_MEM - CRC_LEN;

    if((my_query.statment = (uint8_t* )calloc(O_MEM,sta_len+5)) == NULL)
    {
        ret = 5;
        printf("query_handle:calloc ERROR %s %d \n",ERR_POST);
        return ret;
    }

    if(fread(my_query.statment,sta_len,O_MEM,fds) != O_MEM)
    {
        ret = 6;
        printf("query_handle:fread ERROR %s %d \n",ERR_POST);
        return ret;
    }


    strncpy(sta, reinterpret_cast<const char *>(my_query.statment), 35);
    //printf("db_len:%u meta len:%u  sta len: %u \n",db_len,meta_len,sta_len);
    if(GT){
        printf("Exe_time:%u  Use_db:%s Statment(35b-trun):%s",my_query.exe_time,my_query.db_name,sta);
    }
    //add 2017 08 12
//    EXE_T =  my_query.exe_time ;
    if(!strcmp(sta,"BEGIN") || !strcmp(sta,"B"))
        //!strcmp(sta,"B") 5.5 only
    {
//        POS_T = (uint64_t)event_pos;
        if(GT){
            printf(" /*!Trx begin!*/ Gno:%lu\n",GTID_GNO_);
        }
    }
    else
    {
        if(GT){
            printf(" Gno:%lu\n",GTID_GNO_);
        }
    }

    xfree(my_query.statment);

    return ret;
}

int binlog_handle(FILE* fd)
{
    int ret = 0;
    FILE* fds = NULL;
    uint32_t event_pos = 0;
    uint8_t  event_type = 0;
    uint32_t event_next = 4;
    uint64_t max_file_size = 0;

    uint32_t event_size = 0;

    unsigned char  gtid_buf[25];
    unsigned char  gtid_co_seq[16];

    fds = fd;

    if(fds == NULL)
    {
        ret = 10;
        printf("binlog_handle:fds == NULL? ERROR %s %d \n",ERR_POST);
        return ret;
    }

    memset(gtid_buf,0,25);
    memset(gtid_co_seq,0,16);
    fseek(fds,OFFSET_0,SEEK_END);
    max_file_size = ftell(fds);

    printf("------------Detail now--------------\n");

    while((max_file_size - (uint64_t)event_next) > LOG_EVENT_HEADER_LEN )
    {
        printf("\n");
        fseek(fds,event_next,SEEK_SET);
        event_pos = ftell(fds);

        if (fread(&event_time,EVENT_TIMESTAMP,O_MEM,fds) != O_MEM)
        {
            ret = 2;
            printf("binlog_handle:fread ERROR %s %d \n",ERR_POST);
            return ret;
        }

        if(fread(&event_type,EVENT_TYPE_OFFSET - EVENT_TIMESTAMP,O_MEM,fds)!=O_MEM)
        {
            ret = 3;
            printf("binlog_handle:fread ERROR %s %d \n",ERR_POST);
            return ret;
        }

        fseek(fds,EVENT_LEN_OFFSET-EVENT_TYPE_OFFSET,SEEK_CUR);

        if(fread(&event_next,LOG_POS_OFFSET - EVENT_LEN_OFFSET,O_MEM,fds) != O_MEM)
        {
            ret =4;
            printf("binlog_handle:fread ERROR %s %d \n",ERR_POST);
            return ret;
        }

        event_size = event_next - event_pos;

        switch(event_type)
        {
            case Insert_Event_TYPE:
                printf("------>Insert Event:Pos:%u(0X%x) N_pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                fseek(fds,FLAGS_OFFSET-LOG_POS_OFFSET,SEEK_CUR);
                if(insert_handle(fds) != 0)
                {
                    ret = 8;
                    printf("binlog_handle:dml_handle ERROR %s %d \n",ERR_POST);
                    return ret;
                }
                break;
            case Update_Event_TYPE:
                printf("------>Update Event:Pos:%u(0X%x) N_pos is:%u(0X%x) time:%u event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                fseek(fds,FLAGS_OFFSET-LOG_POS_OFFSET,SEEK_CUR);
                if(update_handle(fds) != 0)
                {
                    ret = 9;
                    printf("binlog_handle:dml_handle ERROR %s %d \n",ERR_POST);
                    return ret;
                }
                break;
            case Delete_Event_TYPE:
                printf("------>Delete Event:Pos:%u(0X%x) N_pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                fseek(fds,FLAGS_OFFSET-LOG_POS_OFFSET,SEEK_CUR);
                if(delete_handle(fds) != 0)
                {
                    ret = 10;
                    printf("binlog_handle:dml_handle ERROR %s %d \n",ERR_POST);
                    return ret;
                }
                break;
            case 19:
                if(GT){
                    printf("---->Map Event:Pos%u(0X%x) N_pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                }
                fseek(fds,FLAGS_OFFSET-LOG_POS_OFFSET,SEEK_CUR);

                if(map_handle(fds) != 0 )
                {
                    ret = 7;
                    printf("binlog_handle:map_handle ERROR %s %d \n",ERR_POST);
                    return ret;
                }
                break;
            case 33:
                if(GT){
                    printf(">Gtid Event:Pos:%u(0X%x) N_pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                }
                fseek(fds,FLAGS_OFFSET-LOG_POS_OFFSET,SEEK_CUR);

                if(fread(gtid_buf,GTID_GNO,O_MEM,fds) != O_MEM)
                {
                    ret = 6;
                    printf("binlog_handle:fread gtid_buf ERROR %s %d \n",ERR_POST);
                    return ret;
                }

                if( gtid_handdle(gtid_buf) != 0 )
                {
                    ret = 5;
                    printf("binlog_handle:gtid_handdle() ERROR %s %d \n",ERR_POST);
                    return ret;
                }


                if(event_size > (uint32_t)44)
                {
                    fseek(fds,GTID_FLAG-GTID_GNO,SEEK_CUR);
                    if(fread(gtid_co_seq,GTID_SEQ-GTID_FLAG,O_MEM,fds) != O_MEM)
                    {
                        ret = 7;
                        printf("binlog_handle:fread gtid_co_seq ERROR %s %d \n",ERR_POST);
                        return ret;

                    }
                    if(gtid_com_handdle(gtid_co_seq) != 0 )
                    {
                        ret =  8 ;
                        printf("binlog_handle:gtid_com_handdle() ERROR %s %d \n",ERR_POST);
                        return ret;
                    }
                }

                memset(gtid_buf,0,25);
                memset(gtid_co_seq,0,16);
                break;
            case 2:
                if(GT){
                    printf("-->Query Event:Pos:%u(0X%x) N_Pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                }
                fseek(fds,FLAGS_OFFSET-LOG_POS_OFFSET,SEEK_CUR);
//                QUR_T = (uint32_t)event_time; //获得query event 时间
//                QUR_POS = (uint32_t)event_pos; //获得query event pos位置 OK

                if(query_handle(fds,event_size,event_pos) != 0)
                {
                    ret =12;
                    printf("binlog_handle:query_handle ERROR %s %d \n",ERR_POST);
                    return ret;
                }

                break;
            case 16:
                if(GT){
                    printf(">Xid Event:Pos:%u(0X%x) N_Pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                }
//                QUR_X = (uint32_t)event_time; //获得xid event时间  OK
//                QUR_POS_END = (uint64_t)event_next;//获得xid event 结束位置
                if(GT){
                    printf("COMMIT; /*!Trx end*/ Gno:%lu\n",GTID_GNO_);
                }
                break;
            case 34:
                if(GT){
                    printf(">Anonymous gtid Event:Pos:%u(0X%x) N_pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                    printf("Gtid:Anonymous(Gno=0)");
                }
                if(event_size > (uint32_t)44)
                {
                    fseek(fds,FLAGS_OFFSET-LOG_POS_OFFSET,SEEK_CUR);
                    fseek(fds,GTID_FLAG,SEEK_CUR);
                    if(fread(gtid_co_seq,GTID_SEQ-GTID_FLAG,O_MEM,fds) != O_MEM)
                    {
                        ret = 50;
                        printf("binlog_handle:fread gtid_co_seq ERROR %s %d \n",ERR_POST);
                        return ret;
                    }
                    if(gtid_com_handdle(gtid_co_seq) != 0 )
                    {
                        ret = 51;
                        printf("binlog_handle:gtid_com_handdle() ERROR %s %d \n",ERR_POST);
                        return ret;
                    }
                }
                memset(gtid_buf,0,25);
                memset(gtid_co_seq,0,16);
                break;
            case 35:
                if(GT){
                    printf(">Previous gtid Event:Pos:%u(0X%x) N_pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                }
                break;
            case 3:
                if(GT){
                    printf(">Stop log Event:Pos:%u(0X%x) N_pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                }
                break;
            case 4:
                if(GT){
                    printf(">Rotate log Event:Pos:%u(0X%x) N_pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                }
                break;
            case 5:
                if(GT){
                    printf(">Intvar log Event:Pos:%u(0X%x) N_pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                }
                break;
            case 6:
                if(GT){
                    printf(">Rand log Event:Pos:%u(0X%x) N_pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                }
                break;
            case 15:
                if(GT){
                    printf(">Format description log Event:Pos:%u(0X%x) N_pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                }
//                BEG_TIME = event_time;//add
                break;
            case 29:
                if(GT){
                    printf("-->Rows query(use execute sql) log Event:Pos:%u(0X%x) N_pos:%u(0X%x) Time:%u Event_size:%u(bytes) \n",event_pos,event_pos,event_next,event_next,event_time,event_size);
                }
                break;
            default:
                break;
        }
    }
    return ret;

}

int binlog_parse(const char* bin_file)
{
    int ret = 0;
    FILE* fd;
    if(bin_file == NULL)
    {
        ret = 1;
        printf("binlog_handle:bin_file == NULL? %s %d \n",ERR_POST);
        return ret;
    }

    if((fd=fopen(bin_file,"r"))==NULL)
    {
        ret = 2;
        printf("binlog_handle:fopen() binlog file error %s %d \n",ERR_POST);
        perror("openfile error");
        return ret;
    }

    if(check_bin_format(fd) != 0)
    {
        ret = 3;
        printf("binlog_handle:check_bin_format() binlog error %s %d \n",ERR_POST);
        return ret;
    }

    if(binlog_handle(fd) != 0 )
    {
        ret = 4;
        printf("binlog_handle:binlog_handle() error %s %d \n",ERR_POST);
        return ret ;
    }

    return ret;

}

int main(int argc,char* argv[])
{
//    for (int i=1;i<3;i++){
//        ThreadPool * p = new ThreadPool(i);
//        p->init();
//        pool.push_back(p);
//    }
    pool.init();
    std::vector<std::string> column_names;
    column_names.push_back("id");
    column_names.push_back("name");
    column_names.push_back("aa");
    std::vector<uint8_t> column_width;
    column_width.push_back(4);
    column_width.push_back(0);
    column_width.push_back(4);
    table = Table("test", column_names, column_width);

    int ret = 0;

    if(check_lit() != 0 )
    {
        ret = 3;
        printf("ERROR:This tool only Little_endian platform!\n");
        return ret;
    }

    printf("-------------Now begin--------------\n");

    if(binlog_parse(*(argv+1)) != 0 )
    {
        ret = 2;
        printf("ERROR:a_binlog fun error\n");
        return ret;
    }
    pool.shutdown();
//    for (auto it=pool.begin();it!=pool.end();it++){
//        (*it)->shutdown();
//    }
    std::unordered_map<int, Hash_Header*>::iterator it;
    for (it=table.rows.begin();it!=table.rows.end();it++){
        Row *row = it->second->next;
        do{
            printf("%d %s %d ", *(uint64_t *)(row->column_values[0]),(uint8_t *)row->column_values[1], *(uint64_t *)row->column_values[2]);
            printf("%d \n", row->event_time);
            row = row->next;
        }while(row);
    }
    return ret;
}
//std::mutex mu;
//std::condition_variable cv;
//class Test{
//public:
////    std::mutex mu;
//     void print(int time){
//        std::unique_lock<std::mutex> lock(mu);
////        cv.notify_all();
//        sleep(time);
//        printf("llllllll%d\n", time);
//    }
//};
////
////int main(){
////    Test t1;
////    Test t2;
////    std::thread a(std::bind(&Test::print, &t1, 5));
////    std::thread b(std::bind(&Test::print, &t2, 2));
//////    std::thread a(print,2);
////    a.join();
////    sleep(1);
////    b.join();
//////    std::thread();
////}
//bool ready = false;
//void print1(int time)
//{
//    std::unique_lock<std::mutex> lock(mu);
//    ready=true;
//    cv.notify_all();
//    sleep(time);
//    printf("11111111111\n");
//}
//
//int a = 2;
//void print2(int time)
//{
//    std::unique_lock<std::mutex> lock(mu);
//    while (a!=1){
//        cv.wait(lock);
//    }
//    sleep(time);
//    printf("22222222222   %d\n", a);
//    cv.notify_all();
//}
//int main() {
//    // 创建3个线程的线程池
//
//    ThreadPool pool(3);
//    pool.init();
//    Test t1;
//    Test t2;
//    pool.submit(print2, 5);
//    sleep(1);
//    a = 1;
//    pool.submit(print2, 5);
//
////    pool.submit(std::bind(&Test::print, &t1, 1));
////    sleep(0);
////    pool.submit(std::bind(&Test::print, &t2, 2));
////    sleep(20);
//    //关闭线程池
//    pool.shutdown();
//}

