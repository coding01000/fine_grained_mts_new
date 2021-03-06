#include "binlog_file_event_fetcher.h"

Binlog_file_event_fetcher::Binlog_file_event_fetcher(const char *file_dir) {

    if (file_dir==NULL){
        printf("a_binlog:bin_file == NULL? %s %d \n", ERR_POST);
        return ;
    }
    if ((fd=fopen(file_dir,"r"))==NULL){
        printf("a_binlog:fopen() binlog file error %s %d \n",ERR_POST);
        perror("openfile error");
        return ;
    }
    event_pos = 0;
    event_next = BIN_LOG_HEADER_SIZE;
    fseek(fd,OFFSET_0,SEEK_END);
    max_file_size = ftell(fd);
    fseek(fd, BIN_LOG_HEADER_SIZE, SEEK_SET);
}

int Binlog_file_event_fetcher::fetch_a_event(uint8_t* &buf, int &length) {

    if ((max_file_size - (uint64_t)event_next) <= LOG_EVENT_HEADER_LEN){
        return 1;
    }
    event_pos = event_next;
    fseek(fd, event_pos + LOG_POS_OFFSET, SEEK_SET);
    if(fread(&event_next,FLAGS_OFFSET-LOG_POS_OFFSET,O_MEM,fd) != O_MEM)
    {
        printf("analyze_binlog:fread ERROR %s %d \n",ERR_POST);
        return 2;
    }
    length = event_next - event_pos;
    fseek(fd, event_pos, SEEK_SET);
    buf = new uint8_t[length];
    fread(buf,length, O_MEM, fd);
    return 0;
}