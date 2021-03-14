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
    read_size = 1024*1024*1;
    buffer_size = 1024*1024*100;
    ringBuffer = new RingBuffer<uint8_t>(buffer_size);
    tmpBuffer = new uint8_t[read_size];
    has_read = 0;
}

Binlog_file_event_fetcher::Binlog_file_event_fetcher(std::vector<std::string> _files) {
    files = _files;
    iter = files.begin();
    fd = nullptr;
    read_size = 1024*1024*0.5;
    buffer_size = 1024*1024*200;
    ringBuffer = new RingBuffer<uint8_t>(buffer_size);
    tmpBuffer = new uint8_t[read_size];
    open_a_file(iter->c_str());
}

int Binlog_file_event_fetcher::open_a_file(const char *file_dir) {
    if (file_dir==NULL){
        printf("a_binlog:bin_file == NULL? %s %d \n", ERR_POST);
        return 1;
    }
    if ((fd=fopen(file_dir,"r"))==NULL){
        printf("a_binlog:fopen() binlog file error %s %d \n",ERR_POST);
        perror("openfile error");
        return 2;
    }
    std::cout<<file_dir<<std::endl;

    event_pos = 0;
    event_next = BIN_LOG_HEADER_SIZE;
    fseek(fd,OFFSET_0,SEEK_END);
    max_file_size = ftell(fd);
    fseek(fd, BIN_LOG_HEADER_SIZE, SEEK_SET);
    ringBuffer->clean();
    has_read = 0;
    iter++;
}

int Binlog_file_event_fetcher::fetch_a_event(uint8_t* &buf, int &length) {
    if ((max_file_size - (uint64_t)event_next) <= LOG_EVENT_HEADER_LEN){
        if (iter!=files.end()){
            open_a_file(iter->c_str());
        }else{
            return 1;
        }
    }
    if (max_file_size > has_read){
        read_size = read_size>max_file_size-has_read ? max_file_size-has_read : read_size;
        if (ringBuffer->capacity() > read_size){
            fread(tmpBuffer, read_size, O_MEM, fd);
            ringBuffer->write(tmpBuffer, read_size);
            has_read += read_size;
        }
    }
    event_pos = event_next;
    event_next = uint4korr(ringBuffer->read(LOG_POS_OFFSET, FLAGS_OFFSET-LOG_POS_OFFSET));
    length = event_next - event_pos;
    while (ringBuffer->size() < length){
        if (max_file_size > has_read){
            read_size = read_size<max_file_size-has_read ? max_file_size-has_read : read_size;
            if (ringBuffer->capacity() > read_size){
                fread(tmpBuffer, read_size, O_MEM, fd);
                has_read += read_size;
            }else {
                printf("Big event 2!\n");
                return 2;
            }
        }else{
            printf("Big event 3!\n");
            return 3;
        }
    }

    buf = new uint8_t[length];
    ringBuffer->read(buf, length);
    return 0;
}

//int Binlog_file_event_fetcher::fetch_a_event(uint8_t* &buf, int &length) {
//
//    if ((max_file_size - (uint64_t)event_next) <= LOG_EVENT_HEADER_LEN){
//        return 1;
//    }
//    event_pos = event_next;
//    fseek(fd, event_pos + LOG_POS_OFFSET, SEEK_SET);
//    if(fread(&event_next,FLAGS_OFFSET-LOG_POS_OFFSET,O_MEM,fd) != O_MEM)
//    {
//        printf("analyze_binlog:fread ERROR %s %d \n",ERR_POST);
//        return 2;
//    }
//    length = event_next - event_pos;
//    fseek(fd, event_pos, SEEK_SET);
//    buf = new uint8_t[length];
//    fread(buf,length, O_MEM, fd);
//    return 0;
//}