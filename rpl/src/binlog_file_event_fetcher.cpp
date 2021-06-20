#include "binlog_file_event_fetcher.h"
#include "gperftools/tcmalloc.h"
#include <time.h>
#include <sys/time.h>

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
    buffer_size = 1024*1024*1024*30.7;
    event_pos = 0;
    event_next = BIN_LOG_HEADER_SIZE;
    ringBuffer = new RingBuffer<uint8_t>(buffer_size);
    tmpBuffer = new uint8_t[read_size];
    read_all_to_buffer();
//    open_a_file(iter->c_str());
//    std::thread t(&Binlog_file_event_fetcher::async_read_files, this);
//    t.detach();
}

//一次性加载
int Binlog_file_event_fetcher::read_all_to_buffer() {
    for (const auto &file : files) {
        open_a_file(file.c_str());
        while (has_read < max_file_size){
            read_size = read_size>max_file_size-has_read ? max_file_size-has_read : read_size;
            fread(tmpBuffer, read_size, O_MEM, fd);
            ringBuffer->write(tmpBuffer, read_size);
            has_read += read_size;
        }
    }
    return 0;
}
struct timeval tv;
time_t get_now(){
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

void Binlog_file_event_fetcher::init() {
    event_pos = 0;
    event_next = BIN_LOG_HEADER_SIZE;
    ringBuffer->init();
}

int Binlog_file_event_fetcher::fetch_a_event(uint8_t *&buf, int &length) {
    if (ringBuffer->size() <= LOG_EVENT_HEADER_LEN){
        return 1;
    }

    event_pos = event_next;
    event_next = uint4korr(ringBuffer->read(LOG_POS_OFFSET, FLAGS_OFFSET-LOG_POS_OFFSET));


    length = event_next - event_pos;

    if (length <= 0){
        length = event_next - BIN_LOG_HEADER_SIZE;
    }
//    time_t start = get_now();
//    buf = new uint8_t[length];
//    buf = (uint8_t *)malloc(sizeof(uint8_t)*length);
//    time_read += (get_now() - start);

    ringBuffer->read(buf, length);

    return 0;
}
//bool f = false;
//int Binlog_file_event_fetcher::open_a_file(const char *file_dir) {
//    if (file_dir==NULL){
//        printf("a_binlog:bin_file == NULL? %s %d \n", ERR_POST);
//        return 1;
//    }
//    if ((fd=fopen(file_dir,"r"))==NULL){
//        printf("a_binlog:fopen() binlog file error %s %d \n",ERR_POST);
//        perror("openfile error");
//        return 2;
//    }//1073775332
//    std::cout<<file_dir<<std::endl;
//    read_size = 1024*1024*0.5;
//    fseek(fd,OFFSET_0,SEEK_END);
//    max_file_size = ftell(fd)-4;
//    fseek(fd, BIN_LOG_HEADER_SIZE, SEEK_SET);
//    has_read = 0;
//    iter++;
//}


//int Binlog_file_event_fetcher::async_read_files(){
////    std::unique_lock<std::mutex> lock(mu);
//    while (true){
////        如果读到文件结尾，重新读下一个文件
//        if (has_read >= max_file_size){
//
//            if (iter!=files.end()){
//                open_a_file(iter->c_str());
//            }else{
//                return 1;
//            }
//        }
//        read_size = read_size>max_file_size-has_read ? max_file_size-has_read : read_size;
////        while (ringBuffer->capacity() < read_size){
////            read_cv.wait(lock);
////        }
//        while (ringBuffer->capacity() > read_size && has_read < max_file_size){
//            fread(tmpBuffer, read_size, O_MEM, fd);
//            ringBuffer->write(tmpBuffer, read_size);
//            has_read += read_size;
//        }
//    }
//}

//int Binlog_file_event_fetcher::fetch_a_event(uint8_t* &buf, int &length) {
//    if (ringBuffer->size() <= LOG_EVENT_HEADER_LEN){
//        std::cout << "read: " << time_read << std::endl;
//        return 1;
//    }
//    time_t start = get_now();
//    event_pos = event_next;
//    event_next = uint4korr(ringBuffer->read(LOG_POS_OFFSET, FLAGS_OFFSET-LOG_POS_OFFSET));
//
//    length = event_next - event_pos;
//    if (length <= 0){
//        length = event_next - BIN_LOG_HEADER_SIZE;
//    }
//    buf = new uint8_t[length];
//    ringBuffer->read(buf, length);
//    time_read += (get_now() - start);
////    read_cv.notify_one();
//    return 0;
//}

//读ringbuffer
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
//    std::cout<<file_dir<<std::endl;

    event_pos = 0;
    event_next = BIN_LOG_HEADER_SIZE;
    fseek(fd,OFFSET_0,SEEK_END);
    max_file_size = ftell(fd) - 4;
    fseek(fd, BIN_LOG_HEADER_SIZE, SEEK_SET);
//    ringBuffer->clean();
    has_read = 0;
    iter++;
}
////
//int Binlog_file_event_fetcher::fetch_a_event(uint8_t* &buf, int &length) {
//    time_t start = get_now();
//    if ((max_file_size - (uint64_t)event_next) <= LOG_EVENT_HEADER_LEN){
//        if (iter!=files.end()){
//            open_a_file(iter->c_str());
//        }else{
//            std::cout << "read: " << time_read << std::endl;
//            return 1;
//        }
//    }
//    if (max_file_size > has_read){
//        read_size = read_size>max_file_size-has_read ? max_file_size-has_read : read_size;
//        if (ringBuffer->capacity() > read_size){
//            fread(tmpBuffer, read_size, O_MEM, fd);
//            ringBuffer->write(tmpBuffer, read_size);
//            has_read += read_size;
//        }
//    }
//    event_pos = event_next;
//    event_next = uint4korr(ringBuffer->read(LOG_POS_OFFSET, FLAGS_OFFSET-LOG_POS_OFFSET));
//    length = event_next - event_pos;
//    if (length<0){
//        length = event_next - 4;
//    }
//    while (ringBuffer->size() < length){
//        if (max_file_size > has_read){
//            read_size = read_size<max_file_size-has_read ? max_file_size-has_read : read_size;
//            if (ringBuffer->capacity() > read_size){
//                fread(tmpBuffer, read_size, O_MEM, fd);
//                has_read += read_size;
//            }else {
//                printf("Big event 2!\n");
//                return 2;
//            }
//        }else{
//            printf("Big event 3!\n");
//            return 3;
//        }
//    }
//
//    buf = new uint8_t[length];
//    ringBuffer->read(buf, length);
//    time_read += (get_now() - start);
//    return 0;
//}


//原始串行读
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

Binlog_file_event_fetcher::~Binlog_file_event_fetcher() {
    delete tmpBuffer;
    delete ringBuffer;
}