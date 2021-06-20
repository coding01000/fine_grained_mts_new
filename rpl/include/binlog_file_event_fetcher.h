#ifndef FINE_GRAINED_MTS_BINLOG_FILE_EVENT_FETCHER_H
#define FINE_GRAINED_MTS_BINLOG_FILE_EVENT_FETCHER_H
#include "event_fetcher.h"
#include "cstdio"
#include "event_header.h"
#include "binary_log.h"
#include "ring_buffer.h"
#include "my_byteorder.h"
#include "atomic"
#include "condition_variable"
#include "thread"
#include "stdlib.h"
#include "gperftools/tcmalloc.h"

class Binlog_file_event_fetcher: public Event_fetcher{

public:
    Binlog_file_event_fetcher(const char *file_dir);
    int async_read_files();
    ~Binlog_file_event_fetcher();
    Binlog_file_event_fetcher(std::vector<std::string> files);
    int open_a_file(const char *file_dir);
    int fetch_a_event(uint8_t* &buf, int &length);
    int read_all_to_buffer();
    RingBuffer<uint8_t> *ringBuffer;
    void init();

private:
    FILE *fd;
    uint32_t event_pos;
    uint32_t event_next;
    uint64_t max_file_size;
    uint64_t read_size;
    uint64_t buffer_size;
    uint64_t has_read;
    uint8_t *tmpBuffer;
    std::condition_variable read_cv;
    std::mutex mu;
    std::vector<std::string> files;
    std::vector<std::string>::iterator iter;
};

#endif //FINE_GRAINED_MTS_BINLOG_FILE_EVENT_FETCHER_H
