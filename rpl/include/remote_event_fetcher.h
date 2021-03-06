#ifndef FINE_GRAINED_MTS_REMOTE_EVENT_FETCHER_H
#define FINE_GRAINED_MTS_REMOTE_EVENT_FETCHER_H
#include "event_fetcher.h"
#include "master_info.h"
#include "mysql/mysql.h"

class Remote_event_fetcher: public Event_fetcher{
public:
    int fetch_a_event(uint8_t* &buf, int &length);
    Remote_event_fetcher();

private:
    MYSQL *mysql;
    MYSQL_RPL rpl;
    bool is_connect;

};

#endif //FINE_GRAINED_MTS_REMOTE_EVENT_FETCHER_H
