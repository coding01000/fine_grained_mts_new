#ifndef FINE_GRAINED_MTS_EVENT_FETCHER_H
#define FINE_GRAINED_MTS_EVENT_FETCHER_H
#include "cstdint"

class Event_fetcher{
public:
    virtual int fetch_a_event(uint8_t* &buf, int &length) = 0;
    virtual void init(){};
};

#endif //FINE_GRAINED_MTS_EVENT_FETCHER_H
