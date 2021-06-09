#ifndef FINE_GRAINED_MTS_QUERY_FERQ_H
#define FINE_GRAINED_MTS_QUERY_FERQ_H
#include "vector"
#include "string"
#include "set"

class query_freq{
public:
    int ferq;
    std::set<int> query_group;
};

#endif //FINE_GRAINED_MTS_QUERY_FERQ_H
