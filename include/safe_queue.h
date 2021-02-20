#ifndef FINE_GRAINED_MTS_SAFE_QUEUE_H
#define FINE_GRAINED_MTS_SAFE_QUEUE_H
#include <queue>
#include <mutex>
//#include "../utils/safe_queue.cc"

template <typename T>
class SafeQueue{
public:
    std::mutex mu;
    std::queue<T> q;
public:
    SafeQueue();
    ~SafeQueue();
    bool empty();
    int size();
    void enqueue(T &t);
    bool dequeue(T &t);
};

#endif //FINE_GRAINED_MTS_SAFE_QUEUE_H
