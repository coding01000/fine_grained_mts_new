#ifndef FINE_GRAINED_MTS_SAFE_QUEUE_H
#define FINE_GRAINED_MTS_SAFE_QUEUE_H
#include <queue>
#include <mutex>
//#include <mutex>
#include "unordered_map"
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

template <typename T>
class SafePriorityQueue{
public:
    std::mutex mu;
    std::priority_queue<T> q;
public:
    SafePriorityQueue(){};
    ~SafePriorityQueue(){};
    bool empty(){
        std::lock_guard<std::mutex> lock(mu);
        return q.empty();
    }
    int size(){
        std::lock_guard<std::mutex> lock(mu);
        return q.size();
    }
    void enqueue(T &t){
        std::lock_guard<std::mutex> lock(mu);
        q.push(t);
    }
    bool dequeue(T &t){
        std::lock_guard<std::mutex> lock(mu);
        if(q.empty()){
            return false;
        }
        t = std::move(q.top());   //取出队首元素，返回队首元素值，并进行右值引用
        q.pop();
        return true;
    }
    T front(){
        std::lock_guard<std::mutex> lock(mu);
        return q.top();
    }
};


template<typename T>
SafeQueue<T>::~SafeQueue() {}

template<typename T>
SafeQueue<T>::SafeQueue() {}

template<typename T>
bool SafeQueue<T>::empty() {
    std::lock_guard<std::mutex> lock(mu);
    return q.empty();
}

template<typename T>
int SafeQueue<T>::size() {
    std::lock_guard<std::mutex> lock(mu);
    return q.size();
}

template<typename T>
void SafeQueue<T>::enqueue(T &t) {
    std::lock_guard<std::mutex> lock(mu);
    q.push(t);
}

template<typename T>
bool SafeQueue<T>::dequeue(T &t) {
    std::lock_guard<std::mutex> lock(mu);
    if(q.empty()){
        return false;
    }
    t = std::move(q.front());   //取出队首元素，返回队首元素值，并进行右值引用
    q.pop();
    return true;
}
#endif //FINE_GRAINED_MTS_SAFE_QUEUE_H
