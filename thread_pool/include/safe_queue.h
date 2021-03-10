#ifndef FINE_GRAINED_MTS_SAFE_QUEUE_H
#define FINE_GRAINED_MTS_SAFE_QUEUE_H
#include <queue>
#include <mutex>
#include <shared_mutex>
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


template <typename key, typename val>
class SafeMap{
public:
    std::mutex mu;
    std::unordered_map<key, val> _map;
    void insert(key k, val v){
        std::lock_guard<std::mutex> lockGuard(mu);
        _map[k] = v;
    }
    val get(key k){
        std::lock_guard<std::mutex> lockGuard(mu);
        return _map[k];
    }
    int find(key k){
        std::lock_guard<std::mutex> lockGuard(mu);
        if (_map.find(k)==_map.end()){
            return 0;
        }
        return 1;
    }

    void erase(key k) {
        std::lock_guard<std::mutex> lockGuard(mu);
        _map.erase(k);
    }
};
#endif //FINE_GRAINED_MTS_SAFE_QUEUE_H
