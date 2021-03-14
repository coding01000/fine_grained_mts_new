#include "../include/safe_queue.h"

template<typename T>
SafeQueue<T>::~SafeQueue() {}

template<typename T>
SafeQueue<T>::SafeQueue() {}

template<typename T>
bool SafeQueue<T>::empty() {
    std::shared_lock<std::shared_mutex> lock(mu);
    return q.empty();
}

template<typename T>
int SafeQueue<T>::size() {
     std::shared_lock<std::shared_mutex> lock(mu);
    return q.size();
}

template<typename T>
void SafeQueue<T>::enqueue(T &t) {
    std::lock_guard<std::shared_mutex> lock(mu);
    q.push(t);
}

template<typename T>
bool SafeQueue<T>::dequeue(T &t) {
    std::lock_guard<std::shared_mutex> lock(mu);
    if(q.empty()){
        return false;
    }
    t = std::move(q.front());   //取出队首元素，返回队首元素值，并进行右值引用
    q.pop();
    return true;
}