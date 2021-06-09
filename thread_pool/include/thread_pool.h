#ifndef FINE_GRAINED_MTS_THREAD_POOL_H
#define FINE_GRAINED_MTS_THREAD_POOL_H
#include <stdint.h>
#include "iostream"
#include "safe_queue.h"
#include "concurrentqueue.h"
#include "blockingconcurrentqueue.h"
#include <functional>
#include <thread>
#include <condition_variable>
#include <future>
#include "atomic"

class ThreadPool;
class ThreadWorker {
private:
    int m_id;
    ThreadPool * m_pool;
//    std::mutex *mu;
public:
    ThreadWorker(ThreadPool * pool, const int id);
    void operator()();
};

class ThreadPool{
public:
    bool m_shutdown; //线程池是否关闭
//    moodycamel::ConcurrentQueue<std::function<void ()>> m_queue;
//    SafeQueue<std::function<void()>> m_queue;//执行函数安全队列，即任务队列
//    boost::lockfree::queue<std::function<void()> > m_queue{10000000};//执行函数安全队列，即任务队列
    std::atomic<uint64_t> n;
    uint32_t n_threads;
    std::vector<std::thread> m_threads; //工作线程队列
    std::vector<std::mutex> mu; //工作线程队列
//    std::vector<moodycamel::BlockingConcurrentQueue<std::function<void ()>>> m_queue;
    std::vector<moodycamel::BlockingConcurrentQueue<std::function<void ()>>> m_queue;
//    std::vector<SafeQueue<std::function<void()>>> m_queue;
//    std::mutex m_conditional_mutex;//线程休眠锁互斥变量
//    std::condition_variable m_conditional_lock; //线程环境锁，让线程可以处于休眠或者唤醒状态
    std::vector<std::condition_variable> m_conditional_lock; //线程环境锁，让线程可以处于休眠或者唤醒状态
public:
    ThreadPool(const int n_threads);
    ThreadPool(const ThreadPool &) = delete; //拷贝构造函数，并且取消默认父类构造函数
    ThreadPool(ThreadPool &&) = delete; // 拷贝构造函数，允许右值引用
    ThreadPool & operator=(const ThreadPool &) = delete; // 赋值操作
    ThreadPool & operator=(ThreadPool &&) = delete; //赋值操作
    void init(bool bind = false);
    void shutdown();
    template<typename F, typename...Args>
//    auto submit(F&& f, Args&&... args) -> std::future<decltype(f(args...))>{
    void submit(F&& f, Args&&... args){
        // Create a function with bounded parameters ready to execute
//        std::bind
        std::function<decltype(f(args...))()> func = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        // Encapsulate it into a shared ptr in order to be able to copy construct / assign
        auto task_ptr = std::make_shared<std::packaged_task<decltype(f(args...))()>>(func);
        // Wrap packaged task into void function
        std::function<void()> wrapper_func = [task_ptr]() {
            (*task_ptr)();
        };
        // Enqueue generic wrapper function
//        uint64_t a = n%n_threads;
        uint64_t a = n.fetch_add(1);
        m_queue[a%n_threads].enqueue(wrapper_func);
//        m_queue.push(&wrapper_func);
        // Wake up one thread if its waiting
//        m_conditional_lock[a].notify_one();
//        n++;
//        moodycamel::BlockingConcurrentQueue<int> q;
//        q.
        // Return future from promise
//        return task_ptr->get_future();
    };
};

#endif
