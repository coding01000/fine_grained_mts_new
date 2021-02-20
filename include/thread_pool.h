#ifndef FINE_GRAINED_MTS_THREAD_POOL_H
#define FINE_GRAINED_MTS_THREAD_POOL_H
#include <stdint.h>
//#include "safe_queue.h"
#include "../utils/safe_queue.cc"
#include <functional>
#include <thread>
#include <condition_variable>
#include <future>

class ThreadPool;
class ThreadWorker {
private:
    int m_id;
    ThreadPool * m_pool;
public:
    ThreadWorker(ThreadPool * pool, const int id);
    void operator()();
};

class ThreadPool{
public:
    bool m_shutdown; //线程池是否关闭
    SafeQueue<std::function<void()>> m_queue;//执行函数安全队列，即任务队列
    std::vector<std::thread> m_threads; //工作线程队列
    std::mutex m_conditional_mutex;//线程休眠锁互斥变量
    std::condition_variable m_conditional_lock; //线程环境锁，让线程可以处于休眠或者唤醒状态
public:
    ThreadPool(const int n_threads);
    ThreadPool(const ThreadPool &) = delete; //拷贝构造函数，并且取消默认父类构造函数
    ThreadPool(ThreadPool &&) = delete; // 拷贝构造函数，允许右值引用
    ThreadPool & operator=(const ThreadPool &) = delete; // 赋值操作
    ThreadPool & operator=(ThreadPool &&) = delete; //赋值操作
    void init();
    void shutdown();
    template<typename F, typename...Args>
    auto submit(F&& f, Args&&... args) -> std::future<decltype(f(args...))>{
        // Create a function with bounded parameters ready to execute
        std::function<decltype(f(args...))()> func = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        // Encapsulate it into a shared ptr in order to be able to copy construct / assign
        auto task_ptr = std::make_shared<std::packaged_task<decltype(f(args...))()>>(func);
        // Wrap packaged task into void function
        std::function<void()> wrapper_func = [task_ptr]() {
            (*task_ptr)();
        };
        // Enqueue generic wrapper function
        m_queue.enqueue(wrapper_func);
        // Wake up one thread if its waiting
        m_conditional_lock.notify_one();
        // Return future from promise
        return task_ptr->get_future();
    };
};

#endif
