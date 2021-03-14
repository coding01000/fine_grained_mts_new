#include "../include/thread_pool.h"

ThreadWorker::ThreadWorker(ThreadPool *pool, const int id) : m_pool(pool), m_id(id) {}

static int ci = 0;
//重载`()`操作
void ThreadWorker::operator()() {
    std::function<void()> func;//定义基础函数类func
    bool dequeued;//是否正在取出队列中元素
    //判断线程池是否关闭，没有关闭，循环提取
    while (!m_pool->m_shutdown||m_pool->m_queue.size()>0) {
//    while (!m_pool->m_shutdown||(!m_pool->m_queue.empty())) {
        {
            //为线程环境锁加锁，互访问工作线程的休眠和唤醒
//            std::cout<<m_pool->m_queue.size()<<std::endl;
            std::unique_lock<std::mutex> lock(m_pool->m_conditional_mutex);
            dequeued = m_pool->m_queue.dequeue(func);
//            dequeued = m_pool->m_queue.pop(func);
            if ((!dequeued)&&(!m_pool->m_shutdown)) {
                m_pool->m_conditional_lock.wait(lock);
                dequeued = m_pool->m_queue.dequeue(func);
//                dequeued = m_pool->m_queue.pop(func);
            }
        }
        if (dequeued) {
            func();
//            (*func)();
        }
    }
}

ThreadPool::ThreadPool(const int n_threads): m_threads(std::vector<std::thread>(n_threads)), m_shutdown(false){}

void ThreadPool::init(bool bind) {
    for (int i = 0; i < m_threads.size(); ++i) {
        m_threads[i] = std::thread(ThreadWorker(this, i));
        if (bind){
//            std::cout<<ci<<std::endl;
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(ci++, &cpuset);
            pthread_setaffinity_np(m_threads[i].native_handle(),
                                   sizeof(cpu_set_t), &cpuset);
        }
    }
}

void ThreadPool::shutdown() {
    m_shutdown = true;
    m_conditional_lock.notify_all();
    for (int i = 0; i < m_threads.size(); ++i) {
        if(m_threads[i].joinable()) {
            m_threads[i].join();
        }
    }
}
