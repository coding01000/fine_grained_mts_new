#include "../include/thread_pool.h"

ThreadWorker::ThreadWorker(ThreadPool *pool, const int id) : m_pool(pool), m_id(id) {}

static int ci = 0;
//重载`()`操作
void ThreadWorker::operator()() {
    std::function<void()> func;//定义基础函数类func
    bool dequeued;//是否正在取出队列中元素
    //判断线程池是否关闭，没有关闭，循环提取
    while (!m_pool->m_shutdown||m_pool->m_queue[m_id].size_approx() > 0) {
//    while (!m_pool->m_shutdown||m_pool->m_queue[m_id].size() > 0) {
//    while (!m_pool->m_shutdown||(!m_pool->m_queue.empty())) {
//        {
            //为线程环境锁加锁，互访问工作线程的休眠和唤醒
//            std::cout<<m_pool->m_queue.size()<<std::endl;
//            lock.unlock();
//            dequeued = m_pool->m_queue[m_id].try_dequeue(func);
//            std::unique_lock<std::mutex> lock(m_pool->mu[m_id]);
//            dequeued = m_pool->m_queue[m_id].dequeue(func);
//            dequeued = m_pool->m_queue.pop(func);
//            if ((!dequeued)&&(!m_pool->m_shutdown)) {
////                lock.lock();
//                m_pool->m_conditional_lock[m_id].wait(lock);
//                dequeued = m_pool->m_queue[m_id].try_dequeue(func);
//                lock.unlock();
//                dequeued = m_pool->m_queue[m_id].de(func);
//                dequeued = m_pool->m_queue.pop(func);
//            }
//            lock.unlock();
//        }
//        if (dequeued) {
            m_pool->m_queue[m_id].wait_dequeue(func);
            func();
//            (*func)();
//        }
    }
}

ThreadPool::ThreadPool(const int _n_threads): m_threads(std::vector<std::thread>(_n_threads)), m_shutdown(false), n(0),n_threads(_n_threads), m_queue(std::vector<moodycamel::BlockingConcurrentQueue<std::function<void ()>>>(_n_threads)), m_conditional_lock(std::vector<std::condition_variable>(_n_threads)), mu(std::vector<std::mutex>(_n_threads)){}

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
    for (auto &mConditionalLock : m_conditional_lock) {
        mConditionalLock.notify_one();
    }
//    m_conditional_lock.notify_all();
    for (int i = 0; i < m_threads.size(); ++i) {
        if(m_threads[i].joinable()) {
            for (auto &item : m_queue) {
                item.enqueue([](){});
            }
            m_threads[i].join();
        }
    }
}
