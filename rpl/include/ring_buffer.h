#ifndef FINE_GRAINED_MTS_RING_BUFFER_H
#define FINE_GRAINED_MTS_RING_BUFFER_H
#include "cstdint"
#include "cstring"
#include "atomic"

template<typename T>
class RingBuffer{
private:
    bool m_empty, m_full;
    T *m_buffer;
//    std::atomic<uint64_t> m_size;
//    std::atomic<uint64_t> m_readPos;
//    std::atomic<uint64_t> m_writePos;
    uint64_t m_size;
    uint64_t m_readPos;
    uint64_t m_writePos;
public:
    RingBuffer(uint64_t size): m_size(size), m_empty(true), m_full(false), m_readPos(0), m_writePos(0), m_buffer(new T[size]){}

    ~RingBuffer(){
        if (m_buffer){
            delete m_buffer;
            m_buffer = nullptr;
        }
    }

    bool is_full(){
        return m_full;
    }

    bool is_empty(){
        return m_empty;
    }

    void clean(){
        m_readPos = m_writePos = 0;
        m_full = false;
        m_empty = true;
    }

    void init(){
        m_readPos = 0;
    }


    uint64_t capacity(){
        if (m_full){
            return 0;
        }
        if (m_empty){
            return m_size;
        }
        if (m_readPos <= m_writePos){
            return m_size - m_writePos + m_readPos;
        }
        return m_readPos - m_writePos;
    }

    uint64_t size(){
        return m_size - capacity();
    }

    int write(T *buf, uint64_t len){
        if (len < 0){
            return 0;
        }
        if (capacity() < len){
            return 0;
        }
        m_empty = false;
        if (m_readPos <= m_writePos){
            uint64_t right_length = m_size - m_writePos;
            if (right_length > len){
                memcpy(m_buffer + m_writePos, buf, len);
                m_writePos += len;
                return len;
            }else {
                memcpy(m_buffer + m_writePos, buf, right_length);
                m_writePos = len - right_length;
                memcpy(m_buffer, buf + right_length, m_writePos);
                m_full = (m_writePos == m_readPos);
                return right_length + m_writePos;
            }
        }
        if (m_readPos > m_writePos){
            memcpy(m_buffer + m_writePos, buf, len);
            m_writePos += len;
            m_full = (m_writePos == m_readPos);
            return len;
        }

    }

    int read(T *&buf, int len){
//        if (len < 0){
//            return 0;
//        }
//        if (m_empty){
//            return 0;
//        }
//        if (size() < len){
//            return 0;
//        }
        m_full = false;
        buf = m_buffer+m_readPos;
        m_readPos += len;
        return len;
//        if (m_readPos >= m_writePos){
//            uint64_t right_length = m_size - m_readPos;
//            if (right_length > len){
//                memcpy(buf, m_buffer + m_readPos, len);
//                m_readPos += len;
//                return len;
//            }else {
//                memcpy(buf, m_buffer + m_readPos, right_length);
//                m_readPos = len - right_length;
//                memcpy(buf + right_length, m_buffer, m_readPos);
//                m_empty = (m_readPos == m_writePos);
//                return len;
//            }
//        }
//        if (m_readPos < m_writePos){
//            memcpy(buf, m_buffer + m_readPos, len);
//            m_readPos += len;
//            m_empty = (m_readPos == m_writePos);
//            return len;
//        }
    }

    uint8_t *read(uint64_t offset, int len){
        return m_buffer+m_readPos+offset;
//        uint8_t *tmp = new uint8_t[len];
//        uint64_t now_readPos = m_readPos;
//        bool now_full = m_full;
//        bool now_empty = m_empty;
//        m_readPos = m_readPos+offset>m_size ? m_readPos+offset-m_size : m_readPos + offset;
//        read(tmp, len);
//        m_readPos = now_readPos;
//        m_empty = now_empty;
//        m_full = now_full;
//        return tmp;
    }
};

#endif //FINE_GRAINED_MTS_RING_BUFFER_H
