#ifndef FINE_GRAINED_MTS_SAFE_MAP_H
#define FINE_GRAINED_MTS_SAFE_MAP_H

#include "unordered_map"
#include "mutex"

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
//        std::lock_guard<std::mutex> lockGuard(mu);
        std::lock_guard<std::mutex> lockGuard(mu);
        return _map[k];
    }
    auto find(key k){
        std::lock_guard<std::mutex> lockGuard(mu);
        return _map.find(k);
    }

    bool is_(key k){
        std::lock_guard<std::mutex> lockGuard(mu);
        return _map.find(k)==_map.end();
    }

//    auto end(){
//        return _map.end();
//    }
//
//    auto begin(){
//        return _map.begin();
//    }

    void erase(key k) {
        std::lock_guard<std::mutex> lockGuard(mu);
        _map.erase(k);
    }
};

#endif //FINE_GRAINED_MTS_SAFE_MAP_H
