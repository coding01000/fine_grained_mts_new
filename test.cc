#include "replayer.h"
#include "single_group_replayer.h"
#include "multi_group_replayer.h"
#include "coordinator.h"
#include <fstream>
#include <dirent.h>
//#include "gperftools/"

int main()
{
//    auto a = new std::vector<int>(2);
//    a->push_back(1);
//    a.emplace_back(1);
//    std::cout<<a->size()<<std::endl;

//    Rpl_info rplInfo;
//    rpl::MultiGroupReplayer a(rplInfo);
//    a.delay();
    Rpl_info rplInfo;
    if (rplInfo.is_mysql_mode){
        mysql_mts::Coordinator coordinator;
        coordinator.init(rplInfo);
        coordinator.run();
    }else {
        rpl::Replayer *replayer;
        if (rplInfo.is_single_group){
//        replayer = new rpl::SingleGrozpReplayer(rplInfo);
        }else {
            replayer = new rpl::MultiGroupReplayer(rplInfo);
        }
        std::cout<<"------------------------------------------------------"<<std::endl;
        replayer->init();
        replayer->run();
    };





//    replayer->get();
//    moodycamel::ConcurrentQueue<int> q;
//    q.enqueue(1);
//    q.enqueue(2);
//    std::cout<<q.size_approx()<<std::endl;
//    int a, b;
//    bool f1 = q.try_dequeue(a);
//    bool f2 = q.try_dequeue(b);
//
//    std::cout<<q.size_approx()<<std::endl;
//    std::cout<<f1<<" "<<f2<<std::endl;
//    std::cout<<a<<" "<<b<<std::endl;

}

