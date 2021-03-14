#include "replayer.h"
#include "single_group_replayer.h"
#include "multi_group_replayer.h"

int main()
{
//    cpu_set_t mask;
//    CPU_ZERO(&mask);
//    CPU_SET(0, &mask);//将cpu0绑定
//    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &mask);
//    sched_setaffinity(0, sizeof(cpu_set_t), &mask) ;
    std::cout<<pthread_self<<std::endl;

    Rpl_info rplInfo;
    rpl::Replayer *replayer;
    if (rplInfo.is_single_group){
        replayer = new rpl::SingleGroupReplayer(rplInfo);
    }else {
        replayer = new rpl::MultiGroupReplayer(rplInfo);
//    replayer = new rpl::MultiGroupReplayer(rplInfo);
    }
    replayer->init();
    replayer->run();
}

