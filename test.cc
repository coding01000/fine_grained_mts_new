#include "mts.h"
#include "master_info.h"

int main()
{
    rpl::MTS_Handler mtsHandler;
    mtsHandler.init();
    mtsHandler.run();
    return 0;
}

