#include "mts.h"

int main()
{
    rpl::MTS_Handler mtsHandler;
    mtsHandler.init();
    mtsHandler.run();
}

