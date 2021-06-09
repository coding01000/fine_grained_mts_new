aux_source_directory(./src CFG_LIB_SRCS)

add_library(cfg_lib STATIC ${CFG_LIB_SRCS})

