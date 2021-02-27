aux_source_directory(./src EV_UNPACK_LIB_SRCS)

add_library(event_unpack_lib STATIC ${EV_UNPACK_LIB_SRCS})

