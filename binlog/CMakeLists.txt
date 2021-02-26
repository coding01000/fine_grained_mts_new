aux_source_directory(./src BINLOG_LIB_SRCS)

add_library(binlog_lib STATIC ${BINLOG_LIB_SRCS})

