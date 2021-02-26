aux_source_directory(./src BINLOG_LIB_SRCS)
aux_source_directory(./include BINLOG_I_LIB_SRCS)

add_library(binlog_lib ${BINLOG_LIB_SRCS} ${BINLOG_I_LIB_SRCS})