//#define BIN_LOG_HEADER_SIZE 4U
//#define LOG_EVENT_HEADER_LEN 19U    /* the fixed header length */
#define O_MEM  1
#define CRC_LEN 4
//#define MAX_MEM_COUNT 134217728
//
//
#define OFFSET_0  0
////event header
//
//#ifdef __MYSQL_SOURCE
//#define EVENT_TYPE_OFFSET    4
//#define SERVER_ID_OFFSET     5
//#define EVENT_LEN_OFFSET     9
//#define LOG_POS_OFFSET       13
//#define FLAGS_OFFSET         17
//#endif
//
#define EVENT_TIMESTAMP 4
//#define EVENT_TYPE_OFFSET 5
//#define SERVER_ID_OFFSET 9
//#define EVENT_LEN_OFFSET 13
//#define LOG_POS_OFFSET 17
//#define FLAGS_OFFSET 19
////FED
#define FORMAT_V 21
//
//
////FED event fixed data
#define FED_BINLOG_FORMAT 2
#define FED_MYSQL_FORMAT 52
#define FED_USED 56
#define FED_EVENT_HEADER 57
//
////DML event fixed data
#define DML_TABLE_ID 6
//
//
////MAP event fixed data
#define MAP_TABLE_ID 6
#define MAP_UNSUED 8
////MAP event variable data
#define MAP_DB_LENGTH 1
#define MAP_TABLE_LENGTH 1
//
////GTID event fixed data
//#define GTID_FLAGS 1
//#define GTID_SID   17
#define GTID_GNO   25
#define GTID_FLAG  26
//#define GTID_LAST  34
#define GTID_SEQ   42
////QUER event fixed data
#define QUERY_T_ID 4
#define QUERY_EXE_T 8
#define QUERY_DEFAULT_DB_LEN 9
#define QUERY_ERR_CODE 11
#define QUERY_META_BLOCK_LEN 13
//
//
////EVENT CODE
////#define  WRITE_ROWS_EVENT 30
////#define  UPDATE_ROWS_EVENT 31
////#define  DELETE_ROWS_EVENT 32
////#define  TABLE_MAP_EVENT 19
////#define  GTID_LOG_EVENT 33
//
//
//
//#define BINLOG_VERSION    4
#define BINLOG_HEADER_LENGTH 19
#define BINLOG_MAICO 1852400382

#define ERR_POST __FILE__,__LINE__

typedef struct map_tab
{
    uint64_t table_id;
    uint8_t db_name[512];
    uint8_t tab_name[512];
} MAP_TAB;

typedef struct query_event
{
    uint32_t exe_time;
    uint8_t db_name[512];
    uint8_t* statment;
} QUERY_EVENT;

typedef struct node_p
{
    struct node_p* next;
    void* data;
} NODE_P;


typedef struct head_p
{
    NODE_P* head;
    NODE_P* end;
} HEAD_P;

typedef struct op_str
{
    char db_name[512];
    char tab_name[512];
    uint64_t   ino;
    uint64_t   inoc;
    uint64_t   upo;
    uint64_t   upoc;
    uint64_t   deo;
    uint64_t   deoc;
    uint64_t   cnt;
    uint64_t   cntc;
} OP_STR;
