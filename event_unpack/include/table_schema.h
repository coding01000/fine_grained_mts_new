#ifndef FINE_GRAINED_MTS_TABLE_SCHEMA_H
#define FINE_GRAINED_MTS_TABLE_SCHEMA_H
#include <vector>
#include <string>
#include "field.h"

namespace binary_log{
    class TableSchema {
    private:
        std::string dbname_;
        std::string tablename_;
        std::vector<Field*> columns_;
        std::string pk_;
    public:
        TableSchema(const std::string& dbname, const std::string& tablename): dbname_(dbname), tablename_(tablename){}
        virtual ~TableSchema();
        TableSchema(TableSchema const &schema);
        const std::string &getTablename() const { return tablename_; }
        const std::string &getDBname() const { return dbname_; }
        std::string& getPrikey() { return pk_; }

        bool createField(const char* name, const char* type, const char* max_octet_length);
        Field* getFieldByName(std::string& FieldName);
        Field* getFieldByIndex(int index);
        int getIdxByName(std::string& FieldName);
        size_t getFieldCount() const { return columns_.size();}
    };
}

#endif //FINE_GRAINED_MTS_TABLE_SCHEMA_H
