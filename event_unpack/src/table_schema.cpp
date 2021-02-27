#include "../include/table_schema.h"

namespace binary_log{

    TableSchema::~TableSchema() {
        for(size_t i = 0; i<columns_.size();++i) {
            delete columns_[i];
        }
        pk_.clear();
    }

//TableSchema
    bool TableSchema::createField(const char *name, const char *type, const char *max_octet_length) {
        Field *field = NULL;

        if (strstr(type, "int") != NULL) {
            bool is_unsigned = (strstr(type, "unsigned") == NULL ? false : true);

            if (strstr(type, "tinyint") == type) {
                field = new FieldInteger(name, 1, is_unsigned);

            } else if (strstr(type, "smallint") == type) {
                field = new FieldInteger(name, 2, is_unsigned);

            } else if (strstr(type, "mediumint") == type) {
                field = new FieldInteger(name, 3, is_unsigned);

            } else if (strstr(type, "int") == type) {
                field = new FieldInteger(name, 4, is_unsigned);

            } else if (strstr(type, "bigint") == type) {
                field = new FieldInteger(name, 8, is_unsigned);
            } else {
                assert(false);
            }
        } else if (strstr(type, "varchar") == type || strstr(type, "char") == type) {
            int pack_length;
            atoi(max_octet_length) < 256 ? pack_length = 1 : pack_length = 2;
            field = new FieldString(name, pack_length);

        } else if (strstr(type, "varbinary") == type || strstr(type, "binary") == type) {
            int pack_length;
            atoi(max_octet_length) < 256 ? pack_length = 1 : pack_length = 2;
            field = new FieldString(name, pack_length);

        } else if (strstr(type, "text") != NULL) {
            int pack_length = 0;
            if (strcmp(type, "tinytext") == 0) {
                pack_length = 1;
            } else if (strcmp(type, "text") == 0) {
                pack_length = 2;
            } else if (strcmp(type, "mediumtext") == 0) {
                pack_length = 3;
            } else if (strcmp(type, "longtext") == 0) {
                pack_length = 4;
            } else {
                assert(false);
            }
            field = new FieldString(name, pack_length);

        } else if (strstr(type, "blob") != NULL) {
            int pack_length = 0;
            if (strcmp(type, "tinyblob") == 0) {
                pack_length = 1;
            } else if (strcmp(type, "blob") == 0) {
                pack_length = 2;
            } else if (strcmp(type, "mediumblob") == 0) {
                pack_length = 3;
            } else if (strcmp(type, "longblob") == 0) {
                pack_length = 4;
            } else {
                assert(false);
            }
            field = new FieldString(name, pack_length);

        } else if (strcmp(type, "time") == 0) {
            field = new FieldTime(name);

        } else if (strcmp(type, "date") == 0) {
            field = new FieldDate(name);

            //depending on mysql version
        } else if (strcmp(type, "datetime") == 0) {
            field = new FieldDatetime(name);

        } else if (strncmp(type, "float",5) == 0) {
            field = new FieldFloat(name);

        } else if (strcmp(type, "double") == 0) {
            field = new FieldDouble(name);
        } else if (strcmp(type, "timestamp") == 0 ) {
            field = new FieldTimestamp(name) ;
        }

        if (field == NULL) {
            std::cout<<"column "<<name<<" type not support: "<<type;
            return false;
        }

        columns_.push_back(field);
        return true;
    }

    TableSchema::TableSchema(const TableSchema &schema) {
        pk_ = schema.pk_;
        dbname_ = schema.dbname_;
        tablename_ = schema.tablename_;
        columns_ = schema.columns_;
    }

    Field* TableSchema::getFieldByName(std::string& FieldName) {
        for(size_t i = 0; i<columns_.size();i++) {
            if(columns_[i]->fieldName() == FieldName) {
                return columns_[i];
            }
        }
        return NULL;
    }

    Field* TableSchema::getFieldByIndex(int index) {
        if(index < 0 || index >= (int)columns_.size()) {
            return NULL;
        }
        return columns_[index];
    }
}

