#ifndef FINE_GRAINED_MTS_FIELD_H
#define FINE_GRAINED_MTS_FIELD_H

#include <string>
#include <vector>
#include "binary_log.h"
#include "my_byteorder.h"
#include "decimal.h"
#define DATETIMEF_INT_OFS 0x8000000000LL

namespace binary_log{
    class Field {

    public:
        Field(const std::string& name): name_(name){}
        virtual ~Field() {}

        const std::string& fieldName() { return name_;}
        virtual std::string valueString(Event_reader& reader) = 0;
        virtual std::string valueDefault() = 0;

    private:
        std::string name_;
    };

    class FieldInteger: public Field {
        int length_;
        bool unsigned_;
    public:
        explicit FieldInteger(const std::string &name, int length, bool is_unsigned): Field(name), length_(length), unsigned_(is_unsigned) {}

        virtual ~FieldInteger() {}

        virtual std::string valueString(Event_reader &reader);

        virtual std::string valueDefault() { return "0"; }
    };	//FieldInteger

    class FieldTimestamp : public Field {public:explicit FieldTimestamp(const std::string &name): Field(name) {}

        virtual ~FieldTimestamp() {}

        virtual std::string valueString(Event_reader &reader);

        virtual std::string valueDefault() { return "1970-01-01 08:00:00" ; }
    };

    class FieldFloat: public Field {
    public:
        explicit FieldFloat(const std::string &name): Field(name) {}

        virtual ~FieldFloat() {}

        virtual std::string valueString(Event_reader &reader);

        virtual std::string valueDefault() { return "0.0"; }
    };	//FieldFloat

    class FieldDecimal: public Field {
    public:
        int precision;
        int scale;
        explicit FieldDecimal(const std::string &name): Field(name) {}

        virtual ~FieldDecimal() {}

        virtual std::string valueString(Event_reader &reader);

        virtual std::string valueDefault() { return "0.0"; }
    };	//FieldDecimal

    class FieldDouble: public Field {
    public:
        explicit FieldDouble(const std::string &name): Field(name) {}

        virtual ~FieldDouble() {}

        virtual std::string valueString(Event_reader &reader);

        virtual std::string valueDefault() { return "0.0"; }
    };	//FieldDouble

    class FieldDatetime: public Field {
        static const int MAX_DATETIME_WIDTH = 19;      /* YYYY-MM-DD HH:MM:SS */
        //only declare
        static int type_;
    public:
        explicit FieldDatetime(const std::string &name): Field(name) {}

        virtual ~FieldDatetime() {}

        virtual std::string valueString(Event_reader &reader);

        virtual std::string valueDefault() { return "1970-01-01 08:00:00"; }
        static void setColumnMeta(int i) { FieldDatetime::type_ = i; }
    };	//FieldDatetime

    class FieldDate: public Field {
        static const int MAX_DATE_WIDTH = 10;      /* YYYY-MM-DD */
    public:
        explicit FieldDate(const std::string &name): Field(name) {}

        virtual ~FieldDate() {}

        virtual std::string valueString(Event_reader &reader);

        virtual std::string valueDefault() { return "1970-01-01"; }
    };	//FieldDate

    class FieldTime: public Field {
        static const int MAX_TIME_WIDTH = 10;      /* HH:MM:SS */
    public:
        explicit FieldTime(const std::string &name): Field(name) {}

        virtual ~FieldTime() {}

        virtual std::string valueString(Event_reader &reader);

        virtual std::string valueDefault() { return "00:00:00"; }
    };	//FieldTime

    class FieldString: public Field {
        int pack_length_;
    public:
        explicit FieldString(const std::string &name, int pack_length): Field(name), pack_length_(pack_length) {}

        virtual ~FieldString() {}

        virtual std::string valueString(Event_reader &reader);

        virtual std::string valueDefault() { return ""; }
    };
}

#endif //FINE_GRAINED_MTS_FIELD_H
