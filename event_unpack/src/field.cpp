#include "../include/field.h"
//#include "mysql/my"

namespace binary_log{

    std::string FieldInteger::valueString(Event_reader &reader) {
        char buf[32];
        int len;
        union {
            int64_t i;
            uint64_t ui;
        } value;

        if (length_ == 1) {
            unsigned_ ? value.ui = reader.read<uint8_t>() : value.i = reader.read<int8_t>();
        } else if (length_ == 2) {
            unsigned_ ? value.ui = reader.read<uint16_t>() : value.i = reader.read<int16_t>();
        } else if (length_ == 3) {
            const char *ptr = reader.ptr(3);
            unsigned_ ? value.ui = uint3korr(ptr) : value.i = sint3korr(ptr);
        } else if (length_ == 4) {
            const char *ptr = reader.ptr(4);
            unsigned_ ? value.ui = uint4korr(ptr) : value.i = (int32_t) uint4korr(ptr);
//            unsigned_ ? value.ui = reader.read<uint32_t>() : value.i = reader.read<uint32_t>();
        } else if (length_ == 8) {
            const char *ptr = reader.ptr(8);
            unsigned_ ? value.ui = uint8korr(ptr) : value.i = (int64_t)uint8korr(ptr);
        }

        if (length_ == 8 && unsigned_) {
#if __WORDSIZE == 64
            len = snprintf(buf, sizeof(buf), "%lu", value.ui);
#else
            len = snprintf(buf, sizeof(buf), "%llu", value.ui);
#endif
        } else {
#if __WORDSIZE == 64
            len = snprintf(buf, sizeof(buf), "%ld", value.i);
#else
            len = snprintf(buf, sizeof(buf), "%lld", value.i);
#endif
        }
        return std::string(buf, len);
    }

//FieldString
    std::string FieldString::valueString(Event_reader &reader) {
        size_t len = 0;
        std::string value;

        if (pack_length_ == 1) {
            len = reader.read<uint8_t>();
        } else if (pack_length_ == 2) {
            len = (size_t) reader.read<uint16>();
        } else if (pack_length_ == 3) {
            const char* ptr = reader.ptr(3);
            len = uint3korr(ptr);
        } else if (pack_length_ == 4) {
            const char* ptr = reader.ptr(4);
            len = uint4korr(ptr);
        }
        value.assign(reader.ptr(len), len);
        return value;
    }


//FieldTimestamp
    std::string FieldTimestamp::valueString(Event_reader &reader) {
        time_t i = reader.read<int32>();
        struct tm* t;
        struct tm tr;
        t = localtime_r(&i, &tr);

        char buf[32];
        strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", t);
        return buf ;
    }

// base on mysql5.6.25 sql/log_event.cc::log_event_print_value
    std::string FieldDatetime::valueString(Event_reader &reader) {
        char buf[MAX_DATETIME_WIDTH + 1];
        int64 value = mi_uint5korr(reader.ptr(5)) - DATETIMEF_INT_OFS;
        //LOG(INFO)<<"datetime value:"<<value<<" ,fieldTypeDecimal:"<<fieldTypeTiny;
        if (value == 0) {
            return valueDefault();
        }
        if (value <0) {
            value = -value;
        }
        longlong ymd = value >> 17;
        longlong ym = ymd >> 5;
        longlong hms = value % (1<<17);
        snprintf(buf, sizeof(buf), "%04lld-%02lld-%02lld %02lld:%02lld:%02lld", ym / 13, ym % 13, ymd % (1 << 5), (hms >> 12), (hms >> 6) % (1 << 6), hms % (1 << 6));
        int frac = 0;
        switch (type_) {
            case 0:
            default:
                break;
            case 1:
            case 2:
                frac = reader.read<uint8_t>() * 10000;
                break;
            case 3:
            case 4:
                frac = mi_sint2korr(reader.ptr(2)) * 100;
                break;
            case 5:
            case 6:
                frac = mi_sint3korr(reader.ptr(3));
                break;
        }
        //LOG(INFO)<<"type_:"<<type_<<" ,frac"<<frac;
        return std::string(buf);
    }

//定义并初始化
    int FieldDatetime::type_ = 0;

/*
//FieldDatetime
std::string FieldDatetime::valueString(const Event_reader &b) {
	char buf[MAX_DATETIME_WIDTH + 1];
	long part1,part2;
	char *pos;
	int part3;

	//uint64_t value = reader.getFixed64();
	uint64_t value = reader.getFixed40();
    LOG(INFO)<<"datetime value:"<<value;
	if (value == 0) {
		return valueDefault();
	}

	//Avoid problem with slow longlong arithmetic and sprintf
	part1=(long) (value / 1000000ULL);
	//part2=(long) (value - part1*1000000ULL);
	part2=(long) (value % 1000000ULL);

	pos=(char*) buf + MAX_DATETIME_WIDTH;
	*pos--=0;
	*pos--= (char) ('0'+(char) (part2%10)); part2/=10;
	*pos--= (char) ('0'+(char) (part2%10)); part3= (int) (part2 / 10);
	*pos--= ':';
	*pos--= (char) ('0'+(char) (part3%10)); part3/=10;
	*pos--= (char) ('0'+(char) (part3%10)); part3/=10;
	*pos--= ':';
	*pos--= (char) ('0'+(char) (part3%10)); part3/=10;
	*pos--= (char) ('0'+(char) part3);
	*pos--= ' ';
	*pos--= (char) ('0'+(char) (part1%10)); part1/=10;
	*pos--= (char) ('0'+(char) (part1%10)); part1/=10;
	*pos--= '-';
	*pos--= (char) ('0'+(char) (part1%10)); part1/=10;
	*pos--= (char) ('0'+(char) (part1%10)); part3= (int) (part1/10);
	*pos--= '-';
	*pos--= (char) ('0'+(char) (part3%10)); part3/=10;
	*pos--= (char) ('0'+(char) (part3%10)); part3/=10;
	*pos--= (char) ('0'+(char) (part3%10)); part3/=10;
	*pos=(char) ('0'+(char) part3);
	return std::string(buf);
}*/

//FieldDate
    std::string FieldDate::valueString(Event_reader &reader) {
        char buf[MAX_DATE_WIDTH + 1];
        uint32_t tmp = uint3korr(reader.ptr(3));
        if (tmp == 0) {
            return valueDefault();
        }

        int part;
        char *pos= buf + MAX_DATE_WIDTH;

        /* Open coded to get more speed */
        *pos--=0;                                     // End NULL
        part=(int) (tmp & 31);
        *pos--= (char) ('0'+part%10);
        *pos--= (char) ('0'+part/10);
        *pos--= '-';
        part=(int) (tmp >> 5 & 15);
        *pos--= (char) ('0'+part%10);
        *pos--= (char) ('0'+part/10);
        *pos--= '-';
        part=(int) (tmp >> 9);
        *pos--= (char) ('0'+part%10); part/=10;
        *pos--= (char) ('0'+part%10); part/=10;
        *pos--= (char) ('0'+part%10); part/=10;
        *pos=   (char) ('0'+part);
        return std::string(buf);
    }

    std::string FieldTime::valueString(Event_reader &reader) {
        char buf[MAX_TIME_WIDTH + 1];
        uint32_t tmp = uint3korr(reader.ptr(3));

        uint32_t hour= (uint) (tmp/10000);
        uint32_t minute= (uint) (tmp/100 % 100);
        uint32_t second= (uint) (tmp % 100);

        snprintf(buf, sizeof(buf), "%u:%u:%u", hour, minute, second);
        return std::string(buf);
    }

//FieldFloat
    std::string FieldFloat::valueString(Event_reader &reader) {
        char buf[32];

        float f;
        const char *ptr = reader.ptr(4);
        f=float4get(reinterpret_cast<const unsigned char*>(ptr));
        snprintf(buf, sizeof(buf), "%f", f);
        return std::string(buf);
    }

//FieldDouble
    std::string FieldDouble::valueString(Event_reader &reader) {
        char buf[64];

        double d;
        const char *ptr = reader.ptr(8);
        d=doubleget(reinterpret_cast<const unsigned char*>(ptr));
        snprintf(buf, sizeof(buf), "%f", d);
        return std::string(buf);
    }
}
