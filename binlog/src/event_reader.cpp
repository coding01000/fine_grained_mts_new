/* Copyright (c) 2018, 2020 Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "event_reader.h"
#include "my_byteorder.h"
#include <string>

namespace binary_log {

    uint64_t net_field_length_ll1(uchar **packet) {
        const uchar *pos = *packet;
        if (*pos < 251) {
            (*packet)++;
            return (uint64_t)*pos;
        }
        if (*pos == 251) {
            (*packet)++;
            return (uint64_t)NULL_LENGTH;
        }
        if (*pos == 252) {
            (*packet) += 3;
            return (uint64_t)uint2korr(pos + 1);
        }
        if (*pos == 253) {
            (*packet) += 4;
            return (uint64_t)uint3korr(pos + 1);
        }
        (*packet) += 9; /* Must be 254 when here */
        return (uint64_t)uint8korr(pos + 1);
    }

    uint8_t net_field_length_size1(const uchar *pos) {
        if (*pos <= 251) return 1;
        if (*pos == 252) return 3;
        if (*pos == 253) return 4;
        return 9;
    }

    void Event_reader::set_error(const char *error) {
        m_error = error;
    }

    void Event_reader::set_length(unsigned long long length) {
        if (length < m_length) {
            set_error("Buffer length cannot shrink");
        } else {
            m_limit = m_length = length;
        }
    }

    void Event_reader::shrink_limit(unsigned long long bytes) {
        if (bytes > m_limit || position() > m_limit - bytes) {
            set_error("Unable to shrink buffer limit");
        } else
            m_limit = m_limit - bytes;
    }

    const char *Event_reader::ptr(unsigned long long length) {
        if (!can_read(length)) {
            set_error("Cannot point to out of buffer bounds");
            return nullptr;
        }
        const char *ret_ptr = m_ptr;
        m_ptr = m_ptr + length;
        return ret_ptr;
    }

    const char *Event_reader::go_to(unsigned long long position) {
        if (position >= m_limit) {
            set_error("Cannot point to out of buffer bounds");
            return nullptr;
        }
        m_ptr = m_buffer + position;
        return m_ptr;
    }

    void Event_reader::alloc_and_memcpy(unsigned char **destination, size_t length,
                                        int flags) {
        if (!can_read(length)) {
            set_error("Cannot read from out of buffer bounds");
            return;
        }
        *destination = static_cast<unsigned char *>(malloc(length));
        if (!*destination) {
            set_error("Out of memory");
            return;
        }
        ::memcpy(*destination, m_ptr, length);
        m_ptr = m_ptr + length;
    }

    void Event_reader::alloc_and_strncpy(char **destination, size_t length,
                                         int flags) {
        if (!can_read(length)) {
            set_error("Cannot read from out of buffer bounds");
            return;
        }
        *destination = static_cast<char *>(malloc(length + 1));
        if (!*destination) {
            set_error("Out of memory");
            return;
        }
        strncpy(*destination, m_ptr, length);
        (*destination)[length] = '\0';
        m_ptr = m_ptr + length;
    }

    void Event_reader::read_str_at_most_255_bytes(const char **destination,
                                                  uint8_t *lenght) {
        if (!can_read(sizeof(uint8_t))) {
            set_error("Cannot read from out of buffer bounds");
            return;
        }
        ::memcpy(lenght, m_ptr, sizeof(uint8_t));
        m_ptr = m_ptr + sizeof(uint8_t);

        if (!can_read(*lenght)) {
            set_error("Cannot read from out of buffer bounds");
            return;
        }
        *destination = m_ptr;
        m_ptr = m_ptr + *lenght;
    }

    uint64_t Event_reader::net_field_length_ll() {
        if (!can_read(sizeof(uint8_t))) {
            set_error("Cannot read from out of buffer bounds");
            return 0;
        }
        // It is safe to read the first byte of the transaction_length
        unsigned char *ptr_length;
        ptr_length = reinterpret_cast<unsigned char *>(const_cast<char *>(m_ptr));
        unsigned int length_size = net_field_length_size1(ptr_length);
//        unsigned int length_size = net_field_length_size1(ptr_length);
        if (!can_read(length_size)) {
            set_error("Cannot read from out of buffer bounds");
            return 0;
        }
        // It is safe to read the full transaction_length from the buffer
        uint64_t value = net_field_length_ll1(&ptr_length);
        m_ptr = m_ptr + length_size;
        return value;
    }

    void Event_reader::read_data_set(uint32_t set_len,
                                     std::list<const char *> *set) {
        uint16_t len = 0;
        for (uint32_t i = 0; i < set_len; i++) {
            len = read_and_letoh<uint16_t>();
            if (m_error) break;
            const char *hash = strndup<const char *>(len);
            if (m_error) break;
            set->push_back(hash);
        }
    }

    void Event_reader::read_data_map(uint32_t map_len,
                                     std::map<std::string, std::string> *map) {
        for (uint32_t i = 0; i < map_len; i++) {
            uint16_t key_len = read_and_letoh<uint16_t>();
            if (m_error) break;
            if (!can_read(key_len)) {
                set_error("Cannot read from out of buffer bounds");
                break;
            }
            std::string key(m_ptr, key_len);
            m_ptr += key_len;

            uint32_t value_len = read_and_letoh<uint32_t>();
            if (m_error) break;
            if (!can_read(value_len)) {
                set_error("Cannot read from out of buffer bounds");
                break;
            }
            std::string value(m_ptr, value_len);
            m_ptr += value_len;

            (*map)[key] = value;
        }
    }

    void Event_reader::strncpyz(char *destination, size_t max_length,
                                size_t dest_length) {
        if (!can_read(max_length)) {
            set_error("Cannot read from out of buffer bounds");
            return;
        }
        strncpy(destination, m_ptr, max_length);
        destination[dest_length - 1] = 0;
        m_ptr = m_ptr + strlen(destination) + 1;
    }

    void Event_reader::assign(std::vector<uint8_t> *vector, size_t length) {
        if (!can_read(length)) {
            set_error("Cannot read from out of buffer bounds");
            return;
        }
        try {
            vector->assign(m_ptr, m_ptr + length);
        } catch (const std::bad_alloc &) {
            vector->clear();
            set_error("std::bad_alloc");
        }
        m_ptr = m_ptr + length;
    }
}  // end namespace binary_log
