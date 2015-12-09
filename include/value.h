/*
Copyright (c) 2003, 2011, Oracle and/or its affiliates. All rights
reserved.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; version 2 of
the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
02110-1301  USA
*/

#ifndef _VALUE_ADAPTER_H
#define	_VALUE_ADAPTER_H

#include <boost/cstdint.hpp>
#include "protocol.h"
#include <boost/any.hpp>
#include <iostream>

using namespace mysql;
namespace mysql {
#define uint2korr(A)  (boost::uint16_t) (((boost::uint16_t) ((boost::uint8_t) (A)[0])) +\
                ((boost::uint16_t) ((boost::uint8_t) (A)[1]) << 8))
  
#define uint3korr(A)  (boost::uint32_t) (((boost::uint32_t) ((boost::uint8_t) (A)[0])) +\
                (((boost::uint32_t) ((boost::uint8_t) (A)[1])) << 8) +\
                (((boost::uint32_t) ((boost::uint8_t) (A)[2])) << 16))
#define sint3korr(A)  ((boost::int32_t) ((((boost::uint8_t) (A)[2]) & 128) ? \
                  (((boost::uint32_t) 255L << 24) | \
                  (((boost::uint32_t) (boost::uint8_t) (A)[2]) << 16) |\
                  (((boost::uint32_t) (boost::uint8_t) (A)[1]) << 8) | \
                  ((boost::uint32_t) (boost::uint8_t) (A)[0])) : \
                  (((boost::uint32_t) (boost::uint8_t) (A)[2]) << 16) |\
                  (((boost::uint32_t) (boost::uint8_t) (A)[1]) << 8) | \
                  ((boost::uint32_t) (boost::uint8_t) (A)[0])))

#define mi_sint1korr(A) ((boost::int8_t)(*A))
#define mi_sint2korr(A) ((boost::int16_t) (((boost::int16_t) (((boost::uint8_t*) (A))[1])) +\
            ((boost::int16_t) ((boost::int16_t) ((boost::int8_t*) (A))[0]) << 8)))
#define mi_sint3korr(A) ((boost::int32_t) (((((boost::uint8_t*) (A))[0]) & 128) ? \
            (((boost::uint32_t) 255L << 24) | \
            (((boost::uint32_t) ((boost::uint8_t*) (A))[0]) << 16) |\
            (((boost::uint32_t) ((boost::uint8_t*) (A))[1]) << 8) | \
            ((boost::uint32_t) ((boost::uint8_t*) (A))[2])) : \
            (((boost::uint32_t) ((boost::uint8_t*) (A))[0]) << 16) |\
            (((boost::uint32_t) ((boost::uint8_t*) (A))[1]) << 8) | \
            ((boost::uint32_t) ((boost::uint8_t*) (A))[2])))
#define mi_sint4korr(A) ((boost::int32_t) (((boost::int32_t) (((boost::uint8_t*) (A))[3])) +\
            ((boost::int32_t) (((boost::uint8_t*) (A))[2]) << 8) +\
            ((boost::int32_t) (((boost::uint8_t*) (A))[1]) << 16) +\
            ((boost::int32_t) ((boost::int16_t) ((boost::int8_t*) (A))[0]) << 24)))

#define mi_uint1korr(A) ((boost::uint8_t)(*A))
#define mi_uint2korr(A) ((boost::uint16_t) (((boost::uint16_t) (((boost::uint8_t*) (A))[1])) +\
            ((boost::uint16_t) (((boost::uint8_t*) (A))[0]) << 8)))
#define mi_uint3korr(A) ((boost::uint32_t) (((boost::uint32_t) (((boost::uint8_t*) (A))[2])) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[1])) << 8) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[0])) << 16)))
#define mi_uint4korr(A) ((boost::uint32_t) (((boost::uint32_t) (((boost::uint8_t*) (A))[3])) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[2])) << 8) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[1])) << 16) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[0])) << 24)))
#define mi_uint5korr(A) ((boost::uint64_t)(((boost::uint32_t) (((boost::uint8_t*) (A))[4])) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[3])) << 8) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[2])) << 16) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[1])) << 24)) +\
            (((boost::uint64_t) (((boost::uint8_t*) (A))[0])) << 32))
#define mi_uint6korr(A) ((boost::uint64_t)(((boost::uint32_t) (((boost::uint8_t*) (A))[5])) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[4])) << 8) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[3])) << 16) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[2])) << 24)) +\
            (((boost::uint64_t) (((boost::uint32_t) (((boost::uint8_t*) (A))[1])) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[0]) << 8)))) <<\
            32))
#define mi_uint7korr(A) ((boost::uint64_t)(((boost::uint32_t) (((boost::uint8_t*) (A))[6])) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[5])) << 8) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[4])) << 16) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[3])) << 24)) +\
            (((boost::uint64_t) (((boost::uint32_t) (((boost::uint8_t*) (A))[2])) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[1])) << 8) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[0])) << 16))) <<\
            32))
#define mi_uint8korr(A) ((boost::uint64_t)(((boost::uint32_t) (((boost::uint8_t*) (A))[7])) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[6])) << 8) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[5])) << 16) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[4])) << 24)) +\
            (((boost::uint64_t) (((boost::uint32_t) (((boost::uint8_t*) (A))[3])) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[2])) << 8) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[1])) << 16) +\
            (((boost::uint32_t) (((boost::uint8_t*) (A))[0])) << 24))) <<\
            32))

#define DATETIMEF_INT_OFS 0x8000000000LL
#define TIMEF_OFS 0x800000000000LL
#define TIMEF_INT_OFS 0x800000LL

typedef struct st_decimal_t {
  int     intg, frac, len;
  int8_t  sign;
  int32_t *buf;
} decimal_t;

#if !defined(__GNUC__) || (__GNUC__ == 2 && __GNUC_MINOR__ < 96)
#define __builtin_expect(x, expected_value) (x)
#endif

#define likely(x) __builtin_expect((x),1)
#define unlikely(x) __builtin_expect((x),0)
#ifdef HAVE_purify
#define sanity(d) assert((d)->len > 0)
#else
#define sanity(d) assert((d)->len >0 && ((d)->buf[0] | \
                                    (d)->buf[(d)->len-1] | 1))
#endif
#define MY_TEST(a)    ((a) ? 1 : 0)
/* Define some useful general macros (should be done after all headers). */
#define MY_MAX(a, b)  ((a) > (b) ? (a) : (b))
#define MY_MIN(a, b)  ((a) < (b) ? (a) : (b))
#define decimal_make_zero(dec)        do {                \
                                            (dec)->buf[0]=0;    \
                                            (dec)->intg=1;      \
                                            (dec)->frac=0;      \
                                            (dec)->sign=0;      \
                                          } while(0)
/** maximum length of buffer in our big digits (uint32). */
#define DECIMAL_BUFF_LENGTH 9

/* the number of digits that my_decimal can possibly contain */
#define DECIMAL_MAX_POSSIBLE_PRECISION (DECIMAL_BUFF_LENGTH * 9)

/**
 *  maximum guaranteed precision of number in decimal digits (number of our
 *  digits * number of decimal digits in one our big digit - number of decimal
 *  digits in one our big digit decreased by 1 (because we always put decimal
 *  point on the border of our big digits))
*/
#define DECIMAL_MAX_PRECISION (DECIMAL_MAX_POSSIBLE_PRECISION - 8*2)
#define DECIMAL_MAX_SCALE 30
#define DECIMAL_NOT_SPECIFIED 31

/**
 * maximum length of string representation (number of maximum decimal
 * digits + 1 position for sign + 1 position for decimal point, no terminator)
*/
#define DECIMAL_MAX_STR_LENGTH (DECIMAL_MAX_POSSIBLE_PRECISION + 2)

/**
 *   maximum size of packet length.
*/
#define DECIMAL_MAX_FIELD_SIZE DECIMAL_MAX_PRECISION

#if defined(_lint) || defined(FORCE_INIT_OF_VARS) || \
      defined(__cplusplus) || !defined(__GNUC__)
#define UNINIT_VAR(x) x= 0
#else
/* GCC specific self-initialization which inhibits the warning. */
#define UNINIT_VAR(x) x= x
#endif

#define FIX_INTG_FRAC_ERROR(len, intg1, frac1, error)               \
    do                                                              \
    {                                                               \
      if (unlikely(intg1+frac1 > (len)))                            \
      {                                                             \
        if (unlikely(intg1 > (len)))                                \
        {                                                           \
          intg1=(len);                                              \
          frac1=0;                                                  \
          error=E_DEC_OVERFLOW;                                     \
        }                                                           \
        else                                                        \
        {                                                           \
          frac1=(len)-intg1;                                        \
          error=E_DEC_TRUNCATED;                                    \
        }                                                           \
      }                                                             \
      else                                                          \
        error=E_DEC_OK;                                             \
    } while(0)

class my_decimal :public decimal_t
{
  int32_t buffer[DECIMAL_BUFF_LENGTH];
  public:
    my_decimal() {
      len= DECIMAL_BUFF_LENGTH;
      buf= buffer;
    }
};


/**
 This helper function calculates the size in bytes of a particular field in a
 row type event as defined by the field_ptr and metadata_ptr arguments.
 @param column_type Field type code
 @param field_ptr The field data
 @param metadata_ptr The field metadata

 @note We need the actual field data because the string field size is not
 part of the meta data. :(

 @return The size in bytes of a particular field
*/
int calc_field_size(unsigned char column_type, const unsigned char *field_ptr,
                    boost::uint32_t metadata);


/**
 * A value object class which encapsluate a tuple (value type, metadata, storage)
 * and provide for views to this storage through a well defined interface.
 *
 * Can be used with a Converter to convert between different Values.
 */
class Value
{
public:
    Value(enum system::enum_field_types type, boost::uint32_t metadata, const char *storage) :
      m_type(type), m_storage(storage), m_metadata(metadata), m_is_null(false)
    {
      m_size= calc_field_size((unsigned char)type,
                              (const unsigned char*)storage,
                              metadata);
      //std::cout << "TYPE: " << type << " SIZE: " << m_size << std::endl;
    };

    Value()
    {
      m_size= 0;
      m_storage= 0;
      m_metadata= 0;
      m_is_null= false;
    }

    /**
     * Copy constructor
     */
    Value(const Value& val);

    Value &operator=(const Value &val);
    bool operator==(const Value &val) const;
    bool operator!=(const Value &val) const;

    ~Value() {}

    void is_null(bool s) { m_is_null= s; }
    bool is_null(void) const { return m_is_null; }

    const char *storage() const { return m_storage; }

    /**
     * Get the length in bytes of the entire storage (any metadata part +
     * atual data)
     */
    size_t length() const { return m_size; }
    enum system::enum_field_types type() const { return m_type; }
    boost::uint32_t metadata() const { return m_metadata; }

    /**
     * Returns the integer representation of a storage of a pre-specified
     * type.
     */
    boost::int32_t as_int32() const;

    /**
     * Returns the integer representation of a storage of pre-specified
     * type.
     */
    boost::int64_t as_int64() const;

    /**
     * Returns the integer representation of a storage of pre-specified
     * type.
     */
    boost::int8_t as_int8() const;

    /**
     * Returns the integer representation of a storage of pre-specified
     * type.
     */
    boost::int16_t as_int16() const;

    /**
     * Returns a pointer to the character data of a string type stored
     * in the pre-defined storage.
     * @note The position is an offset of the storage pointer determined
     * by the metadata and type.
     *
     * @param[out] size The size in bytes of the character string.
     *
     */
    char *as_c_str(unsigned long &size) const;

    /**
     * Returns a pointer to the byte data of a blob type stored in the pre-
     * defined storage.
     * @note The position is an offset of the storage pointer determined
     * by the metadata and type.
     *
     * @param[out] size The size in bytes of the blob data.
     */
    unsigned char *as_blob(unsigned long &size) const;

    float as_float() const;
    double as_double() const;
    void convert(const char* src, char* dst) const;
    boost::int32_t as_rint32() const;
    boost::int8_t as_rint8() const;
    boost::int16_t as_rint16() const;
    boost::int64_t as_rint64() const;
private:
    enum system::enum_field_types m_type;
    size_t m_size;
    const char *m_storage;
    boost::uint32_t m_metadata;
    bool m_is_null;
};

class Converter
{
public:
    /**
     * Converts and copies the sql value to a std::string object.
     * @param[out] str The target string
     * @param[in] val The value object to be converted
     */
    void to(std::string &str, const Value &val) const;

    /**
     * Converts and copies the sql value to a long integer.
     * @param[out] out The target variable
     * @param[in] val The value object to be converted
     */
    void to(long &out, const Value &val) const;

    /**
     * Converts and copies the sql value to a floating point number.
     * @param[out] out The target variable
     * @param[in] val The value object to be converted
     */
    void to(float &out, const Value &val) const;
};


} // end namespace mysql
#endif	/* _VALUE_ADAPTER_H */
