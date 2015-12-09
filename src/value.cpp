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
#include "value.h"
#include "binlog_event.h"
#include <boost/lexical_cast.hpp>
#include <iomanip>
#include <boost/format.hpp>

using namespace mysql;
using namespace mysql::system;
namespace mysql {

#define DIG_PER_DEC1 9
#define DIG_MASK     100000000
#define DIG_BASE     1000000000
#define DIG_MAX      (DIG_BASE-1)
#define ROUND_UP(X)  (((X)+DIG_PER_DEC1-1)/DIG_PER_DEC1)

static const int32_t powers10[DIG_PER_DEC1 + 1]={
      1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};
static const int dig2bytes[DIG_PER_DEC1 + 1] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
static const int32_t frac_max[DIG_PER_DEC1 - 1]={
     900000000, 990000000, 999000000,
     999900000, 999990000, 999999000,
     999999900, 999999990 };

#define E_DEC_OK                0
#define E_DEC_TRUNCATED         1
#define E_DEC_OVERFLOW          2
#define E_DEC_DIV_ZERO          4
#define E_DEC_BAD_NUM           8
#define E_DEC_OOM              16
#define E_DEC_ERROR            31
#define E_DEC_FATAL_ERROR      30

int decimal_bin_size(int precision, int scale)
{
  int intg   = precision - scale;
  int intg0  = intg / DIG_PER_DEC1;
  int frac0  = scale / DIG_PER_DEC1;
  int intg0x = intg - intg0 * DIG_PER_DEC1;
  int frac0x = scale - frac0 * DIG_PER_DEC1;

  return(
    intg0 * sizeof(boost::int32_t) + dig2bytes[intg0x] +
    frac0 * sizeof(boost::int32_t) + dig2bytes[frac0x]
    );
}

int bin2decimal(const uint8_t* from, decimal_t *to, int precision, int scale)
{
  int error=E_DEC_OK, intg=precision-scale,
      intg0=intg/DIG_PER_DEC1, frac0=scale/DIG_PER_DEC1,
      intg0x=intg-intg0*DIG_PER_DEC1, frac0x=scale-frac0*DIG_PER_DEC1,
      intg1=intg0+(intg0x>0), frac1=frac0+(frac0x>0);
  int32_t *buf=to->buf, mask=(*from & 0x80) ? 0 : -1;
  const uint8_t *stop;
  int bin_size= decimal_bin_size(precision, scale);

  sanity(to);
  boost::shared_ptr<uint8_t> d_copys(new uint8_t[bin_size]);
  uint8_t* d_copy = d_copys.get();
  //d_copy= (uint8_t*) my_alloca(bin_size);
  memcpy(d_copy, from, bin_size);
  d_copy[0]^= 0x80;
  from= d_copy;

  FIX_INTG_FRAC_ERROR(to->len, intg1, frac1, error);
  if (unlikely(error))
  {
    if (intg1 < intg0+(intg0x>0))
    {
      from+=dig2bytes[intg0x]+sizeof(int32_t)*(intg0-intg1);
      frac0=frac0x=intg0x=0;
      intg0=intg1;
    }
    else
    {
      frac0x=0;
      frac0=frac1;
    }
  }

  to->sign=(mask != 0);
  to->intg=intg0*DIG_PER_DEC1+intg0x;
  to->frac=frac0*DIG_PER_DEC1+frac0x;
  if (intg0x)
  {
    int i=dig2bytes[intg0x];
    int32_t UNINIT_VAR(x);
    switch (i)
    {
      case 1: x=mi_sint1korr(from); break;
      case 2: x=mi_sint2korr(from); break;
      case 3: x=mi_sint3korr(from); break;
      case 4: x=mi_sint4korr(from); break;
      default: assert(0);
    }
    from+=i;
    *buf=x ^ mask;
    if (((uint64_t)*buf) >= (uint64_t) powers10[intg0x+1])
      goto err;
    if (buf > to->buf || *buf != 0)
      buf++;
    else
      to->intg-=intg0x;
  }
  for (stop=from+intg0*sizeof(int32_t); from < stop; from+=sizeof(int32_t))
  {
    assert(sizeof(int32_t) == 4);
    *buf=mi_sint4korr(from) ^ mask;
    if (((uint32_t)*buf) > DIG_MAX)
      goto err;
    if (buf > to->buf || *buf != 0)
      buf++;
    else
      to->intg-=DIG_PER_DEC1;
  }
  assert(to->intg >=0);
  for (stop=from+frac0*sizeof(int32_t); from < stop; from+=sizeof(int32_t))
  {
    assert(sizeof(int32_t) == 4);
    *buf=mi_sint4korr(from) ^ mask;
    if (((uint32_t)*buf) > DIG_MAX)
      goto err;
    buf++;
  }
  if (frac0x)
  {
    int i=dig2bytes[frac0x];
    int32_t UNINIT_VAR(x);
    switch (i)
    {
      case 1: x=mi_sint1korr(from); break;
      case 2: x=mi_sint2korr(from); break;
      case 3: x=mi_sint3korr(from); break;
      case 4: x=mi_sint4korr(from); break;
      default: assert(0);
    }
    *buf=(x ^ mask) * powers10[DIG_PER_DEC1 - frac0x];
    if (((uint32_t)*buf) > DIG_MAX)
      goto err;
    buf++;
  }
  //my_afree(d_copy);
  /*
  *  No digits? We have read the number zero, of unspecified precision.
  *  Make it a proper zero, with non-zero precision.
  */
  if (to->intg == 0 && to->frac == 0)
    decimal_make_zero(to);
  return error;

err:
  //my_afree(d_copy);
  decimal_make_zero(to);
  return(E_DEC_BAD_NUM);
}
static int32_t *remove_leading_zeroes(const decimal_t *from, int *intg_result)
{
  int intg= from->intg, i;
  int32_t *buf0= from->buf;
  i= ((intg - 1) % DIG_PER_DEC1) + 1;
  while (intg > 0 && *buf0 == 0)
  {
    intg-= i;
    i= DIG_PER_DEC1;
    buf0++;
  }
  if (intg > 0)
  {
    for (i= (intg - 1) % DIG_PER_DEC1; *buf0 < powers10[i--]; intg--) ;
    assert(intg > 0);
  }
  else
    intg=0;
  *intg_result= intg;
  return buf0;
}

/*
 *   Convert decimal to its printable string representation
 *   SYNOPSIS
 *   decimal2string()
 *   from            - value to convert
 *   to              - points to buffer where string representation
 *   should be stored
 *   *to_len         - in:  size of to buffer (incl. terminating '\0')
 *   out: length of the actually written string (excl. '\0')
 *   fixed_precision - 0 if representation can be variable length and
 *   fixed_decimals will not be checked in this case.
 *   Put number as with fixed point position with this
 *   number of digits (sign counted and decimal point is
 *   counted)
 *   fixed_decimals  - number digits after point.
 *   filler          - character to fill gaps in case of fixed_precision > 0
 *   RETURN VALUE E_DEC_OK/E_DEC_TRUNCATED/E_DEC_OVERFLOW
*/

int decimal2string(const decimal_t *from, char *to, int *to_len,
                       int fixed_precision, int fixed_decimals,
                                          char filler)
{
  /* {intg_len, frac_len} output widths; {intg, frac} places in input */
  int len, intg, frac= from->frac, i, intg_len, frac_len, fill;
  /* number digits before decimal point */
  int fixed_intg= (fixed_precision ?
      (fixed_precision - fixed_decimals) : 0);
  int error=E_DEC_OK;
  char *s=to;
  int32_t *buf, *buf0=from->buf, tmp;

  assert(*to_len >= 2+from->sign);

  /* removing leading zeroes */
  buf0= remove_leading_zeroes(from, &intg);
  if (unlikely(intg+frac==0))
  {
    intg=1;
    tmp=0;
    buf0=&tmp;
  }

  if (!(intg_len= fixed_precision ? fixed_intg : intg))
    intg_len= 1;
  frac_len= fixed_precision ? fixed_decimals : frac;
  len= from->sign + intg_len + MY_TEST(frac) + frac_len;
  if (fixed_precision)
  {
    if (frac > fixed_decimals)
    {
      error= E_DEC_TRUNCATED;
      frac= fixed_decimals;
    }
    if (intg > fixed_intg)
    {
      error= E_DEC_OVERFLOW;
      intg= fixed_intg;
    }
  }
  else if (unlikely(len > --*to_len)) /* reserve one byte for \0 */
  {
    int j= len - *to_len;             /* excess printable chars */
    error= (frac && j <= frac + 1) ? E_DEC_TRUNCATED : E_DEC_OVERFLOW;
    /*
     * If we need to cut more places than frac is wide, we'll end up
     * dropping the decimal point as well.  Account for this.
    */
    if (frac && j >= frac + 1)
      j--;

    if (j > frac)
    {
      intg_len= intg-= j-frac;
      frac= 0;
    }
    else
      frac-=j;
    frac_len= frac;
    len= from->sign + intg_len + MY_TEST(frac) + frac_len;
  }
  *to_len= len;
  s[len]= 0;

  if (from->sign)
    *s++='-';

  if (frac)
  {
    char *s1= s + intg_len;
    fill= frac_len - frac;
    buf=buf0+ROUND_UP(intg);
    *s1++='.';
    for (; frac>0; frac-=DIG_PER_DEC1)
    {
      int32_t x=*buf++;
      for (i= MY_MIN(frac, DIG_PER_DEC1); i; i--)
      {
        int32_t y=x/DIG_MASK;
        *s1++='0'+(uint8_t)y;
        x-=y*DIG_MASK;
        x*=10;
      }
    }
    for(; fill > 0; fill--)
      *s1++=filler;
  }

  fill= intg_len - intg;
  if (intg == 0)
    fill--; /* symbol 0 before digital point */
  for(; fill > 0; fill--)
    *s++=filler;
  if (intg)
  {
    s+=intg;
    for (buf=buf0+ROUND_UP(intg); intg>0; intg-=DIG_PER_DEC1)
    {
      int32_t x=*--buf;
      for (i= MY_MIN(intg, DIG_PER_DEC1); i; i--)
      {
        int32_t y=x/10;
        *--s='0'+(uint8_t)(x-y*10);
        x=y;
      }
    }
  }
  else
    *s= '0';
  return error;
}


int calc_field_size(unsigned char column_type, const unsigned char *field_ptr, boost::uint32_t metadata)
{
  boost::uint32_t length;

  switch (column_type) {
  case mysql::system::MYSQL_TYPE_VAR_STRING:
    /* This type is hijacked for result set types. */
    length= metadata;
    break;
  case mysql::system::MYSQL_TYPE_NEWDECIMAL:
  {
    //int precision = (metadata & 0xff);
    //int scale = metadata >> 8;
    int scale = (metadata & 0xff);
    int precision = metadata >> 8;
    length = decimal_bin_size(precision, scale);
    break;
  }
  case mysql::system::MYSQL_TYPE_DECIMAL:
  case mysql::system::MYSQL_TYPE_FLOAT:
  case mysql::system::MYSQL_TYPE_DOUBLE:
    length= metadata;
    break;
  /*
    The cases for SET and ENUM are include for completeness, however
    both are mapped to type MYSQL_TYPE_STRING and their real types
    are encoded in the field metadata.
  */
  case mysql::system::MYSQL_TYPE_SET:
  case mysql::system::MYSQL_TYPE_ENUM:
  case mysql::system::MYSQL_TYPE_STRING:
  {
    unsigned char type= metadata >> 8U;
    //unsigned char type = metadata & 0xff;
    if ((type == mysql::system::MYSQL_TYPE_SET) || (type == mysql::system::MYSQL_TYPE_ENUM))
    {
      length= metadata & 0x00ff;
      //length = (metadata & 0xff00) >> 8;
    }
    else
    {
      /*
        We are reading the actual size from the master_data record
        because this field has the actual lengh stored in the first
        byte.
      */
      uint32_t max_length = (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0x00ff);
      //std::cout << "max_length " << max_length << std::endl;
      length= (max_length>255) ? 2 : 1;
      length+= length == 1 ? (boost::uint32_t) *field_ptr : *((boost::uint16_t *)field_ptr);
      //std::cout << "length " << length << std::endl;
      //assert(length == 0);
    }
    break;
  }
  case mysql::system::MYSQL_TYPE_YEAR:
  case mysql::system::MYSQL_TYPE_TINY:
    length= 1;
    break;
  case mysql::system::MYSQL_TYPE_SHORT:
    length= 2;
    break;
  case mysql::system::MYSQL_TYPE_INT24:
    length= 3;
    break;
  case mysql::system::MYSQL_TYPE_LONG:
    length= 4;
    break;
  case MYSQL_TYPE_LONGLONG:
    length= 8;
    break;
  case mysql::system::MYSQL_TYPE_NULL:
    length= 0;
    break;
  case mysql::system::MYSQL_TYPE_NEWDATE:
    length= 3;
    break;
  case MYSQL_TYPE_DATETIME2:
    length= 5 + (metadata + 1) / 2;
    break;
  case MYSQL_TYPE_TIME2:
    length= 3 + (metadata + 1) / 2;
    break;
  case MYSQL_TYPE_TIMESTAMP2:
    length= 4 + (metadata + 1) / 2;
    break;
  case mysql::system::MYSQL_TYPE_DATE:
  case mysql::system::MYSQL_TYPE_TIME:
    length= 3;
    break;
  case mysql::system::MYSQL_TYPE_TIMESTAMP:
    length= 4;
    break;
  case mysql::system::MYSQL_TYPE_DATETIME:
    length= 8;
    break;
  case mysql::system::MYSQL_TYPE_BIT:
  {
    /*
      Decode the size of the bit field from the master.
        from_len is the length in bytes from the master
        from_bit_len is the number of extra bits stored in the master record
      If from_bit_len is not 0, add 1 to the length to account for accurate
      number of bytes needed.
    */
	  boost::uint32_t from_len= (metadata >> 8U) & 0x00ff;
	  boost::uint32_t from_bit_len= metadata & 0x00ff;
    //DBUG_ASSERT(from_bit_len <= 7);
    length= from_len + ((from_bit_len > 0) ? 1 : 0);
    break;
  }
  case mysql::system::MYSQL_TYPE_VARCHAR:
  {
    length= metadata > 255 ? 2 : 1;
    length+= length == 1 ? (boost::uint32_t) *field_ptr : *((boost::uint16_t *)field_ptr);
    break;
  }
  case mysql::system::MYSQL_TYPE_TINY_BLOB:
  case mysql::system::MYSQL_TYPE_MEDIUM_BLOB:
  case mysql::system::MYSQL_TYPE_LONG_BLOB:
  case mysql::system::MYSQL_TYPE_BLOB:
  case mysql::system::MYSQL_TYPE_GEOMETRY:
  {
     switch (metadata)
    {
      case 1:
        length= 1+ (boost::uint32_t) field_ptr[0];
        break;
      case 2:
        length= 2+ (boost::uint32_t) (*(boost::uint16_t *)(field_ptr) & 0xFFFF);
        break;
      case 3:
        // TODO make platform indep.
        length= 3+ (boost::uint32_t) (long) (*((boost::uint32_t *) (field_ptr)) & 0xFFFFFF);
        break;
      case 4:
        // TODO make platform indep.
        length= 4+ (boost::uint32_t) (long) *((boost::uint32_t *) (field_ptr));
        break;
      default:
        length= 0;
        break;
    }
    break;
  }
  default:
    length= ~(boost::uint32_t) 0;
  }
  return length;
}

/*
Value::Value(Value &val)
{
  m_size= val.length();
  m_storage= val.storage();
  m_type= val.type();
  m_metadata= val.metadata();
  m_is_null= val.is_null();
}
*/

Value::Value(const Value& val)
{
  m_size= val.m_size;
  m_storage= val.m_storage;
  m_type= val.m_type;
  m_metadata= val.m_metadata;
  m_is_null= val.m_is_null;
}

Value &Value::operator=(const Value &val)
{
  m_size= val.m_size;
  m_storage= val.m_storage;
  m_type= val.m_type;
  m_metadata= val.m_metadata;
  m_is_null= val.m_is_null;
  return *this;
}

bool Value::operator==(const Value &val) const
{
  return (m_size == val.m_size) &&
         (m_storage == val.m_storage) &&
         (m_type == val.m_type) &&
         (m_metadata == val.m_metadata);
}

bool Value::operator!=(const Value &val) const
{
  return !operator==(val);
}

void Value::convert(const char* src, char* dst) const {
  int n = m_size;
  do {
    *dst++= *(src+(--n));
  } while (n != 0);
}

char *Value::as_c_str(unsigned long &size) const
{
  if (m_is_null || m_size == 0)
  {
    size= 0;
    return 0;
  }
  /*
   Length encoded; First byte is length of string.
  */
  int metadata_length= m_size > 251 ? 2: 1;
  /*
   Size is length of the character string; not of the entire storage
  */
  size= m_size - metadata_length;

  char *str = const_cast<char *>(m_storage + metadata_length);

  if ((m_type == mysql::system::MYSQL_TYPE_VARCHAR) && m_metadata > 255) {
    str++;
    size--;
  }

  return str;
}

unsigned char *Value::as_blob(unsigned long &size) const
{
  if (m_is_null || m_size == 0)
  {
    size= 0;
    return 0;
  }

  /*
   Size was calculated during construction of the object and only inludes the
   size of the blob data, not the metadata part which also is stored in the
   storage. For blobs this part can be between 1-4 bytes long.
  */
  size= m_size - m_metadata;

  /*
   Adjust the storage pointer with the size of the metadata.
  */
  return (unsigned char *)(m_storage + m_metadata);
}

boost::int32_t Value::as_int32() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::uint32_t to_int;
  Protocol_chunk<boost::uint32_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int32_t Value::as_rint32() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::uint32_t to_int;
  Protocol_chunk<boost::uint32_t> prot_integer(to_int);

  boost::shared_ptr<char> dst(new char[m_size]);
  convert(m_storage, dst.get());
  buffer_source buff(dst.get(), m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int8_t Value::as_rint8() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int8_t to_int;
  Protocol_chunk<boost::int8_t> prot_integer(to_int);

  boost::shared_ptr<char> dst(new char[m_size]);
  convert(m_storage, dst.get());
  buffer_source buff(dst.get(), m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int8_t Value::as_int8() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int8_t to_int;
  Protocol_chunk<boost::int8_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int16_t Value::as_rint16() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int16_t to_int;
  Protocol_chunk<boost::int16_t> prot_integer(to_int);

  boost::shared_ptr<char> dst(new char[m_size]);
  convert(m_storage, dst.get());
  buffer_source buff(dst.get(), m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int16_t Value::as_int16() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int16_t to_int;
  Protocol_chunk<boost::int16_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int64_t Value::as_rint64() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int64_t to_int;
  Protocol_chunk<boost::int64_t> prot_integer(to_int);

  boost::shared_ptr<char> dst(new char[m_size]);
  convert(m_storage, dst.get());
  buffer_source buff(dst.get(), m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int64_t Value::as_int64() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int64_t to_int;
  Protocol_chunk<boost::int64_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

float Value::as_float() const
{
  // TODO
  return *((const float *)storage());
}

double Value::as_double() const
{
  // TODO
  return *((const double *)storage());
}

void Converter::to(std::string &str, const Value &val) const
{
  if (val.is_null())
  {
    str= "NULL";
    return;
  }

  boost::uint8_t type = val.type();
  boost::int32_t len = 0;
  if (type == MYSQL_TYPE_STRING)
  {
    if (val.metadata() >= 256)
    {
      boost::uint8_t byte0= val.metadata() >> 8;
      boost::uint8_t byte1= val.metadata() & 0xFF;
      if ((byte0 & 0x30) != 0x30)
      {
        len = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
        /* a long CHAR() field: see #37426 */
        type= byte0 | 0x30;
      } else {
        switch (byte0) {
          case MYSQL_TYPE_SET:
          case MYSQL_TYPE_ENUM:
          case MYSQL_TYPE_STRING:
            type = byte0;
            len = byte1;
            break;
          default:
            str="MYSQL_TYPE_STRING unknow type:"+type;
        }
      }
    } else {
      len = val.metadata();
    }
  }

  switch(type)
  {
    case MYSQL_TYPE_DECIMAL:
      str= "decimal not implemented";
      break;
    case MYSQL_TYPE_TINY:
      str= boost::lexical_cast<std::string>(static_cast<int>(val.as_int8()));
      break;
    case MYSQL_TYPE_SHORT:
      str= boost::lexical_cast<std::string>(val.as_int16());
      break;
    case MYSQL_TYPE_LONG:
      str= boost::lexical_cast<std::string>(val.as_int32());
      break;
    case MYSQL_TYPE_FLOAT:
    {
      //str= boost::str(boost::format("%-.20g") % val.as_float());
      str= boost::str(boost::format("%d") % val.as_float());
    }
      break;
    case MYSQL_TYPE_DOUBLE:
      //str= boost::str(boost::format("%-.20g") % val.as_double());
      str= boost::str(boost::format("%d") % val.as_double());
      break;
    case MYSQL_TYPE_NULL:
      str= "not implemented";
      break;
    case MYSQL_TYPE_TIMESTAMP:
      str= boost::lexical_cast<std::string>((boost::uint32_t)val.as_int32());
      break;

    case MYSQL_TYPE_LONGLONG:
      str= boost::lexical_cast<std::string>(val.as_int64());
      break;
    case MYSQL_TYPE_INT24:
    {
      str= "not implemented";
      int32_t data_val = sint3korr(val.storage()); 
      uint32_t u_data_val = uint3korr(val.storage()); 
      if (data_val < 0)
        str= boost::lexical_cast<std::string>(u_data_val);
      else 
        str= boost::lexical_cast<std::string>(data_val);
      break;
    }
    case MYSQL_TYPE_DATE:
    {
      const char* val_storage = val.storage();
      unsigned int date_val = (val_storage[0] & 0xff) + ((val_storage[1] & 0xff) << 8) + ((val_storage[2] & 0xff) << 16);
      unsigned int date_year = date_val >> 9;
      date_val -= (date_year << 9);
      unsigned int date_month = date_val >> 5;
      unsigned int date_day = date_val - (date_month << 5);
      str = boost::str(boost::format("%04d-%02d-%02d") % date_year % date_month % date_day);
      break;
    }
    case MYSQL_TYPE_DATETIME:
    {
      boost::uint64_t timestamp= val.as_int64();
      unsigned long d= timestamp / 1000000;
      unsigned long t= timestamp % 1000000;
      std::ostringstream os;

      os << std::setfill('0') << std::setw(4) << d / 10000
         << std::setw(1) << '-'
         << std::setw(2) << (d % 10000) / 100
         << std::setw(1) << '-'
         << std::setw(2) << d % 100
         << std::setw(1) << ' '
         << std::setw(2) << t / 10000
         << std::setw(1) << ':'
         << std::setw(2) << (t % 10000) / 100
         << std::setw(1) << ':'
         << std::setw(2) << t % 100;

      str= os.str();
    }
      break;
    case MYSQL_TYPE_TIME:
    {
      const char* val_storage = val.storage();
      unsigned int time_val = (val_storage[0] & 0xff) + ((val_storage[1] & 0xff) << 8) + ((val_storage[2] & 0xff) << 16);
      unsigned int time_sec = time_val % 100;
      time_val -= time_sec;
      unsigned int time_min = (time_val % 10000) / 100;
      unsigned int time_hour = (time_val - time_min) / 10000;
      str = boost::str(boost::format("%02d:%02d:%02d") % time_hour % time_min % time_sec);
      break;
    }
    case MYSQL_TYPE_YEAR:
    {
      const char* val_storage = val.storage();
      unsigned int year_val = (val_storage[0] & 0xff);
      year_val = year_val > 0 ? (year_val + 1900) : 0;
      str = boost::str(boost::format("%04d") % year_val);
      break;
    }
    case MYSQL_TYPE_NEWDATE:
    {
      //str= "not implemented";
      uint32_t tmp= uint3korr(val.storage());
      int part;
      char buf[11];
      char *pos= &buf[10];  // start from '\0' to the beginning
      /* Copied from field.cc */
      *pos--=0;         // End NULL
      part=(int) (tmp & 31);
      *pos--= (char) ('0'+part%10);
      *pos--= (char) ('0'+part/10);
      *pos--= ':';
      part=(int) (tmp >> 5 & 15);
      *pos--= (char) ('0'+part%10);
      *pos--= (char) ('0'+part/10);
      *pos--= ':';
      part=(int) (tmp >> 9);
      *pos--= (char) ('0'+part%10); part/=10;
      *pos--= (char) ('0'+part%10); part/=10;
      *pos--= (char) ('0'+part%10); part/=10;
      *pos=   (char) ('0'+part);
      str = buf;
      break;
    }
    case MYSQL_TYPE_VARCHAR:
    {
      unsigned long size;
      char *ptr= val.as_c_str(size);
      str.append(ptr, size);
    }
      break;
    case MYSQL_TYPE_VAR_STRING:
    {
      str.append(val.storage(), val.length());
    }
    break;
    case MYSQL_TYPE_STRING:
    {
      unsigned long size = len;
      int metadata_length;
      //std::cout <<"before in string:" << len << std::endl;
      if (len < 256) {
        len = static_cast<int>(val.as_int8());
        metadata_length = 1;
      } else {
        len = static_cast<int>(val.as_int16());
        metadata_length = 2;
      }
      //std::cout <<"in string:" << len << " metadata_len: " << metadata_length<< std::endl;
      /*
         Size is length of the character string; not of the entire storage
         */
      char *ptr = const_cast<char *>(val.storage() + metadata_length);
      str.append(val.storage()+metadata_length, len);
      //std::cout <<"in string:" << len << std::endl;
      //unsigned long size;
      //char *ptr = val.as_c_str(size);
      //str.append(ptr, size);
    }
      break;
    case MYSQL_TYPE_BIT:
    {
      //str= "not implemented";
      uint32_t nbits= ((val.metadata() >> 8) * 8) + (val.metadata() & 0xFF);
      int32_t len = (nbits + 7) / 8;
      if (nbits > 1) {
        switch(len) {
          case 1:
            str = boost::lexical_cast<std::string>mi_uint1korr(val.storage());
            break;
          case 2:
            str = boost::lexical_cast<std::string>mi_uint2korr(val.storage());
            break;
          case 3:
            str = boost::lexical_cast<std::string>mi_uint3korr(val.storage());
            break;
          case 4:
            str = boost::lexical_cast<std::string>mi_uint4korr(val.storage());
            break;
          case 5:
            str = boost::lexical_cast<std::string>mi_uint5korr(val.storage());
            break;
          case 6:
            str = boost::lexical_cast<std::string>mi_uint6korr(val.storage());
            break;
          case 7:
            str = boost::lexical_cast<std::string>mi_uint7korr(val.storage());
            break;
          case 8:
            str = boost::lexical_cast<std::string>mi_uint8korr(val.storage());
            break;
          default:
            str = "unknow len = " + boost::lexical_cast<std::string>(len);
        }
      } else {
            str = boost::lexical_cast<std::string>mi_uint1korr(val.storage());
      }
      /*
      uint32_t bitnum, nbits8= ((nbits + 7) / 8) * 8, skip_bits= nbits8 - nbits;
      for (bitnum= skip_bits ; bitnum < nbits8; bitnum++)
      {
         int is_set= (val.storage()[(bitnum) / 8] >> (7 - bitnum % 8))  & 0x01;
         str += is_set ? "1" : "0";
      }
      */
      break;
    }
    case MYSQL_TYPE_NEWDECIMAL:
    {
      //str= "not implemented";
      //int precision = (val.metadata() & 0xff);
      //int decimals = val.metadata() >> 8;
      int decimals = (val.metadata() & 0xff);
      int precision = val.metadata() >> 8;
      my_decimal dec;
      bin2decimal((uint8_t*) val.storage(), &dec, precision, decimals);
      int len= DECIMAL_MAX_STR_LENGTH;
      char buff[DECIMAL_MAX_STR_LENGTH + 1];
      decimal2string(&dec,buff,&len, 0, 0, 0);
      str = buff;
      break;
    }
    case MYSQL_TYPE_ENUM:
      //str= "not implemented";
      switch (val.metadata() & 0xFF) {
        case 1:
          str = boost::lexical_cast<std::string>(val.as_int32());
          break;
        case 2:
          str = boost::lexical_cast<std::string>(val.as_int16());
          break;
        default:
          str = "!! Unknown ENUM packlen=" + boost::lexical_cast<std::string>(val.metadata() & 0xFF);
      }
      break;
    case MYSQL_TYPE_SET:
    {
      uint32_t nbits= ((val.metadata() &  0xFF) * 8);
      int32_t len = (nbits + 7) / 8;
      if (nbits > 1) {
        switch(len) {
          case 1:
            str= boost::lexical_cast<std::string>(static_cast<int>(val.as_int8()));
            break;
          case 2:
            str = boost::lexical_cast<std::string>mi_uint2korr(val.storage());
            break;
          case 3:
            str = boost::lexical_cast<std::string>mi_uint3korr(val.storage());
            break;
          case 4:
            str = boost::lexical_cast<std::string>mi_uint4korr(val.storage());
            break;
          case 5:
            str = boost::lexical_cast<std::string>mi_uint5korr(val.storage());
            break;
          case 6:
            str = boost::lexical_cast<std::string>mi_uint6korr(val.storage());
            break;
          case 7:
            str = boost::lexical_cast<std::string>mi_uint7korr(val.storage());
            break;
          case 8:
            str = boost::lexical_cast<std::string>mi_uint8korr(val.storage());
            break;
          default:
            str = "unknow len = " + boost::lexical_cast<std::string>(len);
        }
      } else {
        str= boost::lexical_cast<std::string>(static_cast<int>(val.as_int8()));
      }
      break;
    }
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    {
      unsigned long size;
      unsigned char *ptr= val.as_blob(size);
      str.append((const char *)ptr, size);
    }
      break;
    case MYSQL_TYPE_GEOMETRY:
      str= "not implemented";
      break;
    case MYSQL_TYPE_DATETIME2:
    {
      const char* ptr = val.storage();
      int64_t intpart = mi_uint5korr(ptr) - DATETIMEF_INT_OFS;
      boost::uint32_t frac = 0;
      switch (val.metadata()) {
        case 0:
          frac = 0;
          break;
        case 1:
        case 2:
          frac= ((int32_t) (int8_t) ptr[5]) * 10000;
          break;
        case 3:
        case 4:
          frac= mi_sint2korr(ptr + 5) * 100;
          break;
        case 5:
        case 6:
          frac = mi_sint3korr(ptr + 5);
          break;
        default:
          frac = 0;
          break;
      }
      if (intpart == 0) {
        str = "0000-00-00 00:00:00";
      } else {
        int64_t ymd = intpart >> 17;
        int64_t ym = ymd >> 5;
        int64_t hms = intpart % (1 << 17);
        std::ostringstream os;
        os << std::setfill('0') << std::setw(4) << (int32_t) (ym /13)
          << std::setw(1) << '-'
          << std::setw(2) << (int32_t) (ym % 13)
          << std::setw(1) << '-'
          << std::setw(2) << (int32_t) (ymd % (1 << 5))
          << std::setw(1) << ' '
          << std::setw(2) << (int32_t) (hms >> 12)
          << std::setw(1) << ':'
          << std::setw(2) << (int32_t) ((hms >> 6) % (1 <<6))
          << std::setw(1) << ':'
          << std::setw(2) << (int32_t) (hms % (1 << 6));
        str = os.str();
      }

      if (val.metadata() >= 1) {
        str += "." + boost::lexical_cast<std::string>(frac);
      } 
      break;
    }
    case MYSQL_TYPE_TIME2:
    {
      long intpart = 0;
      int32_t frac = 0;
      long ltime = 0;
      const char* ptr = val.storage();
      switch (val.metadata())
      {
        case 0:
          intpart= mi_uint3korr(ptr) - TIMEF_INT_OFS;
          //intpart = val.as_rint32() - TIMEF_INT_OFS;
          ltime = intpart << 24;
          break;
        case 1:
        case 2:
          {
            intpart= mi_uint3korr(ptr) - TIMEF_INT_OFS;
            frac= (uint8_t) ptr[3];
            if (intpart < 0 && frac)
            {
              intpart++; 
              frac-= 0x100; 
            }
            frac = frac * 10000;
            ltime = intpart << 24;
            break;
          }
        case 3:
        case 4:
          {
            intpart= mi_uint3korr(ptr) - TIMEF_INT_OFS;
            frac= mi_uint2korr(ptr+ 3);
            if (intpart < 0 && frac)
            {
              intpart++;
              frac-= 0x10000;
            }
          }
          frac = frac * 100;
          ltime = intpart << 24;
          break;
        case 5:
        case 6:
          intpart = ((long) mi_uint6korr(ptr)) - TIMEF_OFS;
          ltime = intpart;
          frac = (int32_t) (intpart % (1L << 24));
          break;
        default:
          intpart= mi_uint3korr(ptr) - TIMEF_INT_OFS;
          ltime = intpart << 24;
          break;
      }

      if (intpart == 0) {
        str = "00:00:00";
      } else {
        long ultime = ltime < 0 ? -ltime : ltime;
        intpart = ultime >> 24;
        uint32_t time_hour =(uint32_t)((intpart>>12) % (1<<10));
        uint32_t time_min = (uint32_t) ((intpart>>6) % (1<<6));
        uint32_t time_sec = (uint32_t) (intpart % (1<<6));
        uint32_t time_sec_part = ((intpart) % (1LL << 24));
        uint32_t time_type = 2;
        str= boost::str(boost::format("%s%02u:%02u:%02u") %(ltime>=0?"":"-") % time_hour % time_min % time_sec);
      }
      if (val.metadata() >= 1) {
        str += "." + boost::lexical_cast<std::string>(abs(frac));
      } 
      break;
    }
    case MYSQL_TYPE_TIMESTAMP2:
    {
      boost::uint32_t tv_usec = 0;
      boost::uint32_t tv_sec = val.as_rint32();
      switch (val.metadata()) {
        case 0:
          tv_usec = 0;
        case 1:
        case 2:
          tv_usec = val.as_rint8() * 10000;
          break;
        case 3:
        case 4:
          tv_usec = val.as_rint16() * 100;
          break;
        case 5:
        case 6:
          tv_usec = mi_sint3korr(val.storage());
          break;
        default:
          tv_usec = 0;
          break;
      }
      if (tv_sec == 0) {
        str += "0000-00-00 00:00:00";
      } else {
        time_t time_val = tv_sec;
        struct tm* ptm = localtime(&time_val);
        std::ostringstream os;

        os << std::setfill('0') << std::setw(4) << ptm->tm_year + 1900
          << std::setw(1) << '-'
          << std::setw(2) << ptm->tm_mon + 1
          << std::setw(1) << '-'
          << std::setw(2) << ptm->tm_mday
          << std::setw(1) << ' '
          << std::setw(2) << ptm->tm_hour
          << std::setw(1) << ':'
          << std::setw(2) << ptm->tm_min
          << std::setw(1) << ':'
          << std::setw(2) << ptm->tm_sec;
        str += os.str();
      }
      if (val.metadata() >= 1) {
        str += "." + boost::lexical_cast<std::string>(tv_usec);
      } 
      break;
    }
    default:
      str= "not implemented";
      break;
  }
}

void Converter::to(float &out, const Value &val) const
{
  switch(val.type())
  {
  case MYSQL_TYPE_FLOAT:
    out= val.as_float();
    break;
  default:
    out= 0;
  }
}

void Converter::to(long &out, const Value &val) const
{
  switch(val.type())
  {
    case MYSQL_TYPE_DECIMAL:
      // TODO
      out= 0;
      break;
    case MYSQL_TYPE_TINY:
      out= val.as_int8();
      break;
    case MYSQL_TYPE_SHORT:
      out= val.as_int16();
      break;;
    case MYSQL_TYPE_LONG:
      out= (long)val.as_int32();
      break;
    case MYSQL_TYPE_FLOAT:
      out= 0;
      break;
    case MYSQL_TYPE_DOUBLE:
      out= (long)val.as_double();
    case MYSQL_TYPE_NULL:
      out= 0;
      break;
    case MYSQL_TYPE_TIMESTAMP:
      out=(boost::uint32_t)val.as_int32();
      break;

    case MYSQL_TYPE_LONGLONG:
      out= (long)val.as_int64();
      break;
    case MYSQL_TYPE_INT24:
      out= 0;
      break;
    case MYSQL_TYPE_DATE:
      out= 0;
      break;
    case MYSQL_TYPE_TIME:
      out= 0;
      break;
    case MYSQL_TYPE_DATETIME:
      out= (long)val.as_int64();
      break;
    case MYSQL_TYPE_YEAR:
      out= 0;
      break;
    case MYSQL_TYPE_NEWDATE:
      out= 0;
      break;
    case MYSQL_TYPE_VARCHAR:
      out= 0;
      break;
    case MYSQL_TYPE_BIT:
      out= 0;
      break;
    case MYSQL_TYPE_NEWDECIMAL:
      out= 0;
      break;
    case MYSQL_TYPE_ENUM:
      out= 0;
      break;
    case MYSQL_TYPE_SET:
      out= 0;
      break;
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
      out= 0;
      break;
    case MYSQL_TYPE_VAR_STRING:
    {
      std::string str;
      str.append(val.storage(), val.length());
      out= boost::lexical_cast<long>(str.c_str());
    }
      break;
    case MYSQL_TYPE_STRING:
      out= 0;
      break;
    case MYSQL_TYPE_GEOMETRY:
      out= 0;
      break;
    default:
      out= 0;
      break;
  }
}


} // end namespace mysql
