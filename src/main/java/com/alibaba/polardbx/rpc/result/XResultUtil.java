/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.rpc.result;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.OriginalTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.rpc.jdbc.CharsetMapping;
import com.alibaba.polardbx.rpc.utils.LongUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.mysql.cj.polarx.protobuf.PolarxResultset;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Types;
import java.util.Arrays;
import java.util.TimeZone;

/**
 * @version 1.0
 */
public class XResultUtil {

    public static final int COLUMN_FLAGS_UINT_ZEROFILL = 0x0001;
    public static final int COLUMN_FLAGS_DOUBLE_UNSIGNED = 0x0001;
    public static final int COLUMN_FLAGS_FLOAT_UNSIGNED = 0x0001;
    public static final int COLUMN_FLAGS_DECIMAL_UNSIGNED = 0x0001;
    public static final int COLUMN_FLAGS_BYTES_RIGHTPAD = 0x0001;
    public static final int COLUMN_FLAGS_DATETIME_TIMESTAMP = 0x0001;
    public static final int COLUMN_FLAGS_NOT_NULL = 0x0010;
    public static final int COLUMN_FLAGS_PRIMARY_KEY = 0x0020;
    public static final int COLUMN_FLAGS_UNIQUE_KEY = 0x0040;
    public static final int COLUMN_FLAGS_MULTIPLE_KEY = 0x0080;
    public static final int COLUMN_FLAGS_AUTO_INCREMENT = 0x0100;

    public static final int CONTENT_TYPE_BYTES_GEOMETRY = 0x0001;
    public static final int CONTENT_TYPE_BYTES_JSON = 0x0002;
    public static final int CONTENT_TYPE_BYTES_XML = 0x0003;

    public static final int GTS_PROTOCOL_SUCCESS = 0;
    public static final int GTS_PROTOCOL_NOT_INITED = 1;
    public static final int GTS_PROTOCOL_LEASE_EXPIRE = 2;

    public static Pair<Object, byte[]> resultToObject(PolarxResultset.ColumnMetaData meta, ByteString data,
                                                      boolean legacy, TimeZone tz) throws Exception {
        final byte[] rawBytes = data.toByteArray();

        if (0 == rawBytes.length) {
            return new Pair<>(null, null);
        }

        final CodedInputStream stream = CodedInputStream.newInstance(rawBytes);
        final boolean isBinary;
        final String encoding;
        if (meta.hasCollation()) {
            isBinary = 63 == meta.getCollation();
            encoding = CharsetMapping.getJavaEncodingForCollationIndex((int) meta.getCollation());
        } else {
            isBinary = false;
            encoding = null;
        }

        final Object obj;
        final byte[] bytes;
        switch (meta.getType()) {
        case SINT:
            obj = stream.readSInt64();
            bytes = obj.toString().getBytes();
            break;

        case UINT:
            final boolean zerofill = (meta.getFlags() & COLUMN_FLAGS_UINT_ZEROFILL) != 0;
            final byte[] orgBytes;
            switch (meta.getOriginalType()) {
            case MYSQL_TYPE_LONGLONG:
                obj = new BigInteger(ByteBuffer.allocate(9).put((byte) 0).putLong(stream.readUInt64()).array());
                orgBytes = obj.toString().getBytes();
                break;

            case MYSQL_TYPE_YEAR:
                long l = stream.readUInt64();

                MysqlDateTime mysqlDateTime = new MysqlDateTime();
                mysqlDateTime.setSqlType(Types.DATE);
                mysqlDateTime.setNeg(false);
                mysqlDateTime.setYear(l);
                mysqlDateTime.setMonth(1);
                mysqlDateTime.setDay(1);

                obj = new OriginalDate(mysqlDateTime);
                // orgBytes = String.format("%04d", l).getBytes();
                // Manually format this.
                final StringBuilder builder = new StringBuilder();
                if (l >= 1000) {
                    builder.append(l);
                } else if (l >= 100) {
                    builder.append('0').append(l);
                } else if (l >= 10) {
                    builder.append("00").append(l);
                } else if (l >= 0) {
                    builder.append("000").append(l);
                } else {
                    builder.append(l);
                }
                orgBytes = builder.toString().getBytes();
                break;

            default:
                obj = stream.readUInt64();
                orgBytes = obj.toString().getBytes();
                break;
            }
            final int len = meta.getLength();
            if (zerofill && len > orgBytes.length) {
                bytes = new byte[len];
                final int pad = len - orgBytes.length;
                System.arraycopy(orgBytes, 0, bytes, pad, orgBytes.length);
                for (int i = 0; i < pad; ++i) {
                    bytes[i] = '0';
                }
            } else {
                bytes = orgBytes;
            }
            break;

        case DOUBLE:
            obj = stream.readDouble();
            bytes = obj.toString().getBytes();
            break;

        case FLOAT:
            obj = stream.readFloat();
            bytes = obj.toString().getBytes();
            break;

        case BYTES:
            bytes = Arrays.copyOf(rawBytes, rawBytes.length - 1);
            switch (meta.getContentType()) {
            case CONTENT_TYPE_BYTES_GEOMETRY:
                obj = bytes;
                break;

            case CONTENT_TYPE_BYTES_JSON:
            case CONTENT_TYPE_BYTES_XML:
                if (null == encoding || isBinary) {
                    obj = new String(rawBytes, 0, rawBytes.length - 1);
                } else {
                    obj = new String(rawBytes, 0, rawBytes.length - 1, encoding);
                }
                break;

            default:
                if (isBinary) {
                    // VARBINARY
                    obj = bytes;
                } else {
                    // VARCHAR
                    if (null == encoding) {
                        obj = new String(rawBytes, 0, rawBytes.length - 1);
                    } else {
                        obj = new String(rawBytes, 0, rawBytes.length - 1, encoding);
                    }
                }
                break;
            }
            break;

        case TIME: {
            boolean negative = stream.readRawByte() > 0;
            int hours = 0;
            int minutes = 0;
            int seconds = 0;

            int nanos = 0;

            if (!stream.isAtEnd()) {
                hours = (int) stream.readInt64();
                if (!stream.isAtEnd()) {
                    minutes = (int) stream.readInt64();
                    if (!stream.isAtEnd()) {
                        seconds = (int) stream.readInt64();
                        if (!stream.isAtEnd()) {
                            nanos = 1000 * (int) stream.readInt64();
                        }
                    }
                }
            }

            MysqlDateTime mysqlDateTime = new MysqlDateTime();
            mysqlDateTime.setSqlType(Types.TIME);
            mysqlDateTime.setNeg(negative);
            mysqlDateTime.setHour(hours);
            mysqlDateTime.setMinute(minutes);
            mysqlDateTime.setSecond(seconds);
            mysqlDateTime.setSecondPart(nanos);

            if (legacy) {
                obj = new OriginalTime(mysqlDateTime);
            } else {
                throw new UnsupportedOperationException("Unsupported type: " + meta.getType().name());
            }

            // get bytes of time value from mysql datetime.
            String timeStr = mysqlDateTime
                .toTimeString(Math.min(MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE, meta.getFractionalDigits()));
            bytes = timeStr.getBytes();
        }
        break;

        case DATETIME: {
            MysqlDateTime mysqlDateTime = new MysqlDateTime();
            String timeStr;

            int year = (int) stream.readUInt64();
            int month = (int) stream.readUInt64();
            int day = (int) stream.readUInt64();

            // do we have a time too?
            if (stream.getBytesUntilLimit() > 0) {
                int hours = 0;
                int minutes = 0;
                int seconds = 0;

                int nanos = 0;

                if (!stream.isAtEnd()) {
                    hours = (int) stream.readInt64();
                    if (!stream.isAtEnd()) {
                        minutes = (int) stream.readInt64();
                        if (!stream.isAtEnd()) {
                            seconds = (int) stream.readInt64();
                            if (!stream.isAtEnd()) {
                                nanos = 1000 * (int) stream.readInt64();
                            }
                        }
                    }
                }

                mysqlDateTime.setSqlType(Types.TIMESTAMP);
                mysqlDateTime.setNeg(false);
                mysqlDateTime.setYear(year);
                mysqlDateTime.setMonth(month);
                mysqlDateTime.setDay(day);
                mysqlDateTime.setHour(hours);
                mysqlDateTime.setMinute(minutes);
                mysqlDateTime.setSecond(seconds);
                mysqlDateTime.setSecondPart(nanos);

                if (legacy) {
                    obj = new OriginalTimestamp(mysqlDateTime);
                } else {
                    throw new UnsupportedOperationException("Unsupported type: " + meta.getType().name());
                }

                // get bytes of timestamp value from mysql datetime.
                timeStr = mysqlDateTime
                    .toDatetimeString(Math.min(MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE, meta.getFractionalDigits()));
                bytes = timeStr.getBytes();
            } else {
                switch (meta.getOriginalType()) {
                case MYSQL_TYPE_DATE:
                case MYSQL_TYPE_NEWDATE:
                    mysqlDateTime.setSqlType(Types.DATE);
                    mysqlDateTime.setNeg(false);
                    mysqlDateTime.setYear(year);
                    mysqlDateTime.setMonth(month);
                    mysqlDateTime.setDay(day);

                    if (legacy) {
                        obj = new OriginalDate(mysqlDateTime);
                    } else {
                        throw new UnsupportedOperationException("Unsupported type: " + meta.getType().name());
                    }

                    // get bytes of timestamp value from mysql datetime.
                    timeStr = mysqlDateTime.toDateString();
                    bytes = timeStr.getBytes();
                    break;

                case MYSQL_TYPE_DATETIME:
                case MYSQL_TYPE_DATETIME2:
                case MYSQL_TYPE_TIMESTAMP:
                case MYSQL_TYPE_TIMESTAMP2:
                    mysqlDateTime.setSqlType(Types.TIMESTAMP);
                    mysqlDateTime.setNeg(false);
                    mysqlDateTime.setYear(year);
                    mysqlDateTime.setMonth(month);
                    mysqlDateTime.setDay(day);

                    if (legacy) {
                        obj = new OriginalTimestamp(mysqlDateTime);
                    } else {
                        throw new UnsupportedOperationException("Unsupported type: " + meta.getType().name());
                    }
                    // get bytes of timestamp value from mysql datetime.
                    timeStr = mysqlDateTime
                        .toDatetimeString(Math.min(MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE, meta.getFractionalDigits()));
                    bytes = timeStr.getBytes();
                    break;

                default:
                    throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                        "Unsupported type: " + meta.getType().name() + " org_type: " + meta.getOriginalType().name());
                }
            }
        }
        break;

        case SET: {
            StringBuilder vals = new StringBuilder();
            while (stream.getBytesUntilLimit() > 0) {
                if (vals.length() > 0) {
                    vals.append(",");
                }
                long valLen = stream.readUInt64();
                vals.append(new String(stream.readRawBytes((int) valLen), encoding));
            }
            final String str = vals.toString();
            obj = str;
            bytes = str.getBytes();
        }
        break;

        case ENUM:
            final String str = new String(rawBytes, 0, rawBytes.length - 1, encoding);
            obj = str;
            bytes = str.getBytes();
            break;

        case BIT: {
            final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).putLong(stream.readUInt64());
            final int bytesLen = meta.getLength() / 8 + (meta.getLength() % 8 != 0 ? 1 : 0);
            obj = bytes = new byte[bytesLen];
            buf.flip();
            buf.position(Long.BYTES - bytesLen);
            buf.get(bytes);
        }
        break;

        case DECIMAL: {
            byte scale = stream.readRawByte();
            // we allocate an extra char for the sign
            CharBuffer unscaledString = CharBuffer.allocate(2 * stream.getBytesUntilLimit());
            unscaledString.position(1);
            byte sign = 0;
            // read until we encounter the sign bit
            while (true) {
                int b = 0xFF & stream.readRawByte();
                if ((b >> 4) > 9) {
                    sign = (byte) (b >> 4);
                    break;
                }
                unscaledString.append((char) ((b >> 4) + '0'));
                if ((b & 0x0f) > 9) {
                    sign = (byte) (b & 0x0f);
                    break;
                }
                unscaledString.append((char) ((b & 0x0f) + '0'));
            }
            if (stream.getBytesUntilLimit() > 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                    "Did not read all bytes while decoding decimal. Bytes left: " + stream.getBytesUntilLimit());
            }
            switch (sign) {
            case 0xa:
            case 0xc:
            case 0xe:
            case 0xf:
                unscaledString.put(0, '+');
                break;
            case 0xb:
            case 0xd:
                unscaledString.put(0, '-');
                break;
            }
            // may have filled the CharBuffer or one remaining. need to remove it before toString()
            int characters = unscaledString.position();
            unscaledString.clear(); // reset position
            BigInteger unscaled = new BigInteger(unscaledString.subSequence(0, characters).toString());
            obj = new BigDecimal(unscaled, scale);
            bytes = obj.toString().getBytes();
        }
        break;

        default:
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                "Unsupported type: " + meta.getType().name());
        }
        return new Pair<>(obj, bytes);
    }

    private static boolean isUtf8(String charset) {
        return charset.length() >= 4 &&
            ('U' == charset.charAt(0) || 'u' == charset.charAt(0)) &&
            ('T' == charset.charAt(1) || 't' == charset.charAt(1)) &&
            ('F' == charset.charAt(2) || 'f' == charset.charAt(2)) &&
            '8' == charset.charAt(3);
    }

    public static byte[] resultToBytes(PolarxResultset.ColumnMetaData meta, ByteString data, String targetCharset)
        throws Exception {
        if (0 == data.size()) {
            return null;
        }

        final CodedInputStream stream = data.newCodedInput();

        switch (meta.getType()) {
        case SINT:
            return LongUtil.toBytes(stream.readSInt64());

        case UINT: {
            final byte[] orgBytes;
            switch (meta.getOriginalType()) {
            case MYSQL_TYPE_YEAR:
                final byte[] year = LongUtil.toUnsignedBytes(stream.readUInt64());
                orgBytes = new byte[] {'0', '0', '0', '0'};
                assert year.length <= orgBytes.length;
                System.arraycopy(year, 0, orgBytes, orgBytes.length - year.length, year.length);
                break;

            case MYSQL_TYPE_LONGLONG:
            default:
                orgBytes = LongUtil.toUnsignedBytes(stream.readUInt64());
                break;
            }
            final int len = meta.getLength();
            if ((meta.getFlags() & COLUMN_FLAGS_UINT_ZEROFILL) != 0 && len > orgBytes.length) {
                final byte[] bytes = new byte[len];
                final int pad = len - orgBytes.length;
                System.arraycopy(orgBytes, 0, bytes, pad, orgBytes.length);
                for (int i = 0; i < pad; ++i) {
                    bytes[i] = '0';
                }
                return bytes;
            }
            return orgBytes;
        }

        case DOUBLE:
            return Double.toString(stream.readDouble()).getBytes();

        case FLOAT:
            return Float.toString(stream.readFloat()).getBytes();

        case BYTES:
            switch (meta.getContentType()) {
            case CONTENT_TYPE_BYTES_GEOMETRY:
                return stream.readRawBytes(data.size() - 1);

            case CONTENT_TYPE_BYTES_JSON:
            case CONTENT_TYPE_BYTES_XML:
            default:
                final String encoding;
                final String mysqlCharset;
                final boolean isUtf8;
                if (meta.hasCollation()) {
                    if (meta.getCollation() != 63) {
                        encoding = CharsetMapping.getJavaEncodingForCollationIndex((int) meta.getCollation());
                        mysqlCharset = CharsetMapping.getCollationForCollationIndex((int) meta.getCollation());
                        isUtf8 = CharsetMapping.isUtf8((int) meta.getCollation());
                    } else {
                        // Binary.
                        encoding = null;
                        mysqlCharset = null;
                        isUtf8 = false;
                    }
                } else {
                    encoding = null;
                    mysqlCharset = null;
                    isUtf8 = false;
                }

                if (null == mysqlCharset || mysqlCharset.equalsIgnoreCase(targetCharset) || (isUtf8 && isUtf8(
                    targetCharset))) {
                    // Direct copy.
                    return stream.readRawBytes(data.size() - 1);
                }
                return new String(stream.readRawBytes(data.size() - 1), encoding)
                    .getBytes(CharsetMapping.getJavaEncodingForMysqlCharset(targetCharset));
            }

        case TIME: {
            boolean negative = stream.readRawByte() > 0;
            int hours = 0;
            int minutes = 0;
            int seconds = 0;

            int nanos = 0;

            if (!stream.isAtEnd()) {
                hours = (int) stream.readInt64();
                if (!stream.isAtEnd()) {
                    minutes = (int) stream.readInt64();
                    if (!stream.isAtEnd()) {
                        seconds = (int) stream.readInt64();
                        if (!stream.isAtEnd()) {
                            nanos = 1000 * (int) stream.readInt64();
                        }
                    }
                }
            }

            // get bytes of time value from mysql datetime.
            final MysqlDateTime mysqlDateTime = new MysqlDateTime();
            mysqlDateTime.setSqlType(Types.TIME);
            mysqlDateTime.setNeg(negative);
            mysqlDateTime.setHour(hours);
            mysqlDateTime.setMinute(minutes);
            mysqlDateTime.setSecond(seconds);
            mysqlDateTime.setSecondPart(nanos);
            return mysqlDateTime
                .toTimeString(Math.min(MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE, meta.getFractionalDigits())).getBytes();
        }

        case DATETIME: {
            final MysqlDateTime mysqlDateTime = new MysqlDateTime();

            int year = (int) stream.readUInt64();
            int month = (int) stream.readUInt64();
            int day = (int) stream.readUInt64();

            // do we have a time too?
            if (stream.getBytesUntilLimit() > 0) {
                int hours = 0;
                int minutes = 0;
                int seconds = 0;

                int nanos = 0;

                if (!stream.isAtEnd()) {
                    hours = (int) stream.readInt64();
                    if (!stream.isAtEnd()) {
                        minutes = (int) stream.readInt64();
                        if (!stream.isAtEnd()) {
                            seconds = (int) stream.readInt64();
                            if (!stream.isAtEnd()) {
                                nanos = 1000 * (int) stream.readInt64();
                            }
                        }
                    }
                }

                // get bytes of timestamp value from mysql datetime.
                mysqlDateTime.setSqlType(Types.TIMESTAMP);
                mysqlDateTime.setNeg(false);
                mysqlDateTime.setYear(year);
                mysqlDateTime.setMonth(month);
                mysqlDateTime.setDay(day);
                mysqlDateTime.setHour(hours);
                mysqlDateTime.setMinute(minutes);
                mysqlDateTime.setSecond(seconds);
                mysqlDateTime.setSecondPart(nanos);
                return mysqlDateTime
                    .toDatetimeString(Math.min(MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE, meta.getFractionalDigits()))
                    .getBytes();
            } else {
                switch (meta.getOriginalType()) {
                case MYSQL_TYPE_DATE:
                case MYSQL_TYPE_NEWDATE:
                    // get bytes of timestamp value from mysql datetime.
                    mysqlDateTime.setSqlType(Types.DATE);
                    mysqlDateTime.setNeg(false);
                    mysqlDateTime.setYear(year);
                    mysqlDateTime.setMonth(month);
                    mysqlDateTime.setDay(day);
                    return mysqlDateTime.toDateString().getBytes();

                case MYSQL_TYPE_DATETIME:
                case MYSQL_TYPE_DATETIME2:
                case MYSQL_TYPE_TIMESTAMP:
                case MYSQL_TYPE_TIMESTAMP2:
                    // get bytes of timestamp value from mysql datetime.
                    mysqlDateTime.setSqlType(Types.TIMESTAMP);
                    mysqlDateTime.setNeg(false);
                    mysqlDateTime.setYear(year);
                    mysqlDateTime.setMonth(month);
                    mysqlDateTime.setDay(day);
                    return mysqlDateTime
                        .toDatetimeString(Math.min(MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE, meta.getFractionalDigits()))
                        .getBytes();

                default:
                    throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                        "Unsupported type: " + meta.getType().name() + " org_type: " + meta.getOriginalType().name());
                }
            }
        }

        case SET: {
            final String encoding;
            if (meta.hasCollation()) {
                encoding = CharsetMapping.getJavaEncodingForCollationIndex((int) meta.getCollation());
            } else {
                encoding = null;
            }
            StringBuilder vals = new StringBuilder();
            while (stream.getBytesUntilLimit() > 0) {
                if (vals.length() > 0) {
                    vals.append(",");
                }
                long valLen = stream.readUInt64();
                if (encoding != null) {
                    vals.append(new String(stream.readRawBytes((int) valLen), encoding));
                } else {
                    vals.append(new String(stream.readRawBytes((int) valLen)));
                }
            }
            return vals.toString().getBytes(CharsetMapping.getJavaEncodingForMysqlCharset(targetCharset));
        }

        case ENUM: {
            final String encoding;
            final String mysqlCharset;
            final boolean isUtf8;
            if (meta.hasCollation()) {
                if (meta.getCollation() != 63) {
                    encoding = CharsetMapping.getJavaEncodingForCollationIndex((int) meta.getCollation());
                    mysqlCharset = CharsetMapping.getCollationForCollationIndex((int) meta.getCollation());
                    isUtf8 = CharsetMapping.isUtf8((int) meta.getCollation());
                } else {
                    // Binary.
                    encoding = null;
                    mysqlCharset = null;
                    isUtf8 = false;
                }
            } else {
                encoding = null;
                mysqlCharset = null;
                isUtf8 = false;
            }

            if (null == mysqlCharset || mysqlCharset.equalsIgnoreCase(targetCharset) || (isUtf8 && isUtf8(
                targetCharset))) {
                // Direct copy.
                return stream.readRawBytes(data.size() - 1);
            }
            return new String(stream.readRawBytes(data.size() - 1), encoding)
                .getBytes(CharsetMapping.getJavaEncodingForMysqlCharset(targetCharset));
        }

        case BIT: {
            final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).putLong(stream.readUInt64());
            final int bytesLen = meta.getLength() / 8 + (meta.getLength() % 8 != 0 ? 1 : 0);
            final byte[] bytes = new byte[bytesLen];
            buf.flip();
            buf.position(Long.BYTES - bytesLen);
            buf.get(bytes);
            return bytes;
        }

        case DECIMAL: {
            byte scale = stream.readRawByte();
            // we allocate an extra char for the sign
            CharBuffer unscaledString = CharBuffer.allocate(2 * stream.getBytesUntilLimit());
            unscaledString.position(1);
            byte sign = 0;
            // read until we encounter the sign bit
            while (true) {
                int b = 0xFF & stream.readRawByte();
                if ((b >> 4) > 9) {
                    sign = (byte) (b >> 4);
                    break;
                }
                unscaledString.append((char) ((b >> 4) + '0'));
                if ((b & 0x0f) > 9) {
                    sign = (byte) (b & 0x0f);
                    break;
                }
                unscaledString.append((char) ((b & 0x0f) + '0'));
            }
            if (stream.getBytesUntilLimit() > 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                    "Did not read all bytes while decoding decimal. Bytes left: " + stream.getBytesUntilLimit());
            }
            switch (sign) {
            case 0xa:
            case 0xc:
            case 0xe:
            case 0xf:
                unscaledString.put(0, '+');
                break;
            case 0xb:
            case 0xd:
                unscaledString.put(0, '-');
                break;
            }
            // may have filled the CharBuffer or one remaining. need to remove it before toString()
            int characters = unscaledString.position();
            unscaledString.clear(); // reset position
            return new BigDecimal(new BigInteger(unscaledString.subSequence(0, characters).toString()), scale)
                .toString().getBytes();
        }

        default:
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                "Unsupported type: " + meta.getType().name());
        }
    }

    public static PolarxResultset.ColumnMetaData compatibleMetaConvert(
        PolarxResultset.ColumnMetaDataCompatible compatible) {
        final PolarxResultset.ColumnMetaData.Builder builder = PolarxResultset.ColumnMetaData.newBuilder();
        builder.setType(compatible.getType());
        builder.setOriginalType(compatible.getOriginalType());
        if (compatible.hasName()) {
            builder.setName(compatible.getName());
        }
        if (compatible.hasOriginalName()) {
            builder.setOriginalName(compatible.getOriginalName());
        }
        if (compatible.hasTable()) {
            builder.setTable(compatible.getTable());
        }
        if (compatible.hasOriginalTable()) {
            builder.setOriginalTable(compatible.getOriginalTable());
        }
        if (compatible.hasSchema()) {
            builder.setSchema(compatible.getSchema());
        }
        if (compatible.hasCatalog()) {
            builder.setCatalog(compatible.getCatalog());
        }
        if (compatible.hasCollation()) {
            builder.setCollation(compatible.getCollation());
        }
        if (compatible.hasFractionalDigits()) {
            builder.setFractionalDigits(compatible.getFractionalDigits());
        }
        if (compatible.hasLength()) {
            builder.setLength(compatible.getLength());
        }
        if (compatible.hasFlags()) {
            builder.setFlags(compatible.getFlags());
        }
        if (compatible.hasContentType()) {
            builder.setContentType(compatible.getContentType());
        }
        return builder.build();
    }

}
