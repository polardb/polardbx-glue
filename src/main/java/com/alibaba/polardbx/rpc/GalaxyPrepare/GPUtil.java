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

package com.alibaba.polardbx.rpc.GalaxyPrepare;

import com.alibaba.polardbx.common.jdbc.TableName;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.rpc.client.XSession;
import com.google.protobuf.CodedOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @version 1.0
 */
public class GPUtil {

    public static byte[] genUtf8String(String s) {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.write((byte) (MySQLStandardFieldType.MYSQL_TYPE_STRING.getId() & 0xFF)); // type
            coded.write((byte) 0); // flag (null:1 unsigned:1)
            final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            coded.writeInt64NoTag(bytes.length);
            coded.write(bytes, 0, bytes.length);
            coded.flush();
            return stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static byte[] genSInt(long i) {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.write((byte) (MySQLStandardFieldType.MYSQL_TYPE_LONGLONG.getId() & 0xFF)); // type
            coded.write((byte) 0); // flag (null:1 unsigned:1)
            coded.writeInt64NoTag(8);
            coded.writeFixed64NoTag(i);
            coded.flush();
            return stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static byte[] genUInt(long u) {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.write((byte) (MySQLStandardFieldType.MYSQL_TYPE_LONGLONG.getId() & 0xFF)); // type
            coded.write((byte) 2); // flag (null:1 unsigned:1)
            coded.writeInt64NoTag(8);
            coded.writeFixed64NoTag(u);
            coded.flush();
            return stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static byte[] genFloat(float f) {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.write((byte) (MySQLStandardFieldType.MYSQL_TYPE_FLOAT.getId() & 0xFF)); // type
            coded.write((byte) 0); // flag (null:1 unsigned:1)
            coded.writeInt64NoTag(4);
            coded.writeFloatNoTag(f);
            coded.flush();
            return stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static byte[] genDouble(double d) {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.write((byte) (MySQLStandardFieldType.MYSQL_TYPE_DOUBLE.getId() & 0xFF)); // type
            coded.write((byte) 0); // flag (null:1 unsigned:1)
            coded.writeInt64NoTag(8);
            coded.writeDoubleNoTag(d);
            coded.flush();
            return stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static byte[] genBoolean(boolean b) {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.write((byte) (MySQLStandardFieldType.MYSQL_TYPE_TINY.getId() & 0xFF)); // type
            coded.write((byte) 0); // flag (null:1 unsigned:1)
            coded.writeInt64NoTag(1);
            coded.write((byte) (b ? 1 : 0));
            coded.flush();
            return stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static byte[] genNull() {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.write((byte) (MySQLStandardFieldType.MYSQL_TYPE_NULL.getId() & 0xFF)); // type
            coded.write((byte) 1); // flag (null:1 unsigned:1)
            coded.writeInt64NoTag(0);
            coded.flush();
            return stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static byte[] genBytes(byte[] data, int offset, int length) {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.write((byte) (MySQLStandardFieldType.MYSQL_TYPE_STRING.getId() & 0xFF)); // type
            coded.write((byte) 0); // flag (null:1 unsigned:1)
            coded.writeInt64NoTag(length);
            coded.write(data, offset, length);
            coded.flush();
            return stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private static final BigInteger ZERO = new BigInteger("0");
    private static final BigInteger LONG_LIMIT = new BigInteger("18446744073709551616");

    public static Object gen(Object value, XSession session) {
        if (value instanceof String) {
            return genUtf8String((String) value);
        } else if (value instanceof Integer || value instanceof Long || value instanceof Short
            || value instanceof Byte) {
            return genSInt(((Number) value).longValue());
        } else if (null == value) {
            return genNull();
        } else if (value instanceof Float) {
            return genFloat((Float) value);
        } else if (value instanceof Double) {
            return genDouble((Double) value);
        } else if (value instanceof Boolean) {
            return genBoolean((Boolean) value);
        } else if (value instanceof BigInteger) {
            if (((BigInteger) value).compareTo(ZERO) >= 0 &&
                ((BigInteger) value).compareTo(LONG_LIMIT) < 0) {
                return genUInt(((BigInteger) value).longValue());
            } else {
                return genUtf8String(value.toString());
            }
        } else if (value instanceof BigDecimal) {
            return genUtf8String(value.toString());
        } else if (value instanceof Date) {
            return genUtf8String(value.toString());
        } else if (value instanceof Time) {
            return genUtf8String(value.toString());
        } else if (value instanceof Timestamp) {
            return genUtf8String(value.toString());
        } else if (value instanceof java.util.Date) {
            return genUtf8String(
                null == session ? value.toString() : session.formatTime((java.util.Date) value));
        } else if (value instanceof byte[]) {
            final byte[] val = (byte[]) value;
            return genBytes(val, 0, val.length);
        } else if (value instanceof Blob) {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                final InputStream inputStream = ((Blob) value).getBinaryStream();
                byte[] buffer = new byte[4096];
                int n = 0;
                while (-1 != (n = inputStream.read(buffer))) {
                    outputStream.write(buffer, 0, n);
                }
                final byte[] val = outputStream.toByteArray();
                return genBytes(val, 0, val.length);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        } else if (value instanceof TableName) {
            // Return the raw string.
            final String tableName = ((TableName) value).getTableName();
            if (tableName.length() >= 2 && '`' == tableName.charAt(0) && '`' == tableName
                .charAt(tableName.length() - 1)) {
                final String substring = tableName.substring(1, tableName.length() - 1);
                if (substring.indexOf('`') != -1) {
                    return substring.replaceAll("``", "`");
                }
                return substring;
            } else {
                return tableName;
            }
        } else {
            // TODO: support more type.
            throw GeneralUtil.nestedException("TODO: support more type. " + value.getClass().getName());
        }
    }

}
