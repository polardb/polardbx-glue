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

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.jdbc.TableName;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.rpc.client.XSession;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @version 1.0
 */
public class GP57Util {
    private final int typeOffset;
    private final byte[] header;
    private final Object[] params;

    public GP57Util(int paramCnt) {
        typeOffset = (paramCnt + 7) / 8 + 1; // null bits & type flag
        header = new byte[typeOffset + 2 * paramCnt];
        params = new Object[paramCnt];
    }

    public void reset() {
        Arrays.fill(header, (byte) 0);
        Arrays.fill(params, null);
    }

    private static void writeLength(CodedOutputStream coded, int length) throws IOException {
        if (length < 251) {
            coded.write((byte) length);
        } else if (length < 65536) {
            coded.write((byte) 252);
            coded.write((byte) (length & 0xFF));
            coded.write((byte) ((length >>> 8) & 0xFF));
        } else if (length < 16777216) {
            coded.write((byte) 253);
            coded.write((byte) (length & 0xFF));
            coded.write((byte) ((length >>> 8) & 0xFF));
            coded.write((byte) ((length >>> 16) & 0xFF));
        } else {
            coded.write((byte) 254);
            coded.writeInt64NoTag(length);
        }
    }

    public void setTiny(int idx, int i, boolean unsigned) {
        try {
            // skip null bitmap
            header[typeOffset + 2 * idx] = (byte) (MySQLStandardFieldType.MYSQL_TYPE_TINY.getId() & 0xFF); // type
            header[typeOffset + 2 * idx + 1] = (byte) (unsigned ? 0x80 : 0); // flag (unused:7 unsigned:1)

            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.write((byte) i);
            coded.flush();
            params[idx] = stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void setShort(int idx, int i, boolean unsigned) {
        try {
            // skip null bitmap
            header[typeOffset + 2 * idx] = (byte) (MySQLStandardFieldType.MYSQL_TYPE_SHORT.getId() & 0xFF); // type
            header[typeOffset + 2 * idx + 1] = (byte) (unsigned ? 0x80 : 0); // flag (unused:7 unsigned:1)

            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.write((byte) (i & 0xFF));
            coded.write((byte) ((i >>> 8) & 0xFF));
            coded.flush();
            params[idx] = stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void setInt32(int idx, int i, boolean unsigned) {
        try {
            // skip null bitmap
            header[typeOffset + 2 * idx] = (byte) (MySQLStandardFieldType.MYSQL_TYPE_LONG.getId() & 0xFF); // type
            header[typeOffset + 2 * idx + 1] = (byte) (unsigned ? 0x80 : 0); // flag (unused:7 unsigned:1)

            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.writeFixed32NoTag(i);
            coded.flush();
            params[idx] = stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void setInt64(int idx, long l, boolean unsigned) {
        try {
            // skip null bitmap
            header[typeOffset + 2 * idx] = (byte) (MySQLStandardFieldType.MYSQL_TYPE_LONGLONG.getId() & 0xFF); // type
            header[typeOffset + 2 * idx + 1] = (byte) (unsigned ? 0x80 : 0); // flag (unused:7 unsigned:1)

            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.writeFixed64NoTag(l);
            coded.flush();
            params[idx] = stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void setFloat(int idx, float f) {
        try {
            // skip null bitmap
            header[typeOffset + 2 * idx] = (byte) (MySQLStandardFieldType.MYSQL_TYPE_FLOAT.getId() & 0xFF); // type
            header[typeOffset + 2 * idx + 1] = (byte) 0; // flag (unused:7 unsigned:1)

            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.writeFloatNoTag(f);
            coded.flush();
            params[idx] = stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void setDouble(int idx, double d) {
        try {
            // skip null bitmap
            header[typeOffset + 2 * idx] = (byte) (MySQLStandardFieldType.MYSQL_TYPE_DOUBLE.getId() & 0xFF); // type
            header[typeOffset + 2 * idx + 1] = (byte) 0; // flag (unused:7 unsigned:1)

            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.writeDoubleNoTag(d);
            coded.flush();
            params[idx] = stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void setDecimal(int idx, BigDecimal v) {
        try {
            // skip null bitmap
            header[typeOffset + 2 * idx] = (byte) (MySQLStandardFieldType.MYSQL_TYPE_NEWDECIMAL.getId() & 0xFF); // type
            header[typeOffset + 2 * idx + 1] = (byte) 0; // flag (unused:7 unsigned:1)

            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            final byte[] bytes = v.toString().getBytes(StandardCharsets.UTF_8);
            writeLength(coded, bytes.length);
            coded.write(bytes, 0, bytes.length);
            coded.flush();
            params[idx] = stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void setString(int idx, String s, String charset) {
        try {
            // skip null bitmap
            header[typeOffset + 2 * idx] = (byte) (MySQLStandardFieldType.MYSQL_TYPE_STRING.getId() & 0xFF); // type
            header[typeOffset + 2 * idx + 1] = (byte) 0; // flag (unused:7 unsigned:1)

            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            final byte[] bytes = s.getBytes(charset);
            writeLength(coded, bytes.length);
            coded.write(bytes, 0, bytes.length);
            coded.flush();
            params[idx] = stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void setString(int idx, String s, Charset charset) {
        try {
            // skip null bitmap
            header[typeOffset + 2 * idx] = (byte) (MySQLStandardFieldType.MYSQL_TYPE_STRING.getId() & 0xFF); // type
            header[typeOffset + 2 * idx + 1] = (byte) 0; // flag (unused:7 unsigned:1)

            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            final byte[] bytes = s.getBytes(charset);
            writeLength(coded, bytes.length);
            coded.write(bytes, 0, bytes.length);
            coded.flush();
            params[idx] = stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void setBoolean(int idx, boolean b) {
        try {
            // skip null bitmap
            header[typeOffset + 2 * idx] = (byte) (MySQLStandardFieldType.MYSQL_TYPE_TINY.getId() & 0xFF); // type
            header[typeOffset + 2 * idx + 1] = (byte) 0; // flag (unused:7 unsigned:1)

            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            coded.write((byte) (b ? 1 : 0));
            coded.flush();
            params[idx] = stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void setNull(int idx) {
        try {
            header[idx >>> 3] |= 1 << (idx & 7);
            header[typeOffset + 2 * idx] = (byte) (MySQLStandardFieldType.MYSQL_TYPE_NULL.getId() & 0xFF); // type
            header[typeOffset + 2 * idx + 1] = (byte) 0; // flag (unused:7 unsigned:1)
            params[idx] = null;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void setBytes(int idx, byte[] data, int offset, int length) {
        try {
            // skip null bitmap
            header[typeOffset + 2 * idx] = (byte) (MySQLStandardFieldType.MYSQL_TYPE_STRING.getId() & 0xFF); // type
            header[typeOffset + 2 * idx + 1] = (byte) 0; // flag (unused:7 unsigned:1)

            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(stream);
            writeLength(coded, length);
            coded.write(data, offset, length);
            coded.flush();
            params[idx] = stream.toByteArray();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private static final BigInteger ZERO = new BigInteger("0");
    private static final BigInteger LONG_LIMIT = new BigInteger("18446744073709551616");

    public void set(int idx, Object value, XSession session) {
        if (value instanceof String) {
            if (null == session) {
                setString(idx, (String) value, StandardCharsets.UTF_8);
            } else {
                setString(idx, (String) value, XSession.toJavaEncoding(session.getRequestEncodingMySQL()));
            }
        } else if (value instanceof Long) {
            setInt64(idx, (Long) value, false);
        } else if (value instanceof Integer) {
            setInt32(idx, (Integer) value, false);
        } else if (value instanceof Short) {
            setShort(idx, (Short) value, false);
        } else if (value instanceof Byte) {
            setTiny(idx, (Byte) value, false);
        } else if (null == value) {
            setNull(idx);
        } else if (value instanceof Float) {
            setFloat(idx, (Float) value);
        } else if (value instanceof Double) {
            setDouble(idx, (Double) value);
        } else if (value instanceof Boolean) {
            setBoolean(idx, (Boolean) value);
        } else if (value instanceof BigInteger) {
            if (((BigInteger) value).compareTo(ZERO) >= 0 &&
                ((BigInteger) value).compareTo(LONG_LIMIT) < 0) {
                setInt64(idx, ((BigInteger) value).longValue(), true);
            } else {
                setString(idx, value.toString(), StandardCharsets.UTF_8);
            }
        } else if (value instanceof BigDecimal) {
            setDecimal(idx, (BigDecimal) value);
        } else if (value instanceof Date) {
            setString(idx, value.toString(), StandardCharsets.UTF_8);
        } else if (value instanceof Time) {
            setString(idx, value.toString(), StandardCharsets.UTF_8);
        } else if (value instanceof Timestamp) {
            setString(idx, value.toString(), StandardCharsets.UTF_8);
        } else if (value instanceof java.util.Date) {
            setString(idx, null == session ? value.toString() : session.formatTime((java.util.Date) value),
                StandardCharsets.UTF_8);
        } else if (value instanceof byte[]) {
            final byte[] val = (byte[]) value;
            setBytes(idx, val, 0, val.length);
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
                setBytes(idx, val, 0, val.length);
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
                    params[idx] = substring.replaceAll("``", "`");
                } else {
                    params[idx] = substring;
                }
            } else {
                params[idx] = tableName;
            }
        } else if (value instanceof UInt64) {
            setInt64(idx, ((UInt64) value).longValue(), true);
        } else {
            // TODO: support more type.
            throw GeneralUtil.nestedException("TODO: support more type. " + value.getClass().getName());
        }
    }

    public GPParam genParam() {
        // probe table count
        int tableCount = 0;
        for (Object o : params) {
            if (o instanceof String) {
                ++tableCount;
            }
        }

        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            if (0 == tableCount) {
                // just concat bytes
                header[typeOffset - 1] = 1; // enable type array
                stream.write(header);
                for (Object o : params) {
                    assert o instanceof byte[] || null == o; // data or null
                    if (o != null) {
                        stream.write((byte[]) o);
                    }
                }
            } else {
                final int newParamCnt = params.length - tableCount;
                if (newParamCnt > 0) {
                    final int newTypeOffset = (newParamCnt + 7) / 8 + 1; // null bits & type flag
                    final byte[] newHeader = new byte[newTypeOffset + 2 * newParamCnt];
                    int newIdx = 0;
                    for (int idx = 0; idx < params.length; ++idx) {
                        final Object param = params[idx];
                        if (!(param instanceof String)) {
                            assert param instanceof byte[] || null == param; // data or null
                            if ((header[idx >>> 3] & (1 << (idx & 7))) != 0) {
                                newHeader[newIdx >>> 3] |= 1 << (newIdx & 7); // set null flag
                            }
                            newHeader[newTypeOffset + 2 * newIdx] = header[typeOffset + 2 * idx];
                            newHeader[newTypeOffset + 2 * newIdx + 1] = header[typeOffset + 2 * idx + 1];
                            ++newIdx;
                        }
                    }
                    assert newIdx == newParamCnt;

                    // concat new bytes
                    newHeader[newTypeOffset - 1] = 1; // enable type array
                    stream.write(newHeader);
                    for (Object o : params) {
                        if (!(o instanceof String)) {
                            assert o instanceof byte[] || null == o;
                            if (o != null) {
                                stream.write((byte[]) o);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }

        if (0 == tableCount) {
            return new GPParam(Collections.emptyList(),
                0 == stream.size() ? ByteString.EMPTY : ByteString.copyFrom(stream.toByteArray()), params.length);
        }

        List<GPTable> tables = new ArrayList<>(tableCount);
        for (int idx = 0; idx < params.length; ++idx) {
            final Object param = params[idx];
            if (param instanceof String) {
                tables.add(new GPTable(idx + 1, (String) param)); // start from 1
            }
        }
        assert tables.size() == tableCount;
        return new GPParam(tables, 0 == stream.size() ? ByteString.EMPTY : ByteString.copyFrom(stream.toByteArray()),
            params.length - tableCount);
    }
}
