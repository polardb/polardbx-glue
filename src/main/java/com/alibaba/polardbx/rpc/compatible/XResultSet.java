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

package com.alibaba.polardbx.rpc.compatible;

import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxNotice;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.BufferResultSet;
import com.alibaba.polardbx.common.utils.CaseInsensitive;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @version 1.0
 */
public class XResultSet implements BufferResultSet {

    private final XResult result;
    private final XResultSetMetaData metaData;

    public XResultSet(XResult result) {
        this.result = result;
        this.metaData = new XResultSetMetaData(result);
    }

    public XResult getXResult() {
        return result;
    }

    @Override
    public boolean next() throws SQLException {
        return result.next() != null;
    }

    @Override
    public void close() throws SQLException {
        // TODO: Any other resource release.
        // Consume all.
        while (result.next() != null) {
            ;
        }
        result.close();
    }

    private boolean lastWasNull = false;

    @Override
    public boolean wasNull() throws SQLException {
        return lastWasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        final Object obj = getObject(columnIndex);
        return null == obj ? null : obj.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        final Object obj = getObject(columnIndex);
        return obj != null && ((Number) obj).longValue() != 0;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        final Number number = (Number) getObject(columnIndex);
        return null == number ? 0 : number.byteValue();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        final Number number = (Number) getObject(columnIndex);
        return null == number ? 0 : number.shortValue();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        final Number number = (Number) getObject(columnIndex);
        return null == number ? 0 : number.intValue();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        final Number number = (Number) getObject(columnIndex);
        return null == number ? 0 : number.longValue();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        final Number number = (Number) getObject(columnIndex);
        return null == number ? 0 : number.floatValue();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        final Number number = (Number) getObject(columnIndex);
        return null == number ? 0 : number.doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        final Object obj = getObject(columnIndex);
        return obj instanceof String ? ((String) obj).getBytes() : (byte[]) obj;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return (Date) getObject(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return (Time) getObject(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return (Timestamp) getObject(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        final PolarxNotice.Warning warning = result.getWarning();
        if (warning != null) {
            return new SQLWarning(warning.getMsg(), "", warning.getCode());
        }
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        List<PolarxResultset.ColumnMetaData> meta = result.getMetaData();
        List<ByteString> data = result.current().getRow();
        if (null == data || null == meta || data.size() != meta.size()) {
            throw new SQLException("Bad result.");
        }
        if (columnIndex > meta.size()) {
            throw new SQLException("Bad columnIndex.");
        }
        try {
            Object obj = XResultUtil.resultToObject(meta.get(columnIndex - 1), data.get(columnIndex - 1), true,
                result.getSession().getDefaultTimezone()).getKey();
            lastWasNull = null == obj;
            return obj;
        } catch (Exception e) {
            throw new SQLException("Failed to read object from XResult.", e);
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    private volatile Map<String, Integer> labelCache = null;

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        try {
            List<PolarxResultset.ColumnMetaData> meta = result.getMetaData();
            if (null == meta) {
                throw new SQLException("Bad result.");
            }

            // Generate label cache if needed.
            if (null == labelCache) {
                synchronized (this) {
                    if (null == labelCache) {
                        final Map<String, Integer> tmpLabelCache =
                            new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                        for (int i = 0; i < meta.size(); ++i) {
                            final PolarxResultset.ColumnMetaData item = meta.get(i);
                            final String table = item.getTable()
                                .toString(XSession.toJavaEncoding(result.getSession().getResultMetaEncodingMySQL()));
                            final String label = item.getName()
                                .toString(XSession.toJavaEncoding(result.getSession().getResultMetaEncodingMySQL()));
                            tmpLabelCache.put(label, i + 1);
                            tmpLabelCache.put(table + "." + label, i + 1);
                        }
                        labelCache = tmpLabelCache;
                    }
                }
            }

            final Integer idx = labelCache.get(columnLabel);
            if (idx != null) {
                return idx;
            }
        } catch (Exception e) {
            throw new SQLException("Failed to find column in XResult.", e);
        }
        throw new SQLException("ResultSet column label '" + columnLabel + "' not found.", "S0022", 0);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        final Object obj = getObject(columnIndex);
        // TODO: more compatible convert.
        if (obj instanceof BigDecimal) {
            return (BigDecimal) obj;
        } else {
            return new BigDecimal(obj.toString());
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void afterLast() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean first() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean last() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getRow() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getType() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getConcurrency() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Statement getStatement() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        } else {
            throw new SQLException("not a wrapper for " + iface);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return XResultSet.class.isAssignableFrom(iface);
    }

    @Override
    public long estimateCurrentSetSize() {
        if (result != null) {
            return result.currentPipeSize();
        } else {
            return 0;
        }
    }
}
