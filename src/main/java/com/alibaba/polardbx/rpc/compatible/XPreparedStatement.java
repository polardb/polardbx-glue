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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.XUtil;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.result.XResult;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.mysql.cj.x.protobuf.PolarxDatatypes;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 */
public class XPreparedStatement extends XStatement implements PreparedStatement {

    private final String sql;
    private final Map<Integer, Integer> placeHolderPos = new HashMap<>();

    private final PolarxDatatypes.Any[] params;

    // Batch param.
    private final List<PolarxDatatypes.Any[]> batch = new ArrayList<>();

    public XPreparedStatement(XConnection connection, String sql) {
        super(connection);
        this.sql = sql;

        // Dealing sql.
        final int startPos = findStartOfStatement(sql);
        final int statementLength = sql.length();
        final char quotedIdentifierChar = '`';

        boolean inQuotes = false;
        char quoteChar = 0;
        boolean inQuotedId = false;

        for (int i = startPos; i < statementLength; ++i) {
            char c = sql.charAt(i);

            if (c == '\\' && i < (statementLength - 1)) {
                i++;
                continue; // next character is escaped
            }

            // are we in a quoted identifier? (only valid when the id is not inside a 'string')
            if (!inQuotes && (c == quotedIdentifierChar)) {
                inQuotedId = !inQuotedId;
            } else if (!inQuotedId) {
                //	only respect quotes when not in a quoted identifier

                if (inQuotes) {
                    if (((c == '\'') || (c == '"')) && c == quoteChar) {
                        if (i < (statementLength - 1) && sql.charAt(i + 1) == quoteChar) {
                            i++;
                            continue; // inline quote escape
                        }

                        inQuotes = false;
                        quoteChar = 0;
                    }
                } else {
                    if (c == '#' || (c == '-' && (i + 1) < statementLength && sql.charAt(i + 1) == '-')) {
                        // run out to end of statement, or newline, whichever comes first
                        int endOfStmt = statementLength - 1;

                        for (; i < endOfStmt; i++) {
                            c = sql.charAt(i);

                            if (c == '\r' || c == '\n') {
                                break;
                            }
                        }

                        continue;
                    } else if (c == '/' && (i + 1) < statementLength) {
                        // Comment?
                        char cNext = sql.charAt(i + 1);

                        if (cNext == '*') {
                            i += 2;

                            for (int j = i; j < statementLength; j++) {
                                i++;
                                cNext = sql.charAt(j);

                                if (cNext == '*' && (j + 1) < statementLength) {
                                    if (sql.charAt(j + 1) == '/') {
                                        i++;

                                        if (i < statementLength) {
                                            c = sql.charAt(i);
                                        }

                                        break; // comment done
                                    }
                                }
                            }
                        }
                    } else if ((c == '\'') || (c == '"')) {
                        inQuotes = true;
                        quoteChar = c;
                    }
                }
            }

            if ((c == '?') && !inQuotes && !inQuotedId) {
                // Placeholder.
                placeHolderPos.put(placeHolderPos.size(), i);
            }
        }
        this.params = new PolarxDatatypes.Any[placeHolderPos.size()];
    }

    private static String surroundWithBacktick(String identifier) {
        if (identifier.contains("`")) {
            return "`" + identifier.replaceAll("`", "``") + "`";
        }
        return "`" + identifier + "`";
    }

    public Pair<String, List<PolarxDatatypes.Any>> reorganizeParam() {
        return reorganizeParam(params);
    }

    private Pair<String, List<PolarxDatatypes.Any>> reorganizeParam(PolarxDatatypes.Any[] params) {
        // Fixed.
        return new Pair<>(sql, ImmutableList.copyOf(params));
//        final List<String> identifiers = new ArrayList<>();
//        final List<Integer> pos = new ArrayList<>();
//        final List<PolarxDatatypes.Any> newParam = new ArrayList<>(params.length);
//        for (int i = 0; i < params.length; ++i) {
//            if (params[i].getType() == PolarxDatatypes.Any.Type.SCALAR &&
//                params[i].getScalar().getType() == PolarxDatatypes.Scalar.Type.V_IDENTIFIER) {
//                final String s = params[i].getScalar().getVIdentifier().getValue().toStringUtf8();
//                identifiers.add(s);
//                pos.add(placeHolderPos.get(i));
//            } else {
//                newParam.add(params[i]);
//            }
//        }
//
//        final String newSql;
//        if (!identifiers.isEmpty()) {
//            final StringBuilder builder = new StringBuilder();
//            assert identifiers.size() == pos.size();
//            for (int i = 0; i < pos.size(); ++i) {
//                builder.append(sql, 0 == i ? 0 : pos.get(i - 1) + 1, pos.get(i));
//                builder.append(surroundWithBacktick(identifiers.get(i)));
//            }
//            builder.append(sql, pos.get(pos.size() - 1) + 1, sql.length());
//            newSql = builder.toString();
//        } else {
//            newSql = sql;
//        }
//        return new Pair<>(newSql, newParam);
    }

    public XResult executeQueryX() throws SQLException {
        final Pair<String, List<PolarxDatatypes.Any>> newParam = reorganizeParam();
        return connection.execQuery(newParam.getKey(), newParam.getValue());
    }

    public XResult executeQueryX(ByteString digest) throws SQLException {
        final Pair<String, List<PolarxDatatypes.Any>> newParam = reorganizeParam();
        return connection.execQuery(newParam.getKey(), newParam.getValue(), false, digest);
    }

    public long executeUpdateX() throws SQLException {
        final Pair<String, List<PolarxDatatypes.Any>> newParam = reorganizeParam();
        return connection.execUpdate(newParam.getKey(), newParam.getValue());
    }

    public long executeUpdateX(ByteString digest) throws SQLException {
        final Pair<String, List<PolarxDatatypes.Any>> newParam = reorganizeParam();
        return connection.execUpdate(newParam.getKey(), newParam.getValue(), false, digest).getRowsAffected();
    }

    public XResult executeUpdateReturningX(String returning) throws SQLException {
        // execute INSERT/UPDATE/DELETE then return rows affected
        final Pair<String, List<PolarxDatatypes.Any>> newParam = reorganizeParam();
        return connection.execUpdateReturning(newParam.getKey(), newParam.getValue(), returning);
    }

    /**
     * Compatible for JDBC prepared statement.
     */

    private void setParam(int idx, PolarxDatatypes.Any param) {
        final int my_idx = idx - 1;
        params[my_idx] = param; // Use original array out of bound exception like JDBC.
    }

    @Override
    public void close() throws SQLException {
        // TODO: Any other resource release.
        Arrays.fill(params, null);
        batch.clear();
        super.close();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        final XResult result = executeQueryX();
        return new XResultSet(result);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return (int) executeUpdateX();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParam(parameterIndex, XUtil.genAny(XUtil.genNullScalar()));
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParam(parameterIndex, XUtil.genAny(XUtil.genBooleanScalar(x)));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParam(parameterIndex, XUtil.genAny(XUtil.genSIntScalar(x)));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParam(parameterIndex, XUtil.genAny(XUtil.genSIntScalar(x)));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParam(parameterIndex, XUtil.genAny(XUtil.genSIntScalar(x)));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParam(parameterIndex, XUtil.genAny(XUtil.genSIntScalar(x)));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParam(parameterIndex, XUtil.genAny(XUtil.genFloatScalar(x)));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParam(parameterIndex, XUtil.genAny(XUtil.genDoubleScalar(x)));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (null == x) {
            setParam(parameterIndex, XUtil.genAny(XUtil.genNullScalar()));
            return;
        }
        setParam(parameterIndex, XUtil.genAny(XUtil.genUtf8StringScalar(x.toPlainString())));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        if (null == x) {
            setParam(parameterIndex, XUtil.genAny(XUtil.genNullScalar()));
            return;
        }
        setParam(parameterIndex, XUtil.genAny(XUtil.genUtf8StringScalar(x)));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        if (null == x) {
            setParam(parameterIndex, XUtil.genAny(XUtil.genNullScalar()));
            return;
        }
        setParam(parameterIndex, XUtil.genAny(XUtil.genOctetsScalar(ByteBuffer.wrap(x))));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (null == x) {
            setParam(parameterIndex, XUtil.genAny(XUtil.genNullScalar()));
            return;
        }
        setParam(parameterIndex, XUtil.genAny(XUtil.genUtf8StringScalar(x.toString())));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (null == x) {
            setParam(parameterIndex, XUtil.genAny(XUtil.genNullScalar()));
            return;
        }
        setParam(parameterIndex, XUtil.genAny(XUtil.genUtf8StringScalar(x.toString())));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (null == x) {
            setParam(parameterIndex, XUtil.genAny(XUtil.genNullScalar()));
            return;
        }
        setParam(parameterIndex, XUtil.genAny(XUtil.genUtf8StringScalar(x.toString())));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void clearParameters() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setParam(parameterIndex, XUtil.genAny(XUtil.genScalar(x, connection.getSession())));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        setParam(parameterIndex, XUtil.genAny(XUtil.genScalar(x, connection.getSession())));
    }

    @Override
    public boolean execute() throws SQLException {
        XResult result = executeQueryX();
        List<PolarxResultset.ColumnMetaData> metaData = result.getMetaData();
        return metaData != null && metaData.size() != 0;
    }

    @Override
    public void addBatch() throws SQLException {
        batch.add(params.clone());
    }

    @Override
    public void clearBatch() throws SQLException {
        batch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        final XResult[] results = new XResult[batch.size()];
        final int[] affected = new int[batch.size()];
        int prev_done = 0;
        for (int i = 0; i < batch.size(); ++i) {
            final PolarxDatatypes.Any[] paramPair = batch.get(i);
            // Only block and wait result on last query.
            final Pair<String, List<PolarxDatatypes.Any>> newParam = reorganizeParam(paramPair);
            if (XConfig.GALAXY_X_PROTOCOL) {
                // Pipeline not supported now.
                results[i] = connection.execUpdate(newParam.getKey(), newParam.getValue(), false);
            } else {
                results[i] = connection.execUpdate(newParam.getKey(), newParam.getValue(), i != batch.size() - 1);
                results[i].setFatalOnIgnorable(false); // Set not fatal on previous execution(special ignorable).
                if (i - prev_done > XConfig.MAX_QUEUED_BATCH_REQUEST) {
                    prev_done = i;
                    connection.flushNetwork();
                    while (results[i].next() != null) {
                        // Consume all.
                    }
                }
            }
        }
        for (int i = 0; i < batch.size(); ++i) {
            affected[i] = (int) results[i].getRowsAffected();
        }
        return affected;
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        if (null == x) {
            setParam(parameterIndex, XUtil.genAny(XUtil.genNullScalar()));
            return;
        }

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            final InputStream inputStream = x.getBinaryStream();
            byte[] buffer = new byte[4096];
            int n = 0;
            while (-1 != (n = inputStream.read(buffer))) {
                outputStream.write(buffer, 0, n);
            }
        } catch (Exception e) {
            throw new SQLException("XPreparedStatement setBlob error.", e);
        }
        setParam(parameterIndex, XUtil.genAny(XUtil.genOctetsScalar(ByteBuffer.wrap(outputStream.toByteArray()))));
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        if (null == x) {
            setParam(parameterIndex, XUtil.genAny(XUtil.genNullScalar()));
            return;
        }

        final StringBuilder builder = new StringBuilder();
        try {
            final Reader inputStream = x.getCharacterStream();
            char[] buffer = new char[2048];
            int n = 0;
            while (-1 != (n = inputStream.read(buffer))) {
                builder.append(buffer, 0, n);
            }
        } catch (Exception e) {
            throw new SQLException("XPreparedStatement setClob error.", e);
        }
        setParam(parameterIndex, XUtil.genAny(XUtil.genUtf8StringScalar(builder.toString())));
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new NotSupportException();
    }

    /**
     * Useful function.
     */

    public static boolean startsWithIgnoreCase(String searchIn, int startAt, String searchFor) {
        return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
    }

    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor, int beginPos) {
        if (searchIn == null) {
            return searchFor == null;
        }

        int inLength = searchIn.length();

        for (; beginPos < inLength; beginPos++) {
            if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                break;
            }
        }

        return startsWithIgnoreCase(searchIn, beginPos, searchFor);
    }

    public static int findStartOfStatement(String sql) {
        int statementStartPos = 0;

        if (startsWithIgnoreCaseAndWs(sql, "/*", 0)) {
            statementStartPos = sql.indexOf("*/");

            if (statementStartPos == -1) {
                statementStartPos = 0;
            } else {
                statementStartPos += 2;
            }
        } else if (startsWithIgnoreCaseAndWs(sql, "--", 0) || startsWithIgnoreCaseAndWs(sql, "#", 0)) {
            statementStartPos = sql.indexOf('\n');

            if (statementStartPos == -1) {
                statementStartPos = sql.indexOf('\r');

                if (statementStartPos == -1) {
                    statementStartPos = 0;
                }
            }
        }

        return statementStartPos;
    }
}
