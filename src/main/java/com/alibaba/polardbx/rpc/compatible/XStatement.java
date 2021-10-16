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
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.result.XResult;
import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxResultset;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 * ** Not thread-safe **
 */
public class XStatement implements Statement {

    protected final XConnection connection;

    public XStatement(XConnection connection) {
        this.connection = connection;
    }

    public XResult executeQueryX(String sql) throws SQLException {
        return connection.execQuery(sql);
    }

    public XResult executeQueryX(String sql, ByteString digest) throws SQLException {
        return connection.execQuery(sql, null, false, digest);
    }

    public long executeUpdateX(String sql) throws SQLException {
        return connection.execUpdate(sql);
    }

    /**
     * Compatible for JDBC statement.
     */

    private int fetchSize = 0;
    private List<String> batchSql = new ArrayList<>();

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        final XResult result = executeQueryX(sql);
        return new XResultSet(result);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        // TODO: Bad usage of multi statement in sequence.
        List<String> multi = splitMultiStatement(sql);
        if (multi.size() > 1) {
            long sum = 0;
            for (String single : multi) {
                sum += executeUpdateX(single);
            }
            return (int) sum;
        } else {
            return (int) executeUpdateX(sql);
        }
    }

    private boolean closed = false;

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        connection.setNetworkTimeout(null, seconds * 1000);
    }

    @Override
    public void cancel() throws SQLException {
        // Do nothing.
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return connection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        List<String> multi = splitMultiStatement(sql);
        XResult firstResult = null;
        List<XResult> results = new ArrayList<>();
        for (int i = 0; i < multi.size(); ++i) {
            XResult result = connection.execQuery(multi.get(i), null, false);
            if (null == firstResult) {
                firstResult = result;
            }
            results.add(result);
        }
        if (null == firstResult) {
            return false;
        }
        // Should consume all.
        for (XResult result : results) {
            while (result.next() != null) {
                ;
            }
        }
        List<PolarxResultset.ColumnMetaData> metaData = firstResult.getMetaData();
        return metaData != null && metaData.size() != 0;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
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
        fetchSize = rows;
        connection.setStreamMode(rows == Integer.MIN_VALUE);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        batchSql.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        batchSql.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        final XResult[] results = new XResult[batchSql.size()];
        final int[] affecteds = new int[batchSql.size()];
        // First queued and then run.
        int prev_done = 0;
        for (int i = 0; i < batchSql.size(); ++i) {
            // Only block and wait result on last query.
            if (XConfig.GALAXY_X_PROTOCOL) {
                // Pipeline not supported now.
                results[i] = connection.execUpdate(batchSql.get(i), null, false);
            } else {
                results[i] = connection.execUpdate(batchSql.get(i), null, i != batchSql.size() - 1);
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
        for (int i = 0; i < batchSql.size(); ++i) {
            affecteds[i] = (int) results[i].getRowsAffected();
        }
        return affecteds;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        final ArrayResultSet resultSet = new ArrayResultSet();
        resultSet.getColumnName().add("GENERATED_KEY");
        final XResult last = connection.getLastUserRequest();
        if (last != null && last.isHaveGeneratedInsertId()) {
            resultSet.getRows().add(new Object[] {last.getGeneratedInsertId()});
        }
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
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
        return XStatement.class.isAssignableFrom(iface);
    }

    /**
     * Useful function.
     */
    private List<String> splitMultiStatement(String sql) {
        // Dealing sql.
        final int startPos = XPreparedStatement.findStartOfStatement(sql);
        final int statementLength = sql.length();
        final char quotedIdentifierChar = '`';

        boolean inQuotes = false;
        char quoteChar = 0;
        boolean inQuotedId = false;
        int start_pos = 0;
        List<String> result = new ArrayList<>();

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

            if ((c == ';') && !inQuotes && !inQuotedId) {
                // More statement.
                result.add(sql.substring(start_pos, i));
                start_pos = i + 1;
            }
        }
        if (start_pos < statementLength) {
            result.add(sql.substring(start_pos, statementLength));
        }
        return result;
    }
}
