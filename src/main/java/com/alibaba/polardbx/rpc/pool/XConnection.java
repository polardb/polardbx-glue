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

package com.alibaba.polardbx.rpc.pool;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.XLog;
import com.alibaba.polardbx.rpc.client.XClient;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.compatible.XPreparedStatement;
import com.alibaba.polardbx.rpc.compatible.XStatement;
import com.alibaba.polardbx.rpc.result.XResult;
import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxNotice;
import com.mysql.cj.x.protobuf.PolarxDatatypes;
import com.mysql.cj.x.protobuf.PolarxExecPlan;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * @version 1.0
 * thread-safe
 */
public class XConnection implements AutoCloseable, Connection {

    private XSession session; // Null if closed.
    private boolean initialized = false;

    // Connection & request mode.
    private boolean streamMode = false;
    private boolean compactMetadata = false;
    private boolean withFeedback = false;

    // For stream control.
    private int defaultTokenCount = XConnectionManager.getInstance().getDefaultQueryToken();

    private String traceId = null;

    // Creation time info.
    private long connectNano = 0;
    private long waitNano = 0;

    // For belonging.
    private XDataSource dataSource = null;

    public XConnection(XSession session) {
        this.session = session;
        if (XConnectionManager.getInstance().isEnableTrxLeakCheck()) {
            this.session.recordStack();
        }
    }

    public synchronized void init() throws SQLException {
        if (!initialized) {
            // Reset to autocommit.
            session.setAutoCommit(this, true);
            initialized = true;
        }
    }

    // Must hold the lock.
    private void check() throws SQLException {
        if (null == session) {
            throw new SQLException(this + " closed.", "Closed.");
        } else if (!initialized) {
            throw new SQLException(this + " not initialized.", "Not initialized.");
        }
    }

    @Override
    public synchronized void close() {
        if (session != null) {
            try {
                // Transaction safety insurance.
                if (!session.isAutoCommit()) {
                    execUpdate("rollback", null, true); // Rollback in case of any uncommitted trx.
                }
                session.flushIgnorable(this);
                final XResult lastRequest = session.getLastRequest();
                if (lastRequest != null && !lastRequest.isGoodAndDone()) {
                    XLog.XLogLogger.error(this + " last req unclosed. " + lastRequest.getSql());
                }
            } catch (Throwable e) {
                session.setLastException(e);
                XLog.XLogLogger.error(e);
            } finally {
                final XClient parent = session.getClient();
                try {
                    if (!parent.reuseSession(session)) {
                        parent.dropSession(session);
                    }
                } catch (Throwable e) {
                    XLog.XLogLogger.error(this + " failed to check session.");
                    XLog.XLogLogger.error(e);
                    try {
                        parent.dropSession(session);
                    } catch (Exception e1) {
                        XLog.XLogLogger.error(this + " failed to drop session.");
                        XLog.XLogLogger.error(e);
                    }
                } finally {
                    session = null;
                }
            }
        }
    }

    public XSession getSessionUncheck() {
        return session;
    }

    public synchronized XSession getSession() throws SQLException {
        check();
        return session;
    }

    // No synchronized for performance.

    public boolean isStreamMode() {
        return streamMode;
    }

    public void setStreamMode(boolean streamMode) {
        this.streamMode = streamMode;
    }

    public boolean isCompactMetadata() {
        return compactMetadata;
    }

    public void setCompactMetadata(boolean compactMetadata) {
        this.compactMetadata = compactMetadata;
    }

    public boolean isWithFeedback() {
        return withFeedback;
    }

    public void setWithFeedback(boolean withFeedback) {
        this.withFeedback = withFeedback;
    }

    public int getDefaultTokenCount() {
        return defaultTokenCount;
    }

    public void setDefaultTokenCount(int defaultTokenCount) {
        this.defaultTokenCount = defaultTokenCount;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public long getConnectNano() {
        return connectNano;
    }

    public void setConnectNano(long connectNano) {
        this.connectNano = connectNano;
    }

    public long getWaitNano() {
        return waitNano;
    }

    public void setWaitNano(long waitNano) {
        this.waitNano = waitNano;
    }

    public XDataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(XDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void kill() throws SQLException {
        kill(true, true);
    }

    public synchronized void kill(boolean pushKilled, boolean withClose) throws SQLException {
        check();
        session.kill(pushKilled);
        if (withClose) {
            close();
        }
    }

    public void setLazyCtsTransaction() throws SQLException {
        check();
        session.setLazyCtsTransaction();
    }

    public void setLazySnapshotSeq(long lazySnapshotSeq) throws SQLException {
        check();
        session.setLazySnapshotSeq(lazySnapshotSeq);
    }

    public void setLazyCommitSeq(long lazyCommitSeq) throws SQLException {
        check();
        session.setLazyCommitSeq(lazyCommitSeq);
    }

    public synchronized XResult execQuery(PolarxExecPlan.ExecPlan.Builder execPlan, String nativeSql)
        throws SQLException {
        check();
        return session.execQuery(this, execPlan, nativeSql, false);
    }

    public synchronized XResult execQuery(PolarxExecPlan.ExecPlan.Builder execPlan, String nativeSql,
                                          boolean ignoreResult) throws SQLException {
        check();
        return session.execQuery(this, execPlan, nativeSql, ignoreResult);
    }

    public synchronized XResult execQuery(String sql) throws SQLException {
        check();
        return session.execQuery(this, sql, null, false, null);
    }

    public synchronized XResult execQuery(String sql, List<PolarxDatatypes.Any> args) throws SQLException {
        check();
        return session.execQuery(this, sql, args, false, null);
    }

    public synchronized XResult execQuery(String sql, List<PolarxDatatypes.Any> args, boolean ignoreResult)
        throws SQLException {
        check();
        return session.execQuery(this, sql, args, ignoreResult, null);
    }

    public synchronized XResult execQuery(String sql, List<PolarxDatatypes.Any> args, boolean ignoreResult,
                                          ByteString digest)
        throws SQLException {
        check();
        return session.execQuery(this, sql, args, ignoreResult, digest);
    }

    public synchronized long execUpdate(String sql) throws SQLException {
        check();
        return session.execUpdate(this, sql, null, false, null).getRowsAffected();
    }

    public synchronized long execUpdate(String sql, List<PolarxDatatypes.Any> args) throws SQLException {
        check();
        return session.execUpdate(this, sql, args, false, null).getRowsAffected();
    }

    public synchronized XResult execUpdate(String sql, List<PolarxDatatypes.Any> args, boolean ignoreResult)
        throws SQLException {
        check();
        return session.execUpdate(this, sql, args, ignoreResult, null);
    }

    public synchronized XResult execUpdate(String sql, List<PolarxDatatypes.Any> args, boolean ignoreResult,
                                           ByteString digest)
        throws SQLException {
        check();
        return session.execUpdate(this, sql, args, ignoreResult, digest);
    }

    public synchronized XResult execUpdateReturning(String sql, List<PolarxDatatypes.Any> args, String returning)
        throws SQLException {
        check();
        return session.execQuery(this, sql, args, false, null, returning);
    }

    public synchronized long getTSO(int count) throws SQLException {
        check();
        return session.getTSO(this, count);
    }

    public synchronized void flushNetwork() throws SQLException {
        check();
        session.flushNetwork();
    }

    public synchronized boolean supportMessageTimestamp() throws SQLException {
        check();
        return session.supportMessageTimestamp();
    }

    public synchronized boolean supportSingleShardOptimization() throws SQLException {
        check();
        return session.supportSingleShardOptimization();
    }

    public long getConnectionId() throws SQLException {
        check();
        return session.getConnectionId(this);
    }

    public synchronized XResult getLastUserRequest() throws SQLException {
        check();
        return session.getLastUserRequest();
    }

    public synchronized void cancel() throws SQLException {
        check();
        session.cancel();
    }

    public synchronized Throwable getLastException() throws SQLException {
        check();
        return session.getLastException();
    }

    public synchronized Throwable setLastException(Throwable lastException) throws SQLException {
        check();
        return session.setLastException(lastException);
    }

    public synchronized void tokenOffer() throws SQLException {
        check();
        session.tokenOffer(defaultTokenCount);
    }

    public synchronized void setDefaultDB(String defaultDB) throws SQLException {
        check();
        session.setDefaultDB(defaultDB);
    }

    public synchronized void setSessionVariables(Map<String, Object> newServerVariables)
        throws SQLException {
        check();
        session.setSessionVariables(this, newServerVariables);
    }

    public synchronized void setGlobalVariables(Map<String, Object> newGlobalVariables) throws SQLException {
        check();
        session.setGlobalVariables(this, newGlobalVariables);
    }

    /**
     * Compatible for JDBC connection.
     */

    private Executor networkTimeoutExecutor = null;
    private long networkTimeoutNanos = 0;
    private boolean autoCommit = true;

    public long actualTimeoutNanos() {
        long t = networkTimeoutNanos;
        return 0 == t ? XConfig.DEFAULT_TIMEOUT_NANOS : t;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new XStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new XPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public synchronized void setAutoCommit(boolean autoCommit) throws SQLException {
        check();
        session.setAutoCommit(this, autoCommit);
        this.autoCommit = autoCommit;
    }

    @Override
    public synchronized boolean getAutoCommit() throws SQLException {
        return this.autoCommit;
    }

    @Override
    public synchronized void commit() throws SQLException {
        check();
        execUpdate("commit");
    }

    @Override
    public synchronized void rollback() throws SQLException {
        check();
        execUpdate("rollback");
    }

    @Override
    public synchronized boolean isClosed() throws SQLException {
        return null == session;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public synchronized void setTransactionIsolation(int level) throws SQLException {
        check();
        if (level == session.getIsolation()) {
            return;
        }
        final String sql, levelString;
        switch (level) {
        case Connection.TRANSACTION_READ_UNCOMMITTED:
            sql = "set session transaction isolation level read uncommitted";
            levelString = "READ-UNCOMMITTED";
            break;

        case Connection.TRANSACTION_READ_COMMITTED:
            sql = "set session transaction isolation level read committed";
            levelString = "READ-COMMITTED";
            break;

        case Connection.TRANSACTION_REPEATABLE_READ:
            sql = "set session transaction isolation level repeatable read";
            levelString = "REPEATABLE-READ";
            break;

        case Connection.TRANSACTION_SERIALIZABLE:
            sql = "set session transaction isolation level serializable";
            levelString = "SERIALIZABLE";
            break;

        default:
            throw new SQLException("Unknown transaction isolation level: " + level);
        }
        getSession().stashTransactionSequence();
        try {
            execUpdate(sql, null, true); // Set ignore is ok, drop connection when fail.
            session.updateIsolation(levelString);
        } finally {
            getSession().stashPopTransactionSequence();
        }
    }

    @Override
    public synchronized int getTransactionIsolation() throws SQLException {
        check();
        return session.getIsolation();
    }

    @Override
    public synchronized SQLWarning getWarnings() throws SQLException {
        check();
        final XResult last = session.getLastUserRequest();
        final PolarxNotice.Warning warning = null == last ? null : last.getWarning();
        return null == warning ? null : new SQLWarning(warning.getMsg(), "", warning.getCode());
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        if (ResultSet.TYPE_FORWARD_ONLY == resultSetType && ResultSet.CONCUR_READ_ONLY == resultSetConcurrency) {
            return new XPreparedStatement(this, sql);
        }
        throw new NotSupportException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (Statement.RETURN_GENERATED_KEYS == autoGeneratedKeys) {
            return new XPreparedStatement(this, sql);
        } else {
            throw new NotSupportException();
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new NotSupportException();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new NotSupportException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public String getSchema() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        networkTimeoutExecutor = executor;
        networkTimeoutNanos = milliseconds * 1000000L;
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return (int) (networkTimeoutNanos / 1000000L);
    }

    public void setNetworkTimeoutNanos(long nanos) {
        networkTimeoutNanos = nanos;
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
        return XConnection.class.isAssignableFrom(iface);
    }

    @Override
    public String toString() {
        final XSession s = this.session;
        if (null == s) {
            return "XConnection for closed session.";
        }
        return "XConnection for " + s;
    }
}
