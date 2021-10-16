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

import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.pool.XClientPool;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * @version 1.0
 */
public class XDataSource implements DataSource {

    private final String host;
    private final int port;
    private final String username;
    private final String defaultDatabase;
    private final String name;

    private long getConnTimeoutNanos = XConfig.DEFAULT_GET_CONN_TIMEOUT_NANOS;
    private String defaultEncodingMySQL = null;
    private long defaultQueryTimeoutNanos = -1;

    // Perf collections.
    private final AtomicLong queryCount = new AtomicLong(0);
    private final AtomicLong updateCount = new AtomicLong(0);
    private final AtomicLong tsoCount = new AtomicLong(0);
    // Total time of first pkt received after request in ms of all requests. (query, update, tso)
    private final AtomicLong totalRespondTime = new AtomicLong(0);
    // Total time of all pkt received after request in ms of all requests. (query, update, tso)
    private final AtomicLong totalPhysicalTime = new AtomicLong(0);

    // Cache perf.
    private final AtomicLong cachePlanQuery = new AtomicLong(0);
    private final AtomicLong cacheSqlQuery = new AtomicLong(0);
    private final AtomicLong cachePlanMiss = new AtomicLong(0);
    private final AtomicLong cacheSqlMiss = new AtomicLong(0);

    public XDataSource(String host, int port, String username, String password, String defaultDatabase, String name) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.defaultDatabase = defaultDatabase;
        this.name = name;
        XConnectionManager.getInstance().initializeDataSource(host, port, username, password);
    }

    public void close() {
        XConnectionManager.getInstance().deinitializeDataSource(host, port, username);
    }

    public String getUsername() {
        return username;
    }

    public String getName() {
        return name;
    }

    public long getGetConnTimeoutNanos() {
        return getConnTimeoutNanos;
    }

    public long getGetConnTimeoutMillis() {
        return getConnTimeoutNanos / 1000000L;
    }

    public void setGetConnTimeoutNanos(long nanos) {
        this.getConnTimeoutNanos = nanos;
    }

    public void setGetConnTimeoutMillis(long millis) {
        this.getConnTimeoutNanos = millis * 1000000L;
    }

    public void setDefaultEncodingMySQL(String defaultEncoding) {
        if (defaultEncoding.equalsIgnoreCase("utf8")) {
            defaultEncoding = "utf8mb4";
        }
        this.defaultEncodingMySQL = defaultEncoding;
    }

    public void setDefaultQueryTimeoutNanos(long nanos) {
        this.defaultQueryTimeoutNanos = nanos;
    }

    public void setDefaultQueryTimeoutMillis(long millis) {
        this.defaultQueryTimeoutNanos = millis * 1000000L;
    }

    public AtomicLong getQueryCount() {
        return queryCount;
    }

    public AtomicLong getUpdateCount() {
        return updateCount;
    }

    public AtomicLong getTsoCount() {
        return tsoCount;
    }

    public AtomicLong getTotalRespondTime() {
        return totalRespondTime;
    }

    public AtomicLong getTotalPhysicalTime() {
        return totalPhysicalTime;
    }

    public AtomicLong getCachePlanQuery() {
        return cachePlanQuery;
    }

    public AtomicLong getCacheSqlQuery() {
        return cacheSqlQuery;
    }

    public AtomicLong getCachePlanMiss() {
        return cachePlanMiss;
    }

    public AtomicLong getCacheSqlMiss() {
        return cacheSqlMiss;
    }

    public String getUrl() {
        // like this:
        // X://username@xx.xx.xx.xx:13000/abc?allowMultiQueries=true&autoReconnect=false&characterEncoding=utf8&connectTimeout=10000&failOverReadOnly=false&rewriteBatchedStatements=true&socketTimeout=900000&useServerPrepStmts=false&useSSL=false&useUnicode=true
        final StringBuilder builder = new StringBuilder();
        builder.append("X://").append(username).append('@').append(host).append(':').append(port).append('/');
        if (defaultDatabase != null) {
            builder.append(defaultDatabase);
        }
        builder.append("?connectTimeout=").append(getConnTimeoutNanos / 1000000L);
        if (defaultEncodingMySQL != null) {
            builder.append("&characterEncoding=").append(defaultEncodingMySQL);
        }
        if (defaultQueryTimeoutNanos > 0) {
            builder.append("&socketTimeout=").append(defaultQueryTimeoutNanos / 1000000L);
        }
        return builder.toString();
    }

    public XClientPool.XStatus getStatus() {
        return XConnectionManager.getInstance().getClientPool(host, port, username).getStatus();
    }

    public String getDigest() {
        return XConnectionManager.digest(host, port, username);
    }

    public XClientPool getClientPool() {
        return XConnectionManager.getInstance().getClientPool(host, port, username);
    }

    @Override
    public Connection getConnection() throws SQLException {
        XConnection connection = null;
        try {
            connection = XConnectionManager.getInstance()
                .getConnection(host, port, username, defaultDatabase, getConnTimeoutNanos);
            connection.setDataSource(this);
            if (defaultEncodingMySQL != null && !defaultEncodingMySQL.isEmpty() &&
                !defaultEncodingMySQL.equalsIgnoreCase(connection.getSession().getRequestEncodingMySQL())) {
                connection.getSession().setDefalutEncodingMySQL(defaultEncodingMySQL);
            } else {
                connection.getSession().setDefalutEncodingMySQL(null);
            }
            if (defaultQueryTimeoutNanos > 0) {
                connection.setNetworkTimeoutNanos(defaultQueryTimeoutNanos);
            }
            return connection;
        } catch (Exception e) {
            if (connection != null) {
                connection.setLastException(e);
                connection.close();
            }
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT, this + " " + e.getMessage());
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
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
        return XDataSource.class.isAssignableFrom(iface);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        // TODO: impl this
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        // TODO: impl this
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new NotSupportException();
    }

    @Override
    public String toString() {
        return "XDataSource to " + getDigest();
    }
}
