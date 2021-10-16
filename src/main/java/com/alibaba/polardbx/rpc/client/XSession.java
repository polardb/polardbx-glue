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

package com.alibaba.polardbx.rpc.client;

import com.alibaba.polardbx.common.constants.ServerVariables;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.XLog;
import com.alibaba.polardbx.rpc.XUtil;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.packet.XPacket;
import com.alibaba.polardbx.rpc.packet.XPacketBuilder;
import com.alibaba.polardbx.rpc.packet.XPacketQueue;
import com.alibaba.polardbx.rpc.perf.SessionPerfCollection;
import com.alibaba.polardbx.rpc.perf.SessionPerfItem;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxExpect;
import com.mysql.cj.polarx.protobuf.PolarxSession;
import com.mysql.cj.polarx.protobuf.PolarxSql;
import com.mysql.cj.x.protobuf.Polarx;
import com.mysql.cj.x.protobuf.PolarxDatatypes;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class XSession implements Comparable<XSession>, AutoCloseable {

    public static final AtomicInteger GLOBAL_COUNTER = new AtomicInteger(0);

    /**
     * Session identifier.
     */

    private final XClient client;
    private final long sessionId;

    // Creation time for aging.
    private final long createNanos = System.nanoTime();
    // Round delay 1min.
    private final long randomDelay = ThreadLocalRandom.current().nextLong(60_000_000_000L);

    /**
     * Session status.
     */

    public enum Status {
        Init, // Init by session pre-allocate.
        Waiting, // Wait session start response.
        Ready, // Ready for execution sql.
        Closed, // Closed.

        AutoCommit, // Optimized session with highest bit of sessionId set to 1.
    }

    private volatile long lastPacketNanos = 0;
    private final Object statusLock = new Object();
    private volatile Status status;

    /**
     * Context info for logger and others.
     */

    private volatile Map mdcContext = null;

    /**
     * Packet queue & request pipeline.
     */

    private final XPacketQueue packetQueue = new XPacketQueue();

    private volatile XResult lastUserRequest = null;
    private volatile XResult lastRequest = null;
    private Throwable lastException = null;
    private boolean lastIgnore = false;

    private String lastDB = null;
    private String lazyUseDB = null;

    private boolean lazyUseCtsTransaction = false;
    private long lazySnapshotSeq = -1L;
    private long lazyCommitSeq = -1L;

    // Cache control.
    private boolean noCache = false;
    private boolean forceCache = false;

    // Chunk control.
    private boolean chunkResult = false;

    // Active session control.
    private AtomicLong activeRequest = new AtomicLong(0);

    // Returning call control. Auto-reset after executed once
    private boolean returning = false;

    /**
     * Connection history sql for dump & debug.
     */
    private List<String> historySql = new ArrayList<>();
    private List<String> sessionSql = new ArrayList<>();

    private String xaStatus = null;
    private boolean transactionStatus = false;

    private StackTraceElement[] stackTraceElements = null;

    /**
     * Perf collection.
     */
    private final SessionPerfCollection perfCollection = new SessionPerfCollection();

    public void recordStack() {
        stackTraceElements = Thread.currentThread().getStackTrace();
    }

    public XSession(XClient client, long sessionId, Status status) {
        this.client = client;
        this.sessionId = sessionId;
        this.status = status;

        // Check.
        if ((Status.AutoCommit == status && 0 == (sessionId >>> 63)) ||
            (status != Status.AutoCommit && (sessionId >>> 63) != 0)) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION,
                this + " incorrect session id of " + status.name() + " session.");
        }
    }

    private void addActive() {
        final long before = activeRequest.getAndIncrement();
        if (0 == before) {
            // Record on per TCP and DN gather.
            client.getPerfCollection().getSessionActiveCount().getAndIncrement();
            client.getPool().getPerfCollection().getSessionActiveCount().getAndIncrement();
        }
    }

    public void subActive() {
        while (true) {
            final long before = activeRequest.get();
            if (before > 0) {
                if (activeRequest.compareAndSet(before, before - 1)) {
                    if (1 == before) {
                        // Record on per TCP and DN gather.
                        client.getPerfCollection().getSessionActiveCount().getAndDecrement();
                        client.getPool().getPerfCollection().getSessionActiveCount().getAndDecrement();
                    }
                    break;
                }
            } else {
                break;
            }
        }
    }

    private void freeAllActive() {
        while (true) {
            final long before = activeRequest.get();
            if (before > 0) {
                if (activeRequest.compareAndSet(before, 0)) {
                    // Record on per TCP and DN gather.
                    client.getPerfCollection().getSessionActiveCount().getAndDecrement();
                    client.getPool().getPerfCollection().getSessionActiveCount().getAndDecrement();
                    break;
                }
            } else {
                break;
            }
        }
    }

    @Override
    public void close() {
        // Clear all active counter.
        freeAllActive();

        // Check status first.
        final Status nowStatus;
        synchronized (statusLock) {
            nowStatus = status;
            if (Status.Closed == nowStatus) {
                XLog.XLogLogger.error(this + " close at error status.");
                return; // Just ignore and not throw.
            } else if (Status.AutoCommit == nowStatus) {
                return;
            }
        }

        // Send close to server if inited.
        if (nowStatus != Status.Init) {
            final boolean needClose;
            synchronized (statusLock) {
                needClose = (status != Status.Closed);
                if (needClose) {
                    status = Status.Closed;
                }
            }

            if (needClose) {
                GLOBAL_COUNTER.getAndDecrement();
                final PolarxSession.Close.Builder builder = PolarxSession.Close.newBuilder();
                final XPacket packet =
                    new XPacket(sessionId, Polarx.ClientMessages.Type.SESS_CLOSE_VALUE, builder.build());
                client.send(packet, false); // Do not flush and send at any time.
                pushEOF();
            }
        }
    }

    /**
     * Getter.
     */

    public XClient getClient() {
        return client;
    }

    public long getSessionId() {
        return sessionId;
    }

    public long getLastPacketNanos() {
        return lastPacketNanos;
    }

    public void setLastPacketNanos(long lastPacketNanos) {
        this.lastPacketNanos = lastPacketNanos;
    }

    public synchronized Status getStatus() {
        return status;
    }

    public void recordThreadContext() {
        mdcContext = MDC.getCopyOfContextMap();
    }

    public void clearThreadContext() {
        mdcContext = null;
    }

    public void applyThreadContext() {
        final Map nowMdcContext = mdcContext;
        if (nowMdcContext != null) {
            MDC.setContextMap(nowMdcContext);
        }
    }

    public XResult getLastUserRequest() {
        return lastUserRequest;
    }

    public synchronized XResult getLastRequest() {
        return lastRequest;
    }

    public synchronized Throwable getLastException() {
        return lastException;
    }

    public synchronized Throwable setLastException(Throwable lastException) {
        return this.lastException = lastException;
    }

    /**
     * Perf item.
     */

    public SessionPerfItem getPerfItem() {
        final SessionPerfItem item = new SessionPerfItem();

        item.setDnTag(client.getPool().getDnTag());
        item.setTcpTag(client.getTcpTag());
        item.setSessionId(sessionId);
        item.setStatus(status);

        item.setPlanCount(perfCollection.getPlanCount().get());
        item.setQueryCount(perfCollection.getQueryCount().get());
        item.setUpdateCount(perfCollection.getUpdateCount().get());
        item.setTsoCount(perfCollection.getTsoCount().get());

        final long nowNanos = System.nanoTime();
        item.setLiveNanos(nowNanos - createNanos);
        item.setIdleNanosSinceLastPacket(nowNanos - lastPacketNanos);

        item.setCharsetClient(getRequestEncodingMySQL());
        item.setCharsetResult(getResultMetaEncodingMySQL());
        item.setTimezone(getDefaultTimezone().getDisplayName());
        final int isolation = getIsolation();
        final String isolationString;
        switch (isolation) {
        case Connection.TRANSACTION_NONE:
            isolationString = "NONE";
            break;
        case Connection.TRANSACTION_READ_UNCOMMITTED:
            isolationString = "RU";
            break;
        case Connection.TRANSACTION_READ_COMMITTED:
            isolationString = "RC";
            break;
        case Connection.TRANSACTION_REPEATABLE_READ:
            isolationString = "RR";
            break;
        case Connection.TRANSACTION_SERIALIZABLE:
            isolationString = "S";
            break;
        default:
            isolationString = "UNKNOWN";
            break;
        }
        item.setIsolation(isolationString);
        item.setAutoCommit(isAutoCommit());

        // Gather variables.
        final StringBuilder builder = new StringBuilder();
        synchronized (this) {
            boolean first = true;
            for (Map.Entry<String, Object> entry : sessionVariablesChanged.entrySet()) {
                if (first) {
                    first = false;
                } else {
                    builder.append(',');
                }
                builder.append(entry.getKey()).append('=').append(entry.getValue());
            }
            item.setLastRequestDB(lastDB);
        }
        item.setVariablesChanged(builder.toString());

        final XResult request = this.lastRequest;
        if (request != null) {
            item.setQueuedRequestDepth(request.getQueuedDepth());

            // Gather with queued sql.
            StringBuilder sql = new StringBuilder(null == request.getSql() ? "" : request.getSql());
            XResult prob = request;
            while ((prob = prob.getPrevious()) != null) {
                sql.insert(0, (null == prob.getSql() ? ";" : (prob.getSql() + ";")));
            }

            item.setLastRequestTraceId(request.getConnection().getTraceId());
            item.setLastRequestSql(sql.toString());
            item.setLastRequestExtra(request.getExtra());
            item.setLastRequestType(request.getRequestType().toString());
            item.setLastRequestStatus(request.getStatus().toString());
            item.setLastRequestFetchCount(request.getFetchCount());
            item.setLastRequestTokenCount(request.getTokenCount());
            item.setLastRequestWorkingNanos(nowNanos - request.getStartNanos());
            item.setLastRequestDataPktResponseNanos(request.getPktResponseNanos());
            item.setLastRequestResponseNanos(request.getResponseNanos());
            item.setLastRequestFinishNanos(request.getFinishNanos());
            item.setLastRequestTokenDoneCount(request.getTokenDoneCount());
            item.setLastRequestActiveOfferTokenCount(request.getActiveOfferTokenCount());
            item.setLastRequestStartMillis(request.getStartMillis());
            item.setLastRequestResultChunk(request.isResultChunk());
            item.setLastRequestRetrans(request.isRetrans());
            item.setLastRequestGoCache(request.isGoCache());
        }

        return item;
    }

    /**
     * Auto commit state.
     */

    private boolean autoCommit = true;

    public synchronized boolean isAutoCommit() {
        return autoCommit;
    }

    public synchronized void setAutoCommit(XConnection connection, boolean enableAutoCommit) throws SQLException {
        final boolean needSet = enableAutoCommit != autoCommit;
        if (needSet) {
            autoCommit = enableAutoCommit;
        }
        try {
            if (needSet) {
                stashTransactionSequence();
                try {
                    execUpdate(connection, enableAutoCommit ? "SET autocommit=1" : "SET autocommit=0",
                        null, true, null);
                } finally {
                    stashPopTransactionSequence();
                }
            }
        } catch (Throwable e) {
            // This is fatal error.
            lastException = e;
            throw e;
        }
    }

    /**
     * Encoding and time zone.
     */

    private String characterSetClient = null;
    private String characterSetResults = null;
    private TimeZone timeZone = null;
    private SimpleDateFormat timeFormatter = null;
    private Integer isolation = null;

    private void traceCtsInfo() {
        if (lazyUseCtsTransaction) {
            historySql.add("msg_use_cts_transaction=ON");
            sessionSql.add("msg_use_cts_transaction=ON");
        }
        if (lazySnapshotSeq != -1) {
            historySql.add("msg_snapshot_seq=" + lazySnapshotSeq);
            sessionSql.add("msg_snapshot_seq=" + lazySnapshotSeq);
        }
        if (lazyCommitSeq != -1) {
            historySql.add("msg_commit_seq=" + lazyCommitSeq);
            sessionSql.add("msg_commit_seq=" + lazyCommitSeq);
        }
    }

    public static String toJavaEncoding(String encoding) {
        if (encoding.equalsIgnoreCase("utf8mb4")) {
            return "utf8";
        } else if (encoding.equalsIgnoreCase("binary")) {
            return "iso_8859_1";
        }
        return encoding;
    }

    public synchronized String getRequestEncodingMySQL() {
        if (null == characterSetClient) {
            Map<String, Object> variables = null == sessionVariables ? client.getGlobalVariablesL() : sessionVariables;
            if (null == variables) {
                characterSetClient = "utf8";
            } else {
                characterSetClient = (String) variables.get("character_set_client");
                if (null == characterSetClient) {
                    characterSetClient = "utf8";
                }
            }
        }
        return characterSetClient;
    }

    public synchronized String getResultMetaEncodingMySQL() {
        if (null == characterSetResults) {
            Map<String, Object> variables = null == sessionVariables ? client.getGlobalVariablesL() : sessionVariables;
            if (null == variables) {
                characterSetResults = "utf8";
            } else {
                characterSetResults = (String) variables.get("character_set_results");
                if (null == characterSetResults) {
                    characterSetResults = "utf8";
                }
            }
        }
        return characterSetResults;
    }

    public synchronized TimeZone getDefaultTimezone() {
        if (null == timeZone) {
            Map<String, Object> variables = null == sessionVariables ? client.getGlobalVariablesL() : sessionVariables;
            if (null == variables) {
                timeZone = TimeZone.getTimeZone("GMT+08:00");
            } else {
                String nowZone = (String) variables.get("time_zone");
                if (null == nowZone) {
                    timeZone = TimeZone.getTimeZone("GMT+08:00");
                } else {
                    if (nowZone.equalsIgnoreCase("SYSTEM")) {
                        nowZone = (String) variables.get("system_time_zone");
                        if (null == nowZone) {
                            nowZone = "GMT+08:00";
                        } else if (nowZone.equalsIgnoreCase("CST")) {
                            nowZone = "GMT+08:00";
                        }
                    } else {
                        final String trimmed = nowZone.trim();
                        if (trimmed.length() > 0 && ('+' == trimmed.charAt(0) || '-' == trimmed.charAt(0))) {
                            // Convert '+08:00' to 'GMT+08:00'
                            nowZone = "GMT" + trimmed;
                        } else if (!nowZone.equals(trimmed)) {
                            nowZone = trimmed;
                        }
                    }
                    timeZone = TimeZone.getTimeZone(nowZone);
                }
            }
        }
        return timeZone;
    }

    public synchronized String formatTime(Date date) {
        if (null == timeFormatter) {
            timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            timeFormatter.setTimeZone(getDefaultTimezone());
        }
        return timeFormatter.format(date);
    }

    public synchronized int getIsolation() {
        if (null == isolation) {
            Map<String, Object> variables = null == sessionVariables ? client.getGlobalVariablesL() : sessionVariables;
            if (null == variables) {
                isolation = Connection.TRANSACTION_READ_COMMITTED;
            } else {
                final String isolationString = (String) variables.get(isolationVariable);
                if (null == isolationString) {
                    isolation = Connection.TRANSACTION_READ_COMMITTED;
                } else {
                    switch (isolationString) {
                    case "READ-UNCOMMITTED":
                        isolation = Connection.TRANSACTION_READ_UNCOMMITTED;
                        break;

                    case "READ-COMMITTED":
                        isolation = Connection.TRANSACTION_READ_COMMITTED;
                        break;

                    case "REPEATABLE-READ":
                        isolation = Connection.TRANSACTION_REPEATABLE_READ;
                        break;

                    case "SERIALIZABLE":
                        isolation = Connection.TRANSACTION_SERIALIZABLE;
                        break;

                    default:
                        throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT,
                            this + " unknown isolation level: " + isolationString);
                    }
                }
            }
        }
        return isolation;
    }

    public synchronized void resetConnectionInfoCache() {
        characterSetClient = null;
        characterSetResults = null;
        timeZone = null;
        timeFormatter = null;
        isolation = null;
    }

    /**
     * Session variables.
     */

    final static private String[] protectedVariables = {
        "character_set_client",
        "character_set_connection",
        "character_set_results",
        "transaction_isolation",
        "tx_isolation",
        "autocommit"
    };
    final static private String[] encodingVariables = {
        "character_set_client",
        "character_set_connection",
        "character_set_results"
    };
    final static private String isolationVariable = "transaction_isolation";
    private Map<String, Object> sessionVariables = null;
    private Map<String, Object> globalVariables = null;
    private final Map<String, Object> sessionVariablesChanged = new HashMap<>();

    private void setVariables(XConnection connection, Map<String, Object> newVariables, boolean isGlobal)
        throws SQLException {
        if (!isGlobal) {
            if (sessionVariables == null) {
                sessionVariables = new HashMap<>(client.getSessionVariablesL());
            }
        } else {
            if (globalVariables == null) {
                globalVariables = new HashMap<>(client.getGlobalVariablesL());
            }
        }

        Map<String, Object> serverVariables = null;
        Set<String> serverVariablesNeedToRemove = Sets.newHashSet();

        if (!isGlobal) {
            // 这个连接没设置过的变量，需要用global值复原
            for (String key : sessionVariablesChanged.keySet()) {
                boolean isProtected = false;
                for (String var : protectedVariables) {
                    if (var.equals(key)) {
                        isProtected = true;
                        break;
                    }
                }
                if (isProtected || newVariables.containsKey(key)) {
                    // Ignore changes of protected variables.
                    continue;
                }

                if (serverVariables == null) {
                    serverVariables = new HashMap<>(newVariables); // 已经全部小写
                }

                if (client.getGlobalVariablesL().containsKey(key)) {
                    serverVariables.put(key, client.getGlobalVariablesL().get(key));
                } else {
                    serverVariablesNeedToRemove.add(key);
                }
            }
        }

        if (serverVariables == null) {
            serverVariables = newVariables; // 已经全部小写
        }

        boolean first = true;
        List<Object> parmas = new ArrayList<>();
        StringBuilder query = new StringBuilder("SET ");

        // 对比要设的值和已有的值，相同则跳过，不同则设成新值
        Map<String, Object> tmpVariablesChanged = new HashMap<>();

        for (Map.Entry<String, Object> entry : serverVariables.entrySet()) {
            String key = entry.getKey(); // 已经全部小写
            Object newValue = entry.getValue();

            // 不处理 DRDS 自定义的系统变量
            if (ServerVariables.extraVariables.contains(key) ||
                ServerVariables.XbannedVariables.contains(key)) {
                continue;
            }

            // 处理采用变量赋值的情况如 :
            // set sql_mode=@@sql_mode;
            // set sql_mode=@@global.sql_mode;
            // set sql_mode=@@session.sql_mode;
            if (newValue instanceof String) {
                String newValueStr = (String) newValue;

                if (newValueStr.startsWith("@@")) {
                    String newValueLC = newValueStr.toLowerCase();
                    String keyRef;
                    if (newValueLC.startsWith("@@session.")) {
                        keyRef = newValueLC.substring("@@session.".length());
                        newValue = this.sessionVariables.get(keyRef);
                    } else if (newValueLC.startsWith("@@global.")) {
                        keyRef = newValueLC.substring("@@global.".length());
                        newValue = client.getGlobalVariablesL().get(keyRef);
                    } else {
                        keyRef = newValueLC.substring("@@".length());
                        newValue = client.getGlobalVariablesL().get(keyRef);
                    }

                } else if ("default".equalsIgnoreCase(newValueStr)) {
                    newValue = client.getGlobalVariablesL().get(key);
                }
            }

            String newValueStr = String.valueOf(newValue);
            Object oldValue = isGlobal ? this.globalVariables.get(key) : this.sessionVariables.get(key);
            if (oldValue != null) {
                String oldValuesStr = String.valueOf(oldValue);
                if (TStringUtil.equalsIgnoreCase(newValueStr, oldValuesStr)) {
                    // skip same value
                    continue;
                } else if (StringUtils.isEmpty(newValueStr) && "NULL".equalsIgnoreCase(oldValuesStr)) {
                    // skip same value
                    continue;
                } else if (StringUtils.isEmpty(oldValuesStr) && "NULL".equalsIgnoreCase(newValueStr)) {
                    // skip same value
                    continue;
                }
            }

            if (!first) {
                query.append(" , ");
            } else {
                first = false;
            }

            boolean isBoth = ServerVariables.isMysqlBoth(key);
            if (isGlobal) {
                query.append(" GLOBAL ");
            }

            StringBuilder tmpQuery = new StringBuilder(" , ");

            query.append("`").append(key).append("`=");
            tmpQuery.append("`").append(key).append("`=");
            if (TStringUtil.isParsableNumber(newValueStr)) { // 纯数字类型
                query.append(newValueStr);
                tmpQuery.append(newValueStr);
            } else if ("NULL".equalsIgnoreCase(newValueStr) ||
                newValue == null ||
                StringUtils.isEmpty(newValueStr)) { // NULL或空字符串类型
                if (ServerVariables.canExecByBoth.contains(key) || ServerVariables.canOnlyExecByNullVariables
                    .contains(key)) {
                    query.append("NULL");
                    tmpQuery.append("NULL");
                } else if (ServerVariables.canOnlyExecByEmptyStrVariables.contains(key)) {
                    query.append("''");
                    tmpQuery.append("''");
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_VARIABLE_CAN_NOT_SET_TO_NULL_FOR_NOW, key);
                }
            } else { // 字符串兼容类型
                query.append("?");
                parmas.add(newValue);
                if (isBoth) {
                    tmpQuery.append("?");
                    parmas.add(newValue);
                }
            }

            if (isBoth) {
                query.append(tmpQuery);
            }

            tmpVariablesChanged.put(key, newValue);
        }

        for (String key : serverVariablesNeedToRemove) {
            if (!first) {
                query.append(", ");
            }
            query.append("@").append(key).append("=").append("NULL");
        }

        if (!first) { // 需要确保SET指令是完整的, 而不是只有一个SET前缀.
            List<PolarxDatatypes.Any> args = parmas.stream()
                .map(obj -> XUtil.genAny(XUtil.genScalar(obj, this)))
                .collect(Collectors.toList());
            stashTransactionSequence();
            try {
                // This can ignore, because we treat ignorable error as fatal.
                execUpdate(connection, query.toString(), args, true, null);
            } finally {
                stashPopTransactionSequence();
            }

            // Refresh cache after changed.
            resetConnectionInfoCache();
            if (!isGlobal) {
                for (Map.Entry<String, Object> e : tmpVariablesChanged.entrySet()) {
                    sessionVariables.put(e.getKey(), e.getValue());
                    sessionVariablesChanged.put(e.getKey(), e.getValue());
                }
            } else {
                for (Map.Entry<String, Object> e : tmpVariablesChanged.entrySet()) {
                    globalVariables.put(e.getKey(), e.getValue());
                }
            }
        }
    }

    public synchronized void setSessionVariables(XConnection connection, Map<String, Object> newServerVariables)
        throws SQLException {
        setVariables(connection, newServerVariables, false);
    }

    public synchronized void setGlobalVariables(XConnection connection, Map<String, Object> newGlobalVariables)
        throws SQLException {
        setVariables(connection, newGlobalVariables, true);
    }

    private String defaultEncodingMySQL = null;

    private synchronized void applyDefaultEncodingMySQL(XConnection connection) throws SQLException {
        if (defaultEncodingMySQL != null) {
            final String copy = defaultEncodingMySQL;
            defaultEncodingMySQL = null;
            if (!copy.equalsIgnoreCase(getRequestEncodingMySQL())) {
                stashTransactionSequence();
                try {
                    // This can ignore, because we treat ignorable error as fatal.
                    execUpdate(connection, "set names `" + copy + "`", null, true, null);
                } finally {
                    stashPopTransactionSequence();
                }
                // Set variable after successfully invoking.
                updateEncodingMySQL(copy);
            }
        }
    }

    // External set names should use this lazy set function.
    public synchronized void setDefalutEncodingMySQL(String defalutEncodingMySQL) {
        this.defaultEncodingMySQL = defalutEncodingMySQL;
    }

    // Invoke this after successfully set names.
    private synchronized void updateEncodingMySQL(String encoding) {
        // Clear default.
        defaultEncodingMySQL = null;

        if (null == sessionVariables) {
            sessionVariables = new HashMap<>(client.getSessionVariablesL());
        }

        for (String var : encodingVariables) {
            sessionVariables.put(var, encoding);
            sessionVariablesChanged.put(var, encoding);
        }

        // Reset cache.
        characterSetClient = null;
        characterSetResults = null;
    }

    // Invoke this after successfully set transaction isolation.
    public synchronized void updateIsolation(String value) {
        if (null == sessionVariables) {
            sessionVariables = new HashMap<>(client.getSessionVariablesL());
        }

        sessionVariables.put(isolationVariable, value);
        sessionVariablesChanged.put(isolationVariable, value);

        // Reset cache.
        isolation = null;
    }

    /**
     * Functions related to session.
     */

    public void flushNetwork() {
        client.flush();
    }

    public synchronized void flushIgnorable(XConnection connection) throws SQLException {
        if (XConfig.GALAXY_X_PROTOCOL && lastIgnore) {
            // Finish expectations.
            try {
                final long startNanos = System.nanoTime();
                client.send(new XPacket(sessionId, Polarx.ClientMessages.Type.EXPECT_CLOSE_VALUE,
                    PolarxExpect.Close.newBuilder().build()), true);
                lastRequest = new XResult(connection, packetQueue, lastRequest,
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                    true, XResult.RequestType.EXPECTATIONS, "expect_close",
                    connection.getDefaultTokenCount(), null, null);
                lastIgnore = false;
            } catch (Throwable e) {
                XLog.XLogLogger.error(this + " failed to close expect.");
                XLog.XLogLogger.error(lastException = e); // This is fatal error and should never happen.
            }
        }
        if (lastRequest != null && lastRequest.isIgnorableNotFinish()) {
            client.flush();
            try {
                lastRequest.waitFinish(true);
            } catch (Throwable e) {
                XLog.XLogLogger.error(this + " failed to flush ignorable.");
                XLog.XLogLogger.error(lastException = e); // This is fatal error and should never happen.
            }
        }
    }

    public synchronized boolean reusable() {
        if (XConnectionManager.getInstance().isEnableTrxLeakCheck() && (transactionStatus || xaStatus != null)) {
            XLog.XLogLogger.error(this + " transaction leak found. " +
                (transactionStatus ? "normal trx on." : "") +
                (xaStatus != null ? xaStatus : ""));
            final StringBuilder builder = new StringBuilder();
            builder.append("in stack:\n");
            if (stackTraceElements != null) {
                for (StackTraceElement e : stackTraceElements) {
                    builder.append(e.toString()).append('\n');
                }
            }
            final StackTraceElement[] now = Thread.currentThread().getStackTrace();
            builder.append("out stack:\n");
            for (StackTraceElement e : now) {
                builder.append(e.toString()).append('\n');
            }
            XLog.XLogLogger.error(builder.toString());
            return false;
        }
        return Status.Ready == status // Read only, and no status lock needed.
            && null == lastException
            && (null == lastRequest || lastRequest.isGoodAndDone()) // Must all done and no pending ignorable.
            && historySql.size() < 1000
            && client.isActive()
            && System.nanoTime() - createNanos - randomDelay < XConnectionManager.getInstance().getSessionAgingNanos()
            && (!XConfig.GALAXY_X_PROTOCOL || !lastIgnore);
    }

    public synchronized boolean reset() {
        if (reusable() && 0 == packetQueue.count()) {
            lastUserRequest = null;
            lastRequest = null;
            lastException = null;
            lastIgnore = false;
            lazySnapshotSeq = -1L;
            lazyCommitSeq = -1L;
            noCache = false;
            forceCache = false;
            chunkResult = false;
            sessionSql.clear();
            return true;
        }
        return false; // Bad connection.
    }

    public void cancel() {
        final Status nowStatus = status; // Read only, and no status lock needed.
        if (Status.Init == nowStatus || Status.Closed == nowStatus) {
            return; // Ignore inactive session.
        }
        try {
            final XPacket packet =
                new XPacket(sessionId, Polarx.ClientMessages.Type.SESS_KILL_VALUE,
                    PolarxSession.KillSession.newBuilder()
                        .setType(PolarxSession.KillSession.KillType.QUERY)
                        .setXSessionId(sessionId)
                        .build());
            client.send(packet, true);
        } finally {
            synchronized (this) {
                lastException = new Exception("Query was canceled.");
            }
        }
    }

    private void pushEOF() {
        pushFatal(XPacket.EOF);
    }

    private void pushKilled() {
        pushFatal(XPacket.KILLED);
    }

    private void dataNotify() {
        XResult probe = lastRequest;
        if (probe != null) {
            // Do pkt timing first.
            probe.recordPktResponse();
            // Do notify if needed.
            if (XConfig.GALAXY_X_PROTOCOL) {
                probe = lastUserRequest;
                if (null == probe) {
                    return;
                }
            }
            final boolean needPush = probe.getDataNotify() != null;
            if (needPush) {
                final XResult real;
                synchronized (this) {
                    real = XConfig.GALAXY_X_PROTOCOL ? lastUserRequest : lastRequest;
                }
                if (real != null) {
                    final Runnable cb; // Reference the cb in case of set to null.
                    if ((cb = real.getDataNotify()) != null) {
                        cb.run();
                    }
                }
            }
        }
    }

    public void pushFatal(XPacket fatal) {
        packetQueue.put(fatal);
        dataNotify();
    }

    public void kill(boolean pushKilled) {
        final Status nowStatus = status; // Read only, and no status lock needed.
        if (Status.Init == nowStatus || Status.Closed == nowStatus) {
            return; // Ignore inactive session.
        }
        try {
            final XPacket packet =
                new XPacket(sessionId, Polarx.ClientMessages.Type.SESS_KILL_VALUE,
                    PolarxSession.KillSession.newBuilder()
                        .setType(PolarxSession.KillSession.KillType.CONNECTION)
                        .setXSessionId(sessionId)
                        .build());
            client.send(packet, true);
        } finally {
            synchronized (this) {
                lastException = new Exception("Session was killed.");
            }
        }
        if (pushKilled) {
            pushKilled(); // Shutdown it anyway.
        }
    }

    public void killWithMessage(String msg) {
        kill(false);
        final String errStr = this + " killed. " + msg;
        pushFatal(new XPacket(-1, Polarx.ServerMessages.Type.ERROR_VALUE,
            Polarx.Error.newBuilder().setSeverity(Polarx.Error.Severity.FATAL).setCode(-1)
                .setSqlState(errStr)
                .setMsg(errStr).build()));
    }

    public void tokenOffer(int tokenCount) {
        final Status nowStatus = status; // Read only, and no status lock needed.
        if (Status.Init == nowStatus || Status.Closed == nowStatus) {
            return; // Ignore inactive session.
        }
        final XPacket packet =
            new XPacket(sessionId, Polarx.ClientMessages.Type.TOKEN_OFFER_VALUE,
                PolarxSql.TokenOffer.newBuilder()
                    .setToken(tokenCount)
                    .build());
        client.send(packet, true);
    }

    public void pushPackets(ArrayList<XPacket> packets) {
        if (packets.isEmpty()) {
            return;
        }
        try {
            int consume = 0;
            Status nowStatus = status;
            if (Status.Waiting == nowStatus) {
                synchronized (statusLock) {
                    if (Status.Waiting == status) {
                        if (Polarx.ServerMessages.Type.OK_VALUE == packets.get(0).getType()) {
                            ++consume;
                            nowStatus = status = Status.Ready;
                            // Collect perf data.
                            client.getPerfCollection().getSessionCreateSuccessCount().getAndIncrement();
                            client.getPool().getPerfCollection().getSessionCreateSuccessCount().getAndIncrement();
                        } else {
                            // Not ok? Fail or others?
                            if (Polarx.ServerMessages.Type.ERROR_VALUE == packets.get(0).getType()) {
                                final Polarx.Error error = (Polarx.Error) packets.get(0).getPacket();
                                if (error.getSeverity() == Polarx.Error.Severity.FATAL) {
                                    // Just pass through this error. No consume.
                                    nowStatus = status = Status.Ready;
                                }
                            }
                            // Collect perf data.
                            client.getPerfCollection().getSessionCreateFailCount().getAndIncrement();
                            client.getPool().getPerfCollection().getSessionCreateFailCount().getAndIncrement();
                            if (Status.Waiting == status) {
                                // Unknown packet?
                                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT,
                                    "Unknown new session response. sid: " + packets.get(0).getSid() + " type: "
                                        + packets.get(0).getType() + " pkt: " + packets.get(0).getPacket());
                            }
                        }
                    }
                }
            }
            // Status good or changed to ready.
            if (Status.Ready == nowStatus || Status.AutoCommit == nowStatus) {
                // Check memory/row limit.
                final long pipeBufferSizeLimit = XConnectionManager.getInstance().getDefaultPipeBufferSize();
                final int pipeCountLimit = 10 * XConnectionManager.getInstance().getDefaultQueryToken();
                if (packetQueue.getBufferSize() > pipeBufferSizeLimit) {
                    // Buffer size limit exceed.
                    killWithMessage("Pipe buffer size limit exceed.");
                } else if (packetQueue.count() > pipeCountLimit) {
                    // Count limit exceed.
                    killWithMessage("Pipe count limit exceed.");
                } else {
                    packetQueue.put(0 == consume ? packets : packets.subList(consume, packets.size()));
                    boolean gotData = false;
                    for (XPacket packet : packets) {
                        if (Polarx.ServerMessages.Type.RESULTSET_ROW_VALUE == packet.getType() ||
                            Polarx.ServerMessages.Type.RESULTSET_CHUNK_VALUE == packet.getType() ||
                            Polarx.ServerMessages.Type.RESULTSET_TSO_VALUE == packet.getType() ||
                            Polarx.ServerMessages.Type.RESULTSET_TOKEN_DONE_VALUE == packet.getType() ||
                            Polarx.ServerMessages.Type.RESULTSET_FETCH_DONE_VALUE == packet.getType() ||
                            Polarx.ServerMessages.Type.ERROR_VALUE == packet.getType() ||
                            Polarx.ServerMessages.Type.RESULTSET_FETCH_DONE_MORE_RESULTSETS_VALUE == packet.getType()) {
                            gotData = true;
                            break;
                        }
                    }
                    if (gotData) {
                        dataNotify();
                    }
                }
            }
        } catch (Throwable e) {
            XLog.XLogLogger.error(this + " failed to push packet.");
            XLog.XLogLogger.error(e);
            try {
                pushFatal(XPacket.FATAL);
            } catch (Throwable ignore) {
            }
        }
    }

    public synchronized void setDefaultDB(String defaultDB) {
        if (defaultDB != null && !defaultDB.isEmpty()) {
            if (lastDB != null && lastDB.equals(defaultDB)) {
                return;
            }
            lazyUseDB = defaultDB;
            lastDB = defaultDB;
        }
    }

    public void setLazyCtsTransaction() {
        this.lazyUseCtsTransaction = true;
    }

    public void setLazySnapshotSeq(long lazySnapshotSeq) {
        this.lazySnapshotSeq = lazySnapshotSeq;
    }

    public void setLazyCommitSeq(long lazyCommitSeq) {
        this.lazyCommitSeq = lazyCommitSeq;
    }

    // Stash sequence info.
    private boolean stashUseCtsTransaction = false;
    private long stashSnapshotSeq = -1L;
    private long stashCommitSeq = -1L;

    public void stashTransactionSequence() {
        stashUseCtsTransaction = lazyUseCtsTransaction;
        stashSnapshotSeq = lazySnapshotSeq;
        stashCommitSeq = lazyCommitSeq;
        lazyUseCtsTransaction = false;
        lazySnapshotSeq = -1;
        lazyCommitSeq = -1;
    }

    public void stashPopTransactionSequence() {
        lazyUseCtsTransaction = stashUseCtsTransaction;
        lazySnapshotSeq = stashSnapshotSeq;
        lazyCommitSeq = stashCommitSeq;
    }

    // Lazy init. Should invoke in synchronized function.
    private void initForRequest() {
        final Status nowStatus = status;
        if (Status.Init == nowStatus) {
            synchronized (statusLock) {
                if (Status.Init == status) {
                    // Record status change first.
                    status = Status.Waiting;
                    GLOBAL_COUNTER.getAndIncrement();

                    final PolarxSession.NewSession.Builder builder = PolarxSession.NewSession.newBuilder();
                    final XPacket packet =
                        new XPacket(sessionId, Polarx.ClientMessages.Type.SESS_NEW_VALUE, builder.build());
                    client.send(packet, false); // Do not flush and send with first request.
                }
            }
        } else if (Status.AutoCommit == nowStatus && lastRequest != null) {
            // Check auto commit duplicate request.
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION,
                this + " duplicated request on AutoCommit session.");
        }
    }

    public boolean isNoCache() {
        return noCache;
    }

    public void setNoCache(boolean noCache) {
        this.noCache = noCache;
    }

    public boolean isForceCache() {
        return forceCache;
    }

    public void setForceCache(boolean forceCache) {
        this.forceCache = forceCache;
    }

    public boolean isChunkResult() {
        return chunkResult;
    }

    public void setChunkResult(boolean chunkResult) {
        this.chunkResult = chunkResult;
    }

    // Should invoke in synchronized function.
    private XResult getResultSet(XConnection connection, long startNanos, boolean ignoreResult, String sql,
                                 XPacketBuilder retransmit, XResult.RequestType requestType, String extra)
        throws SQLException {
        lastUserRequest = lastRequest = new XResult(connection, packetQueue, lastRequest,
            startNanos, startNanos + connection.actualTimeoutNanos(),
            startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
            ignoreResult, requestType, sql,
            connection.getDefaultTokenCount(), retransmit, extra);
        // Use stream mode when we don't care about the result of query/update.
        // This is useful when set environment before query.
        return lastRequest;
    }

    private synchronized boolean send_internal(
        XConnection connection, XPacket packet, boolean ignoreResult) throws SQLException {
        if (XConfig.GALAXY_X_PROTOCOL) {
            // Dealing expectations.
            if (ignoreResult && !lastIgnore) {
                // Add no_error expect.
                client.send(new XPacket(sessionId, Polarx.ClientMessages.Type.EXPECT_OPEN_VALUE,
                        PolarxExpect.Open.newBuilder().addCond(
                            PolarxExpect.Open.Condition.newBuilder().setConditionKey(1)).build()),
                    false);
                final long startNanos = System.nanoTime();
                lastRequest = new XResult(connection, packetQueue, lastRequest,
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                    true, XResult.RequestType.EXPECTATIONS, "expect([+no_error])",
                    connection.getDefaultTokenCount(), null, null);
                lastIgnore = true;
            }
            if (!ignoreResult && lastIgnore) {
                // Need add close after message.
                client.send(packet, false);
                client.send(new XPacket(sessionId, Polarx.ClientMessages.Type.EXPECT_CLOSE_VALUE,
                    PolarxExpect.Close.newBuilder().build()), true);
                lastIgnore = false;
                return true;
            } else {
                client.send(packet, !ignoreResult);
            }
        } else {
            client.send(packet, !ignoreResult);
        }
        return false;
    }

    public synchronized XResult execQuery(XConnection connection, PolarxExecPlan.ExecPlan.Builder execPlan,
                                          String nativeSql, boolean ignoreResult) throws SQLException {
        if (XConfig.GALAXY_X_PROTOCOL) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION, "Plan trans not supported in galaxy.");
        }

        final XDataSource dataSource = connection.getDataSource();
        if (dataSource != null) {
            dataSource.getQueryCount().getAndIncrement();
        }
        perfCollection.getPlanCount().getAndIncrement();

        initForRequest();
        applyDefaultEncodingMySQL(connection);

        if (XLog.XProtocolLogger.isDebugEnabled()) {
            traceCtsInfo();

            historySql.add(execPlan.toString());
            sessionSql.add(execPlan.toString());
        }

        boolean pureDigest = false;
        // Dealing cache.
        if (!ignoreResult && !noCache && supportPlanCache()) {
            // Only query with no ignore can go the cache.
            // Always use digest.
            pureDigest = true;
            if (dataSource != null) {
                dataSource.getCachePlanQuery().getAndIncrement();
            }
        }

        String extra = null;
        if (!XConfig.GALAXY_X_PROTOCOL) {
            execPlan.setResetError(!lastIgnore);
        }
        execPlan.setToken(connection.getDefaultTokenCount());
        if (lazyUseCtsTransaction) {
            execPlan.setUseCtsTransaction(true);
            if (null == extra) {
                extra = "use_cts;";
            } else {
                extra += "use_cts;";
            }
            lazyUseCtsTransaction = false;
        }
        if (lazySnapshotSeq != -1) {
            execPlan.setSnapshotSeq(lazySnapshotSeq);
            if (null == extra) {
                extra = "snapshot=" + lazySnapshotSeq + ";";
            } else {
                extra += "snapshot=" + lazySnapshotSeq + ";";
            }
            lazySnapshotSeq = -1;
        }
        if (lazyCommitSeq != -1) {
            execPlan.setCommitSeq(lazyCommitSeq);
            if (null == extra) {
                extra = "commit=" + lazyCommitSeq + ";";
            } else {
                extra += "commit=" + lazyCommitSeq + ";";
            }
            lazyCommitSeq = -1;
        }
        if (chunkResult && supportChunkResult()) {
            execPlan.setChunkResult(true);
        }
        // Note: Feedback and compact meta set by caller in execPlan.

        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = ignoreResult;
        }

        final PolarxExecPlan.AnyPlan originalPlan = pureDigest && !forceCache ? execPlan.getPlan() : null;
        if (pureDigest) {
            execPlan.clearPlan();
        }
        final PolarxExecPlan.ExecPlan plan = execPlan.build();

        // Send request.
        final XPacket packet = new XPacket(sessionId, Polarx.ClientMessages.Type.EXEC_PLAN_READ_VALUE, plan);
        final XResult result;
        try {
            final long startNanos = System.nanoTime();
            final boolean postOp = send_internal(connection, packet, ignoreResult);
            if (!ignoreResult) {
                addActive();
            }
            result = getResultSet(connection, startNanos, ignoreResult, nativeSql,
                null == originalPlan ? null :
                    new XPacketBuilder(sessionId, execPlan, originalPlan), XResult.RequestType.PLAN_QUERY, extra);
            if (postOp) {
                lastRequest = new XResult(connection, packetQueue, lastRequest,
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                    true, XResult.RequestType.EXPECTATIONS, "expect_close",
                    connection.getDefaultTokenCount(), null, null);
            }
        } catch (Throwable t) {
            lastException = t; // This exception should drop the connection.
            throw t;
        }
        // Finish block mode outside here so exception will not fatal the session.
        if (!ignoreResult && !connection.isStreamMode()) {
            result.finishBlockMode();
        }
        return ignoreResult ? null : result;
    }

    public XResult execQuery(XConnection connection, String sql, List<PolarxDatatypes.Any> args,
                             boolean ignoreResult, ByteString digest) throws SQLException {
        return execQuery(connection, sql, args, ignoreResult, digest, null);
    }

    public synchronized XResult execQuery(XConnection connection, String sql, List<PolarxDatatypes.Any> args,
                                          boolean ignoreResult, ByteString digest, String returning)
        throws SQLException {
        if (returning != null && XConfig.GALAXY_X_PROTOCOL) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION, "Returning not supported in galaxy.");
        }

        if (XConnectionManager.getInstance().isEnableTrxLeakCheck()) {
            if (StringUtils.containsIgnoreCase(sql, "XA START ") ||
                StringUtils.containsIgnoreCase(sql, "XA PREPARE ")) {
                xaStatus = sql;
            } else if (StringUtils.containsIgnoreCase(sql, "XA ROLLBACK ") ||
                StringUtils.containsIgnoreCase(sql, "XA COMMIT ")) {
                xaStatus = null;
            } else if (sql.equalsIgnoreCase("begin") ||
                StringUtils.containsIgnoreCase(sql, "*/begin") ||
                StringUtils.containsIgnoreCase(sql, "START TRANSACTION")) {
                transactionStatus = true;
            } else if (sql.equalsIgnoreCase("rollback") || sql.equalsIgnoreCase("commit") ||
                StringUtils.containsIgnoreCase(sql, "*/rollback") ||
                StringUtils.containsIgnoreCase(sql, "*/commit")) {
                transactionStatus = false;
            }
        }

        final XDataSource dataSource = connection.getDataSource();
        if (dataSource != null) {
            dataSource.getQueryCount().getAndIncrement();
        }
        perfCollection.getQueryCount().getAndIncrement();

        initForRequest();
        applyDefaultEncodingMySQL(connection);

        if (XLog.XProtocolLogger.isDebugEnabled()) {
            traceCtsInfo();

            historySql.add(sql);
            sessionSql.add(sql);
        }

        // Send request.
        final PolarxSql.StmtExecute.Builder builder = PolarxSql.StmtExecute.newBuilder();

        final String returningHint = null != returning ? "/* +returning fields(" + returning + ") */ " : "";
        boolean setStmt = true;
        // Dealing cache.
        if (!ignoreResult && !noCache && digest != null && supportPlanCache()) {
            // Only query with no ignore can go the cache.
            final String traceId;
            if (sql.startsWith("/*DRDS")) {
                final int endIdx = sql.indexOf("*/", 6) + 2;
                traceId = returningHint + sql.substring(0, endIdx);
                sql = sql.substring(endIdx);
            } else {
                traceId = returningHint;
            }

            // Always use cache.
            setStmt = false;
            builder.setStmtDigest(digest);
            if (traceId != null) {
                builder.setHint(ByteString.copyFromUtf8(traceId));
            }
            if (dataSource != null) {
                dataSource.getCacheSqlQuery().getAndIncrement();
            }
        } else if (!XConfig.GALAXY_X_PROTOCOL) {
            builder.setHint(ByteString.copyFromUtf8(returningHint));
        }

        String extra = null;
        if (setStmt) {
            try {
                builder.setStmt(ByteString.copyFrom(sql, toJavaEncoding(getRequestEncodingMySQL())));
            } catch (UnsupportedEncodingException e) {
                XLog.XLogLogger.error(e);
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CONNECTION, e.getMessage());
            }
        }
        if (args != null) {
            builder.addAllArgs(args);
        }
        if (connection.isCompactMetadata()) {
            builder.setCompactMetadata(true);
        }
        if (lazyUseDB != null) {
            builder.setSchemaName(lazyUseDB);
            lazyUseDB = null;
        }
        if (!XConfig.GALAXY_X_PROTOCOL) {
            builder.setResetError(!lastIgnore);
        }
        builder.setToken(connection.getDefaultTokenCount());
        if (lazyUseCtsTransaction) {
            builder.setUseCtsTransaction(true);
            if (null == extra) {
                extra = "use_cts;";
            } else {
                extra += "use_cts;";
            }
            lazyUseCtsTransaction = false;
        }
        if (lazySnapshotSeq != -1) {
            builder.setSnapshotSeq(lazySnapshotSeq);
            if (null == extra) {
                extra = "snapshot=" + lazySnapshotSeq + ";";
            } else {
                extra += "snapshot=" + lazySnapshotSeq + ";";
            }
            lazySnapshotSeq = -1;
        }
        if (lazyCommitSeq != -1) {
            builder.setCommitSeq(lazyCommitSeq);
            if (null == extra) {
                extra = "commit=" + lazyCommitSeq + ";";
            } else {
                extra += "commit=" + lazyCommitSeq + ";";
            }
            lazyCommitSeq = -1;
        }
        if (chunkResult && supportChunkResult()) {
            builder.setChunkResult(true);
        }
        if (connection.isWithFeedback() && supportFeedback()) {
            builder.setFeedBack(true);
        }

        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = ignoreResult;
        }

        final XPacket packet = new XPacket(sessionId, Polarx.ClientMessages.Type.EXEC_SQL_VALUE, builder.build());
        final XResult result;
        try {
            final long startNanos = System.nanoTime();
            final boolean postOp = send_internal(connection, packet, ignoreResult);
            if (!ignoreResult) {
                addActive();
            }
            final XPacketBuilder retransmit;
            if (!setStmt && !forceCache) {
                retransmit = new XPacketBuilder(sessionId, builder, sql, toJavaEncoding(getRequestEncodingMySQL()));
            } else {
                retransmit = null;
            }
            result = getResultSet(
                connection, startNanos, ignoreResult, sql, retransmit, XResult.RequestType.SQL_QUERY, extra);
            if (postOp) {
                lastRequest = new XResult(connection, packetQueue, lastRequest,
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                    true, XResult.RequestType.EXPECTATIONS, "expect_close",
                    connection.getDefaultTokenCount(), null, null);
            }
        } catch (Throwable t) {
            lastException = t; // This exception should drop the connection.
            throw t;
        }
        // Finish block mode outside here so exception will not fatal the session.
        if (!ignoreResult && !connection.isStreamMode()) {
            result.finishBlockMode();
        }
        return ignoreResult ? null : result;
    }

    // Return 0 if ignore result.
    public synchronized XResult execUpdate(XConnection connection, String sql, List<PolarxDatatypes.Any> args,
                                           boolean ignoreResult, ByteString digest) throws SQLException {
        if (digest != null && XConfig.GALAXY_X_PROTOCOL) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION, "Digest not supported in galaxy.");
        }

        if (XConnectionManager.getInstance().isEnableTrxLeakCheck()) {
            if (StringUtils.containsIgnoreCase(sql, "XA START ") ||
                StringUtils.containsIgnoreCase(sql, "XA PREPARE ")) {
                xaStatus = sql;
            } else if (StringUtils.containsIgnoreCase(sql, "XA ROLLBACK ") ||
                StringUtils.containsIgnoreCase(sql, "XA COMMIT ")) {
                xaStatus = null;
            } else if (sql.equalsIgnoreCase("begin") ||
                StringUtils.containsIgnoreCase(sql, "*/begin") ||
                StringUtils.containsIgnoreCase(sql, "START TRANSACTION")) {
                transactionStatus = true;
            } else if (sql.equalsIgnoreCase("rollback") || sql.equalsIgnoreCase("commit") ||
                StringUtils.containsIgnoreCase(sql, "*/rollback") ||
                StringUtils.containsIgnoreCase(sql, "*/commit")) {
                transactionStatus = false;
            }
        }

        final XDataSource dataSource = connection.getDataSource();
        if (dataSource != null) {
            dataSource.getUpdateCount().getAndIncrement();
        }
        perfCollection.getUpdateCount().getAndIncrement();

        initForRequest();
        applyDefaultEncodingMySQL(connection);

        if (XLog.XProtocolLogger.isDebugEnabled()) {
            traceCtsInfo();

            historySql.add(sql);
            sessionSql.add(sql);
        }

        // Send request.
        final PolarxSql.StmtExecute.Builder builder = PolarxSql.StmtExecute.newBuilder();

        boolean setStmt = true;
        // Dealing cache.
        if (!ignoreResult && !noCache && digest != null && supportPlanCache()) {
            // Only query with no ignore can go the cache.
            final String traceId;
            if (sql.startsWith("/*DRDS")) {
                final int endIdx = sql.indexOf("*/", 6) + 2;
                traceId = sql.substring(0, endIdx);
                sql = sql.substring(endIdx);
            } else {
                traceId = null;
            }

            // Always use cache.
            setStmt = false;
            builder.setStmtDigest(digest);
            if (traceId != null) {
                builder.setHint(ByteString.copyFromUtf8(traceId));
            }
            if (dataSource != null) {
                dataSource.getCacheSqlQuery().getAndIncrement();
            }
        }

        String extra = null;
        if (setStmt) {
            try {
                builder.setStmt(ByteString.copyFrom(sql, toJavaEncoding(getRequestEncodingMySQL())));
            } catch (UnsupportedEncodingException e) {
                XLog.XLogLogger.error(e);
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CONNECTION, e.getMessage());
            }
        }
        if (args != null) {
            builder.addAllArgs(args);
        }
        if (connection.isCompactMetadata()) {
            builder.setCompactMetadata(true);
        }
        if (lazyUseDB != null) {
            builder.setSchemaName(lazyUseDB);
            lazyUseDB = null;
        }
        if (!XConfig.GALAXY_X_PROTOCOL) {
            builder.setResetError(!lastIgnore);
        }
        if (lazyUseCtsTransaction) {
            builder.setUseCtsTransaction(true);
            if (null == extra) {
                extra = "use_cts;";
            } else {
                extra += "use_cts;";
            }
            lazyUseCtsTransaction = false;
        }
        if (lazySnapshotSeq != -1) {
            builder.setSnapshotSeq(lazySnapshotSeq);
            if (null == extra) {
                extra = "snapshot=" + lazySnapshotSeq + ";";
            } else {
                extra += "snapshot=" + lazySnapshotSeq + ";";
            }
            lazySnapshotSeq = -1;
        }
        if (lazyCommitSeq != -1) {
            builder.setCommitSeq(lazyCommitSeq);
            if (null == extra) {
                extra = "commit=" + lazyCommitSeq + ";";
            } else {
                extra += "commit=" + lazyCommitSeq + ";";
            }
            lazyCommitSeq = -1;
        }
        if (connection.isWithFeedback() && supportFeedback()) {
            builder.setFeedBack(true);
        }

        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = ignoreResult;
        }

        final XPacket packet = new XPacket(sessionId, Polarx.ClientMessages.Type.EXEC_SQL_VALUE, builder.build());
        final XResult result;
        try {
            final long startNanos = System.nanoTime();
            final boolean postOp = send_internal(connection, packet, ignoreResult);
            if (!ignoreResult) {
                addActive();
            }
            final XPacketBuilder retransmit;
            if (!setStmt && !forceCache) {
                retransmit = new XPacketBuilder(sessionId, builder, sql, toJavaEncoding(getRequestEncodingMySQL()));
            } else {
                retransmit = null;
            }
            result = lastUserRequest = lastRequest = new XResult(connection, packetQueue, lastRequest,
                startNanos, startNanos + connection.actualTimeoutNanos(),
                startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                ignoreResult, XResult.RequestType.SQL_UPDATE, sql,
                connection.getDefaultTokenCount(), retransmit, extra); // Only care about ignore or not.
            if (postOp) {
                lastRequest = new XResult(connection, packetQueue, lastRequest,
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                    true, XResult.RequestType.EXPECTATIONS, "expect_close",
                    connection.getDefaultTokenCount(), null, null);
            }
        } catch (Throwable t) {
            lastException = t; // This exception should drop the connection.
            throw t;
        }
        // Finish block mode outside here so exception will not fatal the session.
        if (!ignoreResult) {
            result.finishBlockMode();
        }
        return result;
    }

    public synchronized long getTSO(XConnection connection, int count) throws SQLException {
        final XDataSource dataSource = connection.getDataSource();
        if (dataSource != null) {
            dataSource.getTsoCount().getAndIncrement();
        }
        perfCollection.getTsoCount().getAndIncrement();

        initForRequest();

        final String GET_TSO_SQL = "get_tso()";
        if (XLog.XProtocolLogger.isDebugEnabled()) {
            historySql.add(GET_TSO_SQL);
            sessionSql.add(GET_TSO_SQL);
        }

        // Send request.
        final PolarxExecPlan.GetTSO.Builder builder = PolarxExecPlan.GetTSO.newBuilder();
        builder.setLeaderName(ByteString.EMPTY);
        if (count < 1) {
            count = 1;
        }
        builder.setBatchCount(count);

        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = false;
        }

        final XPacket packet = new XPacket(sessionId, Polarx.ClientMessages.Type.GET_TSO_VALUE, builder.build());
        final XResult result;
        try {
            final long startNanos = System.nanoTime();
            final boolean postOp = send_internal(connection, packet, false);
            addActive();
            result = lastUserRequest = lastRequest = new XResult(connection, packetQueue, lastRequest,
                startNanos, startNanos + connection.actualTimeoutNanos(),
                startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                false, XResult.RequestType.FETCH_TSO, GET_TSO_SQL, connection.getDefaultTokenCount(), null, null);
            if (postOp) {
                lastRequest = new XResult(connection, packetQueue, lastRequest,
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                    true, XResult.RequestType.EXPECTATIONS, "expect_close",
                    connection.getDefaultTokenCount(), null, null);
            }
        } catch (Throwable t) {
            lastException = t; // This exception should drop the connection.
            throw t;
        }
        // Finish block mode outside here so exception will not fatal the session.
        result.finishBlockMode();
        if (result.getTsoErrorNo() != XResultUtil.GTS_PROTOCOL_SUCCESS) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CONNECTION,
                this + " failed to get TSO. errno " + result.getTsoErrorNo());
        }
        return result.getTsoValue();
    }

    private long connectionId = -1;

    public long getConnectionId(XConnection connection) throws SQLException {
        // Try cached first.
        if (connectionId != -1) {
            return connectionId;
        }

        // Query.
        Long connId = null;
        final XResult result = execQuery(connection, "select connection_id()", null, false, null);
        while (result.next() != null) {
            try {
                Number res = (Number) XResultUtil.resultToObject(
                    result.getMetaData().get(0), result.current().getRow().get(0), true,
                    TimeZone.getDefault()).getKey();
                if (null == connId) {
                    connId = res.longValue();
                }
            } catch (Throwable ignore) {
            }
        }
        if (null == connId) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CONNECTION,
                this + " failed to get connection id.");
        }
        connectionId = connId;
        return connId;
    }

    private Boolean flagMessageTimestamp = null;
    private Boolean flagQueryCache = null;
    private Boolean flagQueryChunk = null;
    private Boolean flagSingleShardOptimization = null;
    private Boolean flagFeedback = null;

    public boolean supportMessageTimestamp() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }

        if (!XConnectionManager.getInstance().isEnableMessageTimestamp()) {
            return false;
        }
        if (flagMessageTimestamp != null) {
            return flagMessageTimestamp;
        }
        if (!client.isActive()) {
            return false; // Initializing.
        }

        if (client.getBaseVersion() == XClient.DnBaseVersion.DN_X_CLUSTER) {
            final String minor = client.getMinorVersion();
            if (minor.equals("local")) {
                return flagMessageTimestamp = XConfig.X_LOCAL_ENABLE_MESSAGE_TIMESTAMP;
            }
            try {
                final long ver = Long.parseLong(minor);
                if (ver >= 20210204L) { // Since 1.6.0.6 20210204
                    return flagMessageTimestamp = true;
                }
            } catch (Exception e) {
                XLog.XLogLogger.error(this + " unknown xdb minor version: " + minor);
            }
        } else if (client.getBaseVersion() == XClient.DnBaseVersion.DN_RDS_80_X_CLUSTER) {
            return flagMessageTimestamp = true; // Enable by default.
        }
        return flagMessageTimestamp = false;
    }

    public boolean supportPlanCache() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }

        if (!XConnectionManager.getInstance().isEnablePlanCache()) {
            return false;
        }
        if (flagQueryCache != null) {
            return flagQueryCache;
        }
        if (!client.isActive()) {
            return false; // Initializing.
        }

        if (client.getBaseVersion() == XClient.DnBaseVersion.DN_X_CLUSTER) {
            final String minor = client.getMinorVersion();
            if (minor.equals("local")) {
                return flagQueryCache = XConfig.X_LOCAL_ENABLE_PLAN_CACHE;
            }
            try {
                final long ver = Long.parseLong(minor);
                if (ver >= 20210204L) { // Since 1.6.0.6 20210204
                    return flagQueryCache = true;
                }
            } catch (Exception e) {
                XLog.XLogLogger.error(this + " unknown xdb minor version: " + minor);
            }
        } else if (client.getBaseVersion() == XClient.DnBaseVersion.DN_RDS_80_X_CLUSTER) {
            return flagMessageTimestamp = true; // Enable by default.
        }
        return flagQueryCache = false;
    }

    public boolean supportChunkResult() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }

        if (!XConnectionManager.getInstance().isEnableChunkResult()) {
            return false;
        }
        if (flagQueryChunk != null) {
            return flagQueryChunk;
        }
        if (!client.isActive()) {
            return false; // Initializing.
        }

        if (client.getBaseVersion() == XClient.DnBaseVersion.DN_X_CLUSTER) {
            final String minor = client.getMinorVersion();
            if (minor.equals("local")) {
                return flagQueryChunk = XConfig.X_LOCAL_ENABLE_CHUNK_RESULT;
            }
            try {
                final long ver = Long.parseLong(minor);
                if (ver > 20210315L) { // Since 1.6.0.6 20210315
                    return flagQueryChunk = true;
                }
            } catch (Exception e) {
                XLog.XLogLogger.error(this + " unknown xdb minor version: " + minor);
            }
        } else if (client.getBaseVersion() == XClient.DnBaseVersion.DN_RDS_80_X_CLUSTER) {
            return flagMessageTimestamp = true; // Enable by default.
        }
        return flagQueryChunk = false;
    }

    public boolean supportSingleShardOptimization() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }

        if (!XConnectionManager.getInstance().isEnableMessageTimestamp()) {
            return false; // Reuse the timestamp option.
        }
        if (flagSingleShardOptimization != null) {
            return flagSingleShardOptimization;
        }
        if (!client.isActive()) {
            return false; // Initializing.
        }

        if (client.getBaseVersion() == XClient.DnBaseVersion.DN_X_CLUSTER) {
            final String minor = client.getMinorVersion();
            if (minor.equals("local")) {
                // Reuse the timestamp option.
                return flagSingleShardOptimization = XConfig.X_LOCAL_ENABLE_MESSAGE_TIMESTAMP;
            }
            try {
                final long ver = Long.parseLong(minor);
                if (ver >= 20210701L) { // Since 1.6.0.8 20210701
                    return flagSingleShardOptimization = true;
                }
            } catch (Exception e) {
                XLog.XLogLogger.error(this + " unknown xdb minor version: " + minor);
            }
        } else if (client.getBaseVersion() == XClient.DnBaseVersion.DN_RDS_80_X_CLUSTER) {
            return flagMessageTimestamp = true; // Enable by default.
        }
        return flagSingleShardOptimization = false;
    }

    public boolean supportFeedback() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }

        if (!XConnectionManager.getInstance().isEnableFeedback()) {
            return false; // Reuse the timestamp option.
        }
        if (flagFeedback != null) {
            return flagFeedback;
        }
        if (!client.isActive()) {
            return false; // Initializing.
        }

        if (client.getBaseVersion() == XClient.DnBaseVersion.DN_X_CLUSTER) {
            final String minor = client.getMinorVersion();
            if (minor.equals("local")) {
                // Reuse the timestamp option.
                return flagFeedback = XConfig.X_LOCAL_ENABLE_FEEDBACK;
            }
            try {
                final long ver = Long.parseLong(minor);
                if (ver >= 20210624L) { // Since 1.6.0.8 20210624
                    return flagFeedback = true;
                }
            } catch (Exception e) {
                XLog.XLogLogger.error(this + " unknown xdb minor version: " + minor);
            }
        } else if (client.getBaseVersion() == XClient.DnBaseVersion.DN_RDS_80_X_CLUSTER) {
            return flagMessageTimestamp = true; // Enable by default.
        }
        return flagFeedback = false;
    }

    /**
     * Overrides.
     */

    @Override
    public int hashCode() {
        return Long.hashCode(sessionId);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof XSession && ((XSession) obj).sessionId == sessionId;
    }

    @Override
    public int compareTo(XSession o) {
        if (sessionId == o.sessionId) {
            return 0;
        }
        return sessionId < o.sessionId ? -1 : 1;
    }

    @Override
    public String toString() {
        return "XSession sid=" + sessionId + " status=" + status + " from " + client;
    }
}
