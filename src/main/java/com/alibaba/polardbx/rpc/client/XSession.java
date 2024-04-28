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
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.rpc.GalaxyPrepare.GPTable;
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
import com.googlecode.protobuf.format.JsonFormat;
import com.mysql.cj.polarx.protobuf.PolarxExpect;
import com.mysql.cj.polarx.protobuf.PolarxPhysicalBackfill;
import com.mysql.cj.polarx.protobuf.PolarxSession;
import com.mysql.cj.polarx.protobuf.PolarxSql;
import com.mysql.cj.x.protobuf.Polarx;
import com.mysql.cj.x.protobuf.PolarxDatatypes;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import org.apache.commons.lang.StringUtils;

import java.nio.charset.UnsupportedCharsetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class XSession implements Comparable<XSession>, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(XSession.class);

    public static final AtomicInteger GLOBAL_COUNTER = new AtomicInteger(0);
    public static final ByteString EXPLAIN_PRE = ByteString.copyFromUtf8("explain ");

    private static final Exception CANCEL_EXCEPTION = new Exception("Query was canceled.");
    private static final Exception KILL_EXCEPTION = new Exception("Session was killed.");

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

    private final AtomicReference<XResult> lastUserRequest = new AtomicReference<>(null);
    private final AtomicReference<XResult> lastRequest = new AtomicReference<>(null);
    private final AtomicReference<Throwable> lastException = new AtomicReference<>(null);
    private boolean lastIgnore = false;

    private String lastDB = null;
    private String lazyUseDB = null;

    private boolean lazyUseCtsTransaction = false;
    private boolean lazyMarkDistributed = false;
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
                if (!isXRPC()) {
                    // in old Xproto, should kill before close the session to prevent result set leak
                    final XPacket packet =
                        new XPacket(sessionId, Polarx.ClientMessages.Type.SESS_KILL_VALUE,
                            PolarxSession.KillSession.newBuilder()
                                .setType(PolarxSession.KillSession.KillType.CONNECTION)
                                .setXSessionId(sessionId)
                                .build());
                    client.send(packet, false); // flush when close
                }
                final PolarxSession.Close.Builder builder = PolarxSession.Close.newBuilder();
                final XPacket packet =
                    new XPacket(sessionId, Polarx.ClientMessages.Type.SESS_CLOSE_VALUE, builder.build());
                client.send(packet, true);
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
        return lastUserRequest.get();
    }

    public XResult getLastRequest() {
        return lastRequest.get();
    }

    public Throwable getLastException() {
        return lastException.get();
    }

    public Throwable setLastException(Throwable lastException, boolean forceReplace) {
        if (lastException != null && forceReplace) {
            this.lastException.set(lastException);
        } else {
            this.lastException.compareAndSet(null, lastException);
        }
        return lastException;
    }

    public boolean resetExceptionFromCancel() {
        final Throwable throwable = lastException.get();
        if (throwable == CANCEL_EXCEPTION) {
            return lastException.compareAndSet(throwable, null);
        }
        return false;
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

        final XResult request = this.lastRequest.get();
        if (request != null) {
            item.setQueuedRequestDepth(request.getQueuedDepth());

            // Gather with queued sql.
            StringBuilder sql = new StringBuilder(null == request.getSql() ? "" : request.getSql().display());
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
            item.setLastRequestTokenSize(request.getTokenSize());
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
                boolean isStashed = stashTransactionSequence();
                try {
                    execUpdate(connection,
                        BytesSql.getBytesSql(enableAutoCommit ? "SET autocommit=1" : "SET autocommit=0"), null, null,
                        true, null);
                } finally {
                    if (isStashed) {
                        stashPopTransactionSequence();
                    }
                }
            }
        } catch (Throwable e) {
            // This is fatal error.
            throw GeneralUtil.nestedException(setLastException(e, true));
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
        if (lazyMarkDistributed) {
            historySql.add("msg_mark_distributed=ON");
            sessionSql.add("msg_mark_distributed=ON");
        }
    }

    public static String toJavaEncoding(String encoding) {
        if (encoding.startsWith("utf8mb")) {
            return "utf8";
        } else if (encoding.equalsIgnoreCase("binary")) {
            return "iso_8859_1";
        }
        return encoding;
    }

    public synchronized String getRequestEncodingMySQL() {
        // Caution: we may set names before actual sql request, so get target encoding if exists
        if (defaultEncodingMySQL != null) {
            return defaultEncodingMySQL;
        }
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
                    switch (isolationString.toUpperCase()) {
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
        }

        Map<String, Object> serverVariables = null;
        Set<String> serverVariablesNeedToRemove = Sets.newHashSet();
        boolean resetSqlLogBin = false;

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

            // 对于 DN 仅支持 set global 设置的变量，禁止 set session
            if (!isGlobal && ServerVariables.isMysqlGlobal(key)) {
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
            Object oldValue;
            if (!isGlobal) {
                oldValue = this.sessionVariables.get(key);
            } else {
                oldValue = this.globalVariables == null ? this.client.getGlobalVariablesL().get(key) :
                    this.globalVariables.get(key);
            }
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
            } else {
                first = false;
            }
            if (key.equalsIgnoreCase("sql_log_bin")) {
                query.append(key).append("=").append("'ON'");
                resetSqlLogBin = true;
            } else {
                query.append("@").append(key).append("=").append("NULL");
            }
        }

        if (!first) { // 需要确保SET指令是完整的, 而不是只有一个SET前缀.
            List<PolarxDatatypes.Any> args = parmas.stream()
                .map(obj -> XUtil.genAny(XUtil.genScalar(obj, this)))
                .collect(Collectors.toList());
            boolean isStashed = stashTransactionSequence();
            try {
                // This can ignore, because we treat ignorable error as fatal.
                execUpdate(connection, BytesSql.getBytesSql(query.toString()), null, args, true, null);
            } catch (Exception ex) {
                if (resetSqlLogBin) {
                    connection.setLastException(ex, true);
                }
            } finally {
                if (isStashed) {
                    stashPopTransactionSequence();
                }
            }

            // Refresh cache after changed.
            resetConnectionInfoCache();
            if (!isGlobal) {
                for (String key : serverVariablesNeedToRemove) {
                    sessionVariables.remove(key);
                    sessionVariablesChanged.remove(key);
                }
                for (Map.Entry<String, Object> e : tmpVariablesChanged.entrySet()) {
                    sessionVariables.put(e.getKey(), e.getValue());
                    sessionVariablesChanged.put(e.getKey(), e.getValue());
                }
            } else {
                if (globalVariables == null && !tmpVariablesChanged.isEmpty()) {
                    globalVariables = new HashMap<>(client.getGlobalVariablesL());
                }
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
                boolean isStashed = stashTransactionSequence();
                try {
                    // This can ignore, because we treat ignorable error as fatal.
                    execUpdate(connection, BytesSql.getBytesSql("set names `" + copy + "`"), null, null, true,
                        null);
                } finally {
                    if (isStashed) {
                        stashPopTransactionSequence();
                    }
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

    public void flushIgnorable(XConnection connection) throws SQLException {
        final XResult req;
        synchronized (this) {
            if (XConfig.GALAXY_X_PROTOCOL && lastIgnore) {
                // Finish expectations.
                try {
                    final long startNanos = System.nanoTime();
                    client.send(new XPacket(sessionId, Polarx.ClientMessages.Type.EXPECT_CLOSE_VALUE,
                        PolarxExpect.Close.newBuilder().build()), true);
                    lastRequest.set(new XResult(connection, packetQueue, lastRequest.get(),
                        startNanos, startNanos + connection.actualTimeoutNanos(),
                        startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                        true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect_close"),
                        connection.getDefaultTokenKb(), null, null));
                    lastIgnore = false;
                } catch (Throwable e) {
                    XLog.XLogLogger.error(this + " failed to close expect.");
                    XLog.XLogLogger.error(setLastException(e, true)); // This is fatal error and should never happen.
                }
            }
            req = lastRequest.get();
        }
        if (req != null && req.isIgnorableNotFinish()) {
            client.flush();
            try {
                req.waitFinish(true);
            } catch (Throwable e) {
                XLog.XLogLogger.error(this + " failed to flush ignorable.");
                XLog.XLogLogger.error(setLastException(e, true)); // This is fatal error and should never happen.
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
            && null == getLastException()
            && (null == lastRequest.get() || lastRequest.get().isGoodAndDone())
            // Must all done and no pending ignorable.
            && historySql.size() < 1000
            && client.isActive() && client.reusable()
            && System.nanoTime() - createNanos - randomDelay < XConnectionManager.getInstance().getSessionAgingNanos()
            && (!XConfig.GALAXY_X_PROTOCOL || !lastIgnore);
    }

    public synchronized boolean reset() {
        if (reusable() && 0 == packetQueue.count()) {
            lastUserRequest.set(null);
            lastRequest.set(null);
            // lastException.set(null); never reset fatal error
            lastIgnore = false;
            lazySnapshotSeq = -1L;
            lazyCommitSeq = -1L;
            lazyMarkDistributed = false;
            lazyUseCtsTransaction = false;
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
            if (XLog.XProtocolLogger.isDebugEnabled()) {
                historySql.add("cancel query");
                sessionSql.add("cancel query");
            }
            final XPacket packet =
                new XPacket(sessionId, Polarx.ClientMessages.Type.SESS_KILL_VALUE,
                    PolarxSession.KillSession.newBuilder()
                        .setType(PolarxSession.KillSession.KillType.QUERY)
                        .setXSessionId(sessionId)
                        .build());
            client.send(packet, true);
        } finally {
            synchronized (this) {
                setLastException(CANCEL_EXCEPTION, false);
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
        dataNotify(true);
    }

    private void dataNotify(boolean with_lock) {
        XResult probe = lastRequest.get();
        if (probe != null) {
            // Do pkt timing first.
            probe.recordPktResponse();
            // Do notify if needed.
            if (XConfig.GALAXY_X_PROTOCOL) {
                probe = lastUserRequest.get();
                if (null == probe) {
                    return;
                }
            }
            final boolean needPush = probe.getDataNotify() != null;
            if (needPush) {
                final XResult real = XConfig.GALAXY_X_PROTOCOL ? lastUserRequest.get() : lastRequest.get();
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
        dataNotify(false); // Never hold the lock(deadlock may occur) because this may invoke from other thread.
    }

    public void kill(boolean pushKilled) {
        final Status nowStatus = status; // Read only, and no status lock needed.
        if (Status.Init == nowStatus || Status.Closed == nowStatus) {
            return; // Ignore inactive session.
        }
        try {
            if (XLog.XProtocolLogger.isDebugEnabled()) {
                historySql.add("kill query");
                sessionSql.add("kill query");
            }
            final XPacket packet =
                new XPacket(sessionId, Polarx.ClientMessages.Type.SESS_KILL_VALUE,
                    PolarxSession.KillSession.newBuilder()
                        .setType(PolarxSession.KillSession.KillType.CONNECTION)
                        .setXSessionId(sessionId)
                        .build());
            client.send(packet, true);
        } finally {
            synchronized (this) {
                setLastException(KILL_EXCEPTION, true);
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

    public void tokenOffer(int tokenKb) {
        final Status nowStatus = status; // Read only, and no status lock needed.
        if (Status.Init == nowStatus || Status.Closed == nowStatus) {
            return; // Ignore inactive session.
        }
        final XPacket packet =
            new XPacket(sessionId, Polarx.ClientMessages.Type.TOKEN_OFFER_VALUE,
                PolarxSql.TokenOffer.newBuilder()
                    .setToken(tokenKb)
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
                // Memory limitation by server so no check needed here. Ignore the row count limit.
                packetQueue.put(0 == consume ? packets : packets.subList(consume, packets.size()));
                boolean gotData = false;
                for (XPacket packet : packets) {
                    if (Polarx.ServerMessages.Type.RESULTSET_ROW_VALUE == packet.getType() ||
                        Polarx.ServerMessages.Type.RESULTSET_CHUNK_VALUE == packet.getType() ||
                        Polarx.ServerMessages.Type.RESULTSET_TSO_VALUE == packet.getType() ||
                        Polarx.ServerMessages.Type.RESULTSET_TOKEN_DONE_VALUE == packet.getType() ||
                        Polarx.ServerMessages.Type.RESULTSET_FETCH_DONE_VALUE == packet.getType() ||
                        Polarx.ServerMessages.Type.ERROR_VALUE == packet.getType() ||
                        Polarx.ServerMessages.Type.RESULTSET_FETCH_DONE_MORE_RESULTSETS_VALUE == packet.getType() ||
                        // Special case that isDataReady may consume till RESULTSET_FETCH_DONE_VALUE,
                        // and we still need notify when exec ok occurs.
                        Polarx.ServerMessages.Type.SQL_STMT_EXECUTE_OK_VALUE == packet.getType()) {
                        gotData = true;
                        break;
                    }
                }
                if (gotData) {
                    dataNotify();
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

    public void setLazyMarkDistributed() {
        this.lazyMarkDistributed = true;
    }

    public void setLazySnapshotSeq(long lazySnapshotSeq) {
        this.lazySnapshotSeq = lazySnapshotSeq;
    }

    public void setLazyCommitSeq(long lazyCommitSeq) {
        this.lazyCommitSeq = lazyCommitSeq;
    }

    // Stash sequence info.
    private boolean isStashed = false;
    private boolean stashUseCtsTransaction = false;
    private boolean stashMarkDistributed = false;
    private long stashSnapshotSeq = -1L;
    private long stashCommitSeq = -1L;

    public boolean stashTransactionSequence() {
        if (isStashed) {
            return false;
        }
        isStashed = true;
        stashUseCtsTransaction = lazyUseCtsTransaction;
        stashMarkDistributed = lazyMarkDistributed;
        stashSnapshotSeq = lazySnapshotSeq;
        stashCommitSeq = lazyCommitSeq;
        lazyUseCtsTransaction = false;
        lazyMarkDistributed = false;
        lazySnapshotSeq = -1;
        lazyCommitSeq = -1;
        return true;
    }

    public void stashPopTransactionSequence() {
        isStashed = false;
        lazyUseCtsTransaction = stashUseCtsTransaction;
        lazyMarkDistributed = stashMarkDistributed;
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
        } else if (Status.AutoCommit == nowStatus && lastRequest.get() != null) {
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
    private XResult getResultSet(XConnection connection, long startNanos, boolean ignoreResult, BytesSql sql,
                                 XPacketBuilder retransmit, XResult.RequestType requestType, String extra)
        throws SQLException {
        final XResult last = new XResult(connection, packetQueue, lastRequest.get(),
            startNanos, startNanos + connection.actualTimeoutNanos(),
            startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
            ignoreResult, requestType, sql,
            connection.getDefaultTokenKb(), retransmit, extra);
        lastRequest.set(last);
        lastUserRequest.set(last);
        // Use stream mode when we don't care about the result of query/update.
        // This is useful when set environment before query.
        return last;
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
                lastRequest.set(new XResult(connection, packetQueue, lastRequest.get(),
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                    true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect([+no_error])"),
                    connection.getDefaultTokenKb(), null, null));
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
                                          BytesSql nativeSql, byte[] traceId, boolean ignoreResult)
        throws SQLException {
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

        // hint
        if (traceId != null) {
            execPlan.setTraceId(ByteString.copyFrom(traceId));
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
        execPlan.setToken(connection.getDefaultTokenKb());
        if (lazyUseCtsTransaction) {
            execPlan.setUseCtsTransaction(true);
            if (null == extra) {
                extra = "use_cts;";
            } else {
                extra += "use_cts;";
            }
            lazyUseCtsTransaction = false;
        }
        if (lazyMarkDistributed) {
            execPlan.setMarkDistributed(true);
            if (null == extra) {
                extra = "mark_distributed;";
            } else {
                extra += "mark_distributed;";
            }
            lazyMarkDistributed = false;
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
        // Note: Feedback, compact meta and capabilities are set by caller in execPlan.

        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = ignoreResult;
        }

        final PolarxExecPlan.AnyPlan originalPlan = pureDigest && !forceCache ? execPlan.getPlan() : null;
        if (pureDigest) {
            execPlan.clearPlan();
        } else {
            // add audit str
            final JsonFormat format = new JsonFormat();
            execPlan.setAuditStr(ByteString.copyFromUtf8(format.printToString(execPlan.getPlan())));
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
                lastRequest.set(new XResult(connection, packetQueue, lastRequest.get(),
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                    true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect_close"),
                    connection.getDefaultTokenKb(), null, null));
            }
        } catch (Throwable t) {
            // This exception should drop the connection.
            throw GeneralUtil.nestedException(setLastException(t, true));
        }
        // Finish block mode outside here so exception will not fatal the session.
        if (!ignoreResult && !connection.isStreamMode()) {
            result.finishBlockMode();
        }
        return ignoreResult ? null : result;
    }

    private void execPreDealing(XConnection connection, BytesSql sql, boolean isUpdate) throws SQLException {
        if (XConnectionManager.getInstance().isEnableTrxLeakCheck()) {
            String tempSql = sql.toString();
            if (StringUtils.containsIgnoreCase(tempSql, "XA START ") ||
                StringUtils.containsIgnoreCase(tempSql, "XA PREPARE ")) {
                xaStatus = tempSql;
            } else if (StringUtils.containsIgnoreCase(tempSql, "XA ROLLBACK ") ||
                StringUtils.containsIgnoreCase(tempSql, "XA COMMIT ")) {
                xaStatus = null;
            } else if (tempSql.equalsIgnoreCase("begin") ||
                StringUtils.containsIgnoreCase(tempSql, "*/begin") ||
                StringUtils.containsIgnoreCase(tempSql, "START TRANSACTION")) {
                transactionStatus = true;
            } else if (tempSql.equalsIgnoreCase("rollback") || tempSql.equalsIgnoreCase("commit") ||
                StringUtils.containsIgnoreCase(tempSql, "*/rollback") ||
                StringUtils.containsIgnoreCase(tempSql, "*/commit")) {
                transactionStatus = false;
            }
        }

        final XDataSource dataSource = connection.getDataSource();
        if (dataSource != null) {
            if (isUpdate) {
                dataSource.getUpdateCount().getAndIncrement();
            } else {
                dataSource.getQueryCount().getAndIncrement();
            }
        }
        if (isUpdate) {
            perfCollection.getUpdateCount().getAndIncrement();
        } else {
            perfCollection.getQueryCount().getAndIncrement();
        }

        initForRequest();
        applyDefaultEncodingMySQL(connection);

        if (XLog.XProtocolLogger.isDebugEnabled()) {
            traceCtsInfo();

            historySql.add(sql.toString());
            sessionSql.add(sql.toString());
        }
    }

    public XResult execQuery(XConnection connection, BytesSql sql, byte[] hint, List<PolarxDatatypes.Any> args,
                             boolean ignoreResult, ByteString digest) throws SQLException {
        return execQuery(connection, sql, hint, args, ignoreResult, digest, null);
    }

    public synchronized XResult execQuery(XConnection connection, BytesSql sql, byte[] hint,
                                          List<PolarxDatatypes.Any> args,
                                          boolean ignoreResult, ByteString digest, String returning)
        throws SQLException {
        if (returning != null && XConfig.GALAXY_X_PROTOCOL) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION, "Returning not supported in galaxy.");
        }

        execPreDealing(connection, sql, false);

        // handle explain
        boolean explain = isExplain(hint);
        if (explain) {
            hint = Arrays.copyOfRange(hint, 8, hint.length);
        }

        // Send request.
        final PolarxSql.StmtExecute.Builder builder = PolarxSql.StmtExecute.newBuilder();

        final String returningHint = null != returning ? "/* +returning fields(" + returning + ") */ " : "";
        boolean setStmt = true;
        // Dealing cache.
        if (!ignoreResult && !noCache && digest != null && supportPlanCache()) {
            // Always use cache.
            setStmt = false;
            builder.setStmtDigest(digest);
            if (hint != null) {
                builder.setHint(ByteString.copyFrom(hint));
            }
            final XDataSource dataSource = connection.getDataSource();
            if (dataSource != null) {
                dataSource.getCacheSqlQuery().getAndIncrement();
            }
        } else if (!XConfig.GALAXY_X_PROTOCOL) {
            builder.setHint(ByteString.copyFromUtf8(returningHint));
        }

        String extra = null;
        if (setStmt) {
            try {
                if (explain && !isExplain(sql.getBytes())) {
                    builder.setStmt(EXPLAIN_PRE.concat(sql.byteString(toJavaEncoding(getRequestEncodingMySQL()))));
                } else {
                    if ((builder.getStmtDigest() == null || builder.getStmtDigest().isEmpty()) && hint != null) {
                        // digest is null meaning it's the first interview for this sql
                        // hint ntb added in the front of sql
                        builder.setStmt(ByteString.copyFrom(hint)
                            .concat(sql.byteString(toJavaEncoding(getRequestEncodingMySQL()))));
                    } else {
                        builder.setStmt(sql.byteString(toJavaEncoding(getRequestEncodingMySQL())));
                    }
                }
            } catch (UnsupportedCharsetException e) {
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
        builder.setToken(connection.getDefaultTokenKb());

        extra = setLazyTrxVariables(builder, extra);

        if (chunkResult && supportChunkResult()) {
            builder.setChunkResult(true);
        }
        if (connection.isWithFeedback() && supportFeedback()) {
            builder.setFeedBack(true);
        }
        if (connection.getCapabilities() != 0) {
            builder.setCapabilities(connection.getCapabilities());
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
            result = getResultSet(connection, startNanos, ignoreResult, sql, retransmit, XResult.RequestType.SQL_QUERY,
                extra);
            if (postOp) {
                lastRequest.set(new XResult(connection, packetQueue, lastRequest.get(),
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                    true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect_close"),
                    connection.getDefaultTokenKb(), null, null));
            }
        } catch (Throwable t) {
            // This exception should drop the connection.
            throw GeneralUtil.nestedException(setLastException(t, true));
        }
        // Finish block mode outside here so exception will not fatal the session.
        if (!ignoreResult && !connection.isStreamMode()) {
            result.finishBlockMode();
        }
        return ignoreResult ? null : result;
    }

    private boolean isExplain(byte[] hint) {
        if (hint == null || hint.length < 8) {
            return false;
        }
        return hint[0] == 101 && hint[1] == 120 && hint[2] == 112 && hint[3] == 108 && hint[4] == 97 && hint[5] == 105
            && hint[6] == 110 && hint[7] == 32;
    }

    public synchronized XResult execGalaxyPrepare(
        XConnection connection, BytesSql sql, byte[] hint, ByteString digest, List<GPTable> tables, ByteString params,
        int paramNum, boolean ignoreResult, boolean isUpdate) throws SQLException {
        execPreDealing(connection, sql, isUpdate);

        assert !isExplain(hint); // explain with galaxy prepare is not allowed

        // Send request.
        final PolarxSql.GalaxyPrepareExecute.Builder builder = PolarxSql.GalaxyPrepareExecute.newBuilder();

        final ByteString extraPrefix = 0 == tables.size() ? ByteString.copyFromUtf8("/*" + lastDB + "*/") : null;
        final boolean setStmt;
        if (!noCache && digest != null && supportPlanCache()) {
            // Always use cache.
            setStmt = false;
            builder.setStmtDigest(digest);
        } else {
            setStmt = true;
            try {
                final String encoding = toJavaEncoding(getRequestEncodingMySQL());
                builder.setStmt(
                    null == extraPrefix ? sql.byteString(encoding) : extraPrefix.concat(sql.byteString(encoding)));
            } catch (UnsupportedCharsetException e) {
                XLog.XLogLogger.error(e);
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CONNECTION, e.getMessage());
            }
        }

        // always set hint
        if (hint != null) {
            builder.setHint(ByteString.copyFrom(hint));
        }

        // Set fields.
        if (tables != null && !tables.isEmpty()) {
            final PolarxSql.GalaxyPrepareTableData.Builder tableBuilder = PolarxSql.GalaxyPrepareTableData.newBuilder();
            for (GPTable tableData : tables) {
                tableBuilder.setTableIndex(tableData.getTableParamIndex());
                tableBuilder.setTableName(ByteString.copyFromUtf8(tableData.getTableName()));
                builder.addTables(tableBuilder);
            }
        }
        if (params != null && !params.isEmpty()) {
            builder.setParam(params);
            builder.setParamNum(paramNum);
        }
        if (connection.isCompactMetadata()) {
            builder.setCompactMetadata(true);
        }
        if (lazyUseDB != null) {
            builder.setDbName(lazyUseDB);
            lazyUseDB = null;
        }
        if (!XConfig.GALAXY_X_PROTOCOL) {
            builder.setResetError(!lastIgnore);
        }
        builder.setToken(connection.getDefaultTokenKb());
        String extra = null;
        if (lazyUseCtsTransaction) {
            builder.setUseCtsTransaction(true);
            if (null == extra) {
                extra = "use_cts;";
            } else {
                extra += "use_cts;";
            }
            lazyUseCtsTransaction = false;
        }
        if (lazyMarkDistributed) {
            builder.setMarkDistributed(true);
            if (null == extra) {
                extra = "mark_distributed;";
            } else {
                extra += "mark_distributed;";
            }
            lazyMarkDistributed = false;
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
            builder.setResultSetType(PolarxSql.GalaxyPrepareExecute.ResultSetType.CHUNK_V1);
        }
        if (connection.isWithFeedback() && supportFeedback()) {
            builder.setFeedBack(true);
        }
        if (connection.getCapabilities() != 0) {
            builder.setCapabilities(connection.getCapabilities());
        }

        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = ignoreResult;
        }
        final XPacket packet =
            new XPacket(sessionId, Polarx.ClientMessages.Type.GALAXY_PREPARE_EXECUTE_VALUE, builder.build());
        final XResult result;
        try {
            final long startNanos = System.nanoTime();
            final boolean postOp = send_internal(connection, packet, ignoreResult);
            if (!ignoreResult) {
                addActive();
            }
            final XPacketBuilder retransmit;
            if (!setStmt && !forceCache) {
                retransmit =
                    new XPacketBuilder(sessionId, builder, sql, toJavaEncoding(getRequestEncodingMySQL()), extraPrefix);
            } else {
                retransmit = null;
            }
            result = getResultSet(
                connection, startNanos, ignoreResult, sql, retransmit, XResult.RequestType.SQL_PREPARE_EXECUTE, extra);
            if (postOp) {
                lastRequest.set(new XResult(connection, packetQueue, lastRequest.get(),
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                    true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect_close"),
                    connection.getDefaultTokenKb(), null, null));
            }
        } catch (Throwable t) {
            // This exception should drop the connection.
            throw GeneralUtil.nestedException(setLastException(t, true));
        }
        // Finish block mode outside here so exception will not fatal the session.
        if (!ignoreResult && (isUpdate || !connection.isStreamMode())) {
            result.finishBlockMode();
        }
        return ignoreResult ? null : result;
    }

    // Return 0 if ignore result.
    public synchronized XResult execUpdate(XConnection connection, BytesSql sql, byte[] hint,
                                           List<PolarxDatatypes.Any> args,
                                           boolean ignoreResult, ByteString digest) throws SQLException {
        if (digest != null && XConfig.GALAXY_X_PROTOCOL) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION, "Digest not supported in galaxy.");
        }

        execPreDealing(connection, sql, true);

        boolean explain = isExplain(hint);
        if (explain) {
            hint = Arrays.copyOfRange(hint, 8, hint.length);
        }

        // Send request.
        final PolarxSql.StmtExecute.Builder builder = PolarxSql.StmtExecute.newBuilder();

        boolean setStmt = true;
        // Dealing cache.
        if (!ignoreResult && !noCache && digest != null && supportPlanCache()) {
            // Only query with no ignore can go the cache.

            // Always use cache.
            setStmt = false;
            builder.setStmtDigest(digest);
            builder.setHint(ByteString.copyFrom(hint));
            final XDataSource dataSource = connection.getDataSource();
            if (dataSource != null) {
                dataSource.getCacheSqlQuery().getAndIncrement();
            }
        }
        String extra = null;
        if (setStmt) {
            try {
                if (explain && !isExplain(sql.getBytes())) {
                    builder.setStmt(EXPLAIN_PRE.concat(sql.byteString(toJavaEncoding(getRequestEncodingMySQL()))));
                } else {
                    if ((builder.getStmtDigest() == null || builder.getStmtDigest().isEmpty()) && hint != null) {
                        // digest is null meaning it's the first interview for this sql
                        // hint ntb added in the front of sql
                        builder.setStmt(ByteString.copyFrom(hint)
                            .concat(sql.byteString(toJavaEncoding(getRequestEncodingMySQL()))));
                    } else {
                        builder.setStmt(sql.byteString(toJavaEncoding(getRequestEncodingMySQL())));
                    }
                }
            } catch (UnsupportedCharsetException e) {
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

        extra = setLazyTrxVariables(builder, extra);

        if (connection.isWithFeedback() && supportFeedback()) {
            builder.setFeedBack(true);
        }
        if (connection.getCapabilities() != 0) {
            builder.setCapabilities(connection.getCapabilities());
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
            result = new XResult(connection, packetQueue, lastRequest.get(),
                startNanos, startNanos + connection.actualTimeoutNanos(),
                startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                ignoreResult, XResult.RequestType.SQL_UPDATE, sql,
                connection.getDefaultTokenKb(), retransmit, extra); // Only care about ignore or not.
            lastRequest.set(result);
            lastUserRequest.set(result);
            if (postOp) {
                lastRequest.set(new XResult(connection, packetQueue, result,
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                    true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect_close"),
                    connection.getDefaultTokenKb(), null, null));
            }
        } catch (Throwable t) {
            // This exception should drop the connection.
            throw GeneralUtil.nestedException(setLastException(t, true));
        }
        // Finish block mode outside here so exception will not fatal the session.
        if (!ignoreResult) {
            result.finishBlockMode();
        }
        return result;
    }

    private String setLazyTrxVariables(PolarxSql.StmtExecute.Builder builder, String extra) {
        if (lazyUseCtsTransaction) {
            builder.setUseCtsTransaction(true);
            if (null == extra) {
                extra = "use_cts;";
            } else {
                extra += "use_cts;";
            }
            lazyUseCtsTransaction = false;
        }
        if (lazyMarkDistributed) {
            builder.setMarkDistributed(true);
            if (null == extra) {
                extra = "mark_distributed;";
            } else {
                extra += "mark_distributed;";
            }
            lazyMarkDistributed = false;
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
        return extra;
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

        final boolean pendingRequests = lastIgnore;
        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = false;
        }

        final XPacket packet = new XPacket(sessionId, Polarx.ClientMessages.Type.GET_TSO_VALUE, builder.build());
        final XResult result;
        try {
            final long startNanos = System.nanoTime();
            // Caused by special optimize of get TSO(execute on receive thread).
            // We should finish pipeline first.
            if (XConfig.GALAXY_X_PROTOCOL && pendingRequests) {
                // Need add close expect message.
                client.send(new XPacket(sessionId, Polarx.ClientMessages.Type.EXPECT_CLOSE_VALUE,
                    PolarxExpect.Close.newBuilder().build()), true);
                lastIgnore = false;
                final XResult last = new XResult(connection, packetQueue, lastRequest.get(),
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + connection.actualTimeoutNanos(), // for TSO never wait too long
                    true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect_close"),
                    connection.getDefaultTokenKb(), null, null);
                lastRequest.set(last);
                last.finishBlockMode();
            } else if (pendingRequests) {
                // finish pending request for TSO of XRPC
                flushNetwork();
                final XResult last = lastRequest.get();
                if (last != null) {
                    last.finishBlockMode();
                }
            }
            // Then normal TSO request.
            client.send(packet, true);
            addActive();
            result = new XResult(connection, packetQueue, lastRequest.get(), startNanos,
                startNanos + connection.actualTimeoutNanos(), startNanos + connection.actualTimeoutNanos(),
                // for TSO never wait too long
                false, XResult.RequestType.FETCH_TSO, BytesSql.getBytesSql(GET_TSO_SQL),
                connection.getDefaultTokenKb(), null, null);
            lastRequest.set(result);
            lastUserRequest.set(result);
        } catch (Throwable t) {
            // This exception should drop the connection.
            throw GeneralUtil.nestedException(setLastException(t, true));
        }
        // Finish block mode outside here so exception will not fatal the session.
        result.finishBlockMode();
        if (result.getTsoErrorNo() != XResultUtil.GTS_PROTOCOL_SUCCESS) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CONNECTION,
                this + " failed to get TSO. errno " + result.getTsoErrorNo());
        }
        return result.getTsoValue();
    }

    public synchronized void handleAutoSavepoint(XConnection connection, String name,
                                                 PolarxExecPlan.AutoSp.Operation op,
                                                 boolean ignoreResult) throws SQLException {
        if (XConfig.GALAXY_X_PROTOCOL || !isXRPC()) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION, "Auto savepoint opt only support xrpc.");
        }

        initForRequest();

        String sql;
        switch (op) {
        case SET:
            sql = "SAVEPOINT `" + name + "`";
            break;
        case RELEASE:
            sql = "RELEASE SAVEPOINT `" + name + "`";
            break;
        case ROLLBACK:
            sql = "ROLLBACK SAVEPOINT `" + name + "`";
            break;
        default:
            sql = null;
        }

        if (XLog.XProtocolLogger.isDebugEnabled()) {
            historySql.add(sql);
            sessionSql.add(sql);
        }

        final PolarxExecPlan.AutoSp.Builder builder = PolarxExecPlan.AutoSp.newBuilder();
        builder.setSpName(ByteString.copyFromUtf8(name));
        builder.setOp(op);
        builder.setResetError(!lastIgnore);

        lastIgnore = ignoreResult;

        final XPacket packet = new XPacket(sessionId, Polarx.ClientMessages.Type.AUTO_SP_VALUE, builder.build());
        XResult result;
        try {
            final long startNanos = System.nanoTime();

            // Send request.
            client.send(packet, !ignoreResult);

            if (!ignoreResult) {
                addActive();
            }

            // Get result.
            result = new XResult(connection, packetQueue, lastRequest.get(),
                startNanos, startNanos + connection.actualTimeoutNanos(),
                startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                ignoreResult, XResult.RequestType.AUTO_SP, BytesSql.getBytesSql(sql),
                connection.getDefaultTokenKb(), null, null); // Only care about ignore or not.

            lastRequest.set(result);
            lastUserRequest.set(result);
        } catch (Throwable t) {
            // This exception should drop the connection.
            throw GeneralUtil.nestedException(setLastException(t, true));
        }

        if (!ignoreResult) {
            result.finishBlockMode();
        }
    }

    private volatile long connectionId = -1;

    public long getConnectionId() {
        return connectionId != -1 ? connectionId : 0;
    }

    public void refereshConnetionId(XConnection connection) throws SQLException {
        // Try cached first.
        if (connectionId != -1 || status == Status.AutoCommit) {
            return;
        }

        // Query.
        Long connId = null;
        final XResult result =
            execQuery(connection, BytesSql.getBytesSql("select connection_id()"), null, null, false, null);
        while (result.next() != null) {
            try {
                Number res = (Number) XResultUtil.resultToObject(
                    result.getMetaData().get(0), result.current().getRow().get(0), true,
                    TimeZone.getDefault()).getKey();
                if (null == connId) {
                    connId = res.longValue();
                }
            } catch (Exception e) {
                LOGGER.error(e);
            }
        }
        if (null == connId) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CONNECTION,
                this + " failed to get connection id.");
        }
        connectionId = connId;
    }

    private Boolean flagMessageTimestamp = null;
    private Boolean flagQueryCache = null;
    private Boolean flagQueryChunk = null;
    private Boolean flagSingleShardOptimization = null;
    private Boolean flagFeedback = null;
    private Boolean flagRawString = null;
    private Boolean flagXRPC = null;
    private Boolean flagMarkDistributed = null;

    public boolean supportMessageTimestamp() {
        // disabled in XConnectionManager when galaxy protocol
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
            if (minor.equals("local") || minor.equals("log")) {
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
        } else if (isXRPC()) {
            return flagMessageTimestamp = true; // Enable by default.
        }
        return flagMessageTimestamp = false;
    }

    public boolean supportPlanCache() {
        // disabled in XConnectionManager when galaxy protocol
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
            if (minor.equals("local") || minor.equals("log")) {
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
            return flagQueryCache = true; // Enable by default.
        } else if (isXRPC()) {
            return flagQueryCache = true; // Enable by default.
        }
        return flagQueryCache = false;
    }

    public boolean supportChunkResult() {
        // disabled in XConnectionManager when galaxy protocol
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
            if (minor.equals("local") || minor.equals("log")) {
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
            return flagQueryChunk = true; // Enable by default.
        } else if (isXRPC()) {
            return flagQueryChunk = true; // Enable by default.
        }
        return flagQueryChunk = false;
    }

    public boolean supportSingleShardOptimization() {
        // disabled in XConnectionManager when galaxy protocol
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
            if (minor.equals("local") || minor.equals("log")) {
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
            return flagSingleShardOptimization = true; // Enable by default.
        } else if (isXRPC()) {
            return flagSingleShardOptimization = true; // Enable by default.
        }
        return flagSingleShardOptimization = false;
    }

    public boolean supportFeedback() {
        // disabled in XConnectionManager when MySQL80(galaxy protocol or XRPC)
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
            if (minor.equals("local") || minor.equals("log")) {
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
            return flagFeedback = false; // Disable by default.
        }
        return flagFeedback = false;
    }

    public boolean supportRawString() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }

        if (flagRawString != null) {
            return flagRawString;
        }
        if (!client.isActive()) {
            return false; // Initializing.
        }

        if (client.getBaseVersion() == XClient.DnBaseVersion.DN_X_CLUSTER) {
            final String minor = client.getMinorVersion();
            if (minor.equals("local") || minor.equals("log")) {
                // Reuse the timestamp option.
                return flagRawString = XConfig.X_LOCAL_ENABLE_RAW_STRING;
            }
            try {
                final long ver = Long.parseLong(minor);
                if (ver >= 20211104L) { // Since 1.6.1.0 20211104
                    return flagRawString = true;
                }
            } catch (Exception e) {
                XLog.XLogLogger.error(this + " unknown xdb minor version: " + minor);
            }
        } else if (client.getBaseVersion() == XClient.DnBaseVersion.DN_RDS_80_X_CLUSTER) {
            return flagRawString = true; // Enable by default.
        } else if (isXRPC()) {
            return flagRawString = true; // Enable by default.
        }
        return flagRawString = false;
    }

    public boolean isXRPC() {
        if (flagXRPC != null) {
            return flagXRPC;
        }

        if (!client.isActive()) {
            return false; // Initializing.
        }

        final Object newRpc = client.getGlobalVariablesL().get("new_rpc");
        if (newRpc instanceof String && ((String) newRpc).equalsIgnoreCase("ON")) {
            return flagXRPC = true;
        }
        return flagXRPC = false;
    }

    public PolarxPhysicalBackfill.GetFileInfoOperator execCheckFileExistence(XConnection connection,
                                                                             PolarxPhysicalBackfill.GetFileInfoOperator.Builder builder)
        throws SQLException {
        final XDataSource dataSource = connection.getDataSource();
        if (dataSource != null) {
            dataSource.getTsoCount().getAndIncrement();
        }
        perfCollection.getTsoCount().getAndIncrement();

        initForRequest();

        final String SQL_MARK = "checkFileExistence";
        if (XLog.XProtocolLogger.isDebugEnabled()) {
            historySql.add(SQL_MARK);
            sessionSql.add(SQL_MARK);
        }

        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = false;
        }

        final XPacket packet =
            new XPacket(sessionId, Polarx.ClientMessages.Type.FILE_OPERATION_GET_FILE_INFO_VALUE, builder.build());
        final XResult result;
        try {
            final long startNanos = System.nanoTime();
            // Caused by special optimize of get TSO(execute on receive thread).
            // We should finish pipeline first.
            if (XConfig.GALAXY_X_PROTOCOL && lastIgnore) {
                // Need add close expect message.
                client.send(new XPacket(sessionId, Polarx.ClientMessages.Type.EXPECT_CLOSE_VALUE,
                    PolarxExpect.Close.newBuilder().build()), true);
                lastIgnore = false;
                final XResult last = new XResult(connection, packetQueue, lastRequest.get(),
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + connection.actualTimeoutNanos(), // for TSO never wait too long
                    true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect_close"),
                    connection.getDefaultTokenKb(), null, null);
                lastRequest.set(last);
                last.finishBlockMode();
            }
            // Then normal TSO request.
            client.send(packet, true);
            addActive();
            result = new XResult(connection, packetQueue, lastRequest.get(), startNanos,
                startNanos + connection.actualTimeoutNanos(), startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                false, XResult.RequestType.GET_FILE_INFO, BytesSql.getBytesSql(SQL_MARK),
                connection.getDefaultTokenKb(), null, null);
            lastUserRequest.set(result);
            lastRequest.set(result);
        } catch (Throwable t) {
            lastException.set(t); // This exception should drop the connection.
            throw t;
        }
        // Finish block mode outside here so exception will not fatal the session.
        result.finishBlockMode();

        return result.getIbdFileInfo();
    }

    public PolarxPhysicalBackfill.TransferFileDataOperator execReadBufferFromFile(XConnection connection,
                                                                                  PolarxPhysicalBackfill.TransferFileDataOperator.Builder transferFile)
        throws SQLException {
        final XDataSource dataSource = connection.getDataSource();
        if (dataSource != null) {
            dataSource.getTsoCount().getAndIncrement();
        }
        perfCollection.getTsoCount().getAndIncrement();

        initForRequest();

        final String SQL_MARK = "getBufferFromFile";
        if (XLog.XProtocolLogger.isDebugEnabled()) {
            historySql.add(SQL_MARK);
            sessionSql.add(SQL_MARK);
        }

        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = false;
        }

        final XPacket packet =
            new XPacket(sessionId, Polarx.ClientMessages.Type.FILE_OPERATION_TRANSFER_FILE_DATA_VALUE,
                transferFile.build());
        final XResult result;
        try {
            final long startNanos = System.nanoTime();
            // Caused by special optimize of get TSO(execute on receive thread).
            // We should finish pipeline first.
            if (XConfig.GALAXY_X_PROTOCOL && lastIgnore) {
                // Need add close expect message.
                client.send(new XPacket(sessionId, Polarx.ClientMessages.Type.EXPECT_CLOSE_VALUE,
                    PolarxExpect.Close.newBuilder().build()), true);
                lastIgnore = false;
                final XResult lastResult = new XResult(connection, packetQueue, lastRequest.get(),
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + connection.actualTimeoutNanos(), // for TSO never wait too long
                    true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect_close"),
                    connection.getDefaultTokenKb(), null, null);
                lastRequest.set(lastResult);
                lastResult.finishBlockMode();
            }
            // Then normal TSO request.
            client.send(packet, true);
            addActive();
            result = new XResult(connection, packetQueue, lastRequest.get(), startNanos,
                startNanos + connection.actualTimeoutNanos(), startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                false, XResult.RequestType.TRANSFER_FILE, BytesSql.getBytesSql(SQL_MARK),
                connection.getDefaultTokenKb(), null, null);
            lastUserRequest.set(result);
            lastRequest.set(result);
        } catch (Throwable t) {
            lastException.set(t); // This exception should drop the connection.
            throw t;
        }
        // Finish block mode outside here so exception will not fatal the session.
        result.finishBlockMode();
        return result.getBufferFromIbdFile();
    }

    public long execTransferFile(XConnection connection,
                                 PolarxPhysicalBackfill.TransferFileDataOperator.Builder transferFile)
        throws SQLException {
        final XDataSource dataSource = connection.getDataSource();
        if (dataSource != null) {
            //dataSource.getTsoCount().getAndIncrement();
        }
        //perfCollection.getTsoCount().getAndIncrement();

        initForRequest();

        final String SQL_MARK = "transferFile";
        if (XLog.XProtocolLogger.isDebugEnabled()) {
            historySql.add(SQL_MARK);
            sessionSql.add(SQL_MARK);
        }

        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = false;
        }

        final XPacket packet =
            new XPacket(sessionId, Polarx.ClientMessages.Type.FILE_OPERATION_TRANSFER_FILE_DATA_VALUE,
                transferFile.build());
        final XResult result;
        try {
            final long startNanos = System.nanoTime();
            // Caused by special optimize of get TSO(execute on receive thread).
            // We should finish pipeline first.
            if (XConfig.GALAXY_X_PROTOCOL && lastIgnore) {
                // Need add close expect message.
                client.send(new XPacket(sessionId, Polarx.ClientMessages.Type.EXPECT_CLOSE_VALUE,
                    PolarxExpect.Close.newBuilder().build()), true);
                lastIgnore = false;
                final XResult lastResult = new XResult(connection, packetQueue, lastRequest.get(),
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + connection.actualTimeoutNanos(), // for TSO never wait too long
                    true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect_close"),
                    connection.getDefaultTokenKb(), null, null);
                lastRequest.set(lastResult);
                lastResult.finishBlockMode();
            }
            // Then normal TSO request.
            client.send(packet, true);
            addActive();
            result = new XResult(connection, packetQueue, lastRequest.get(), startNanos,
                startNanos + connection.actualTimeoutNanos(), startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                false, XResult.RequestType.TRANSFER_FILE, BytesSql.getBytesSql(SQL_MARK),
                connection.getDefaultTokenKb(), null, null);
            lastUserRequest.set(result);
            lastRequest.set(result);
        } catch (Throwable t) {
            lastException.set(t); // This exception should drop the connection.
            throw t;
        }
        // Finish block mode outside here so exception will not fatal the session.
        result.finishBlockMode();
        return result.getTransferBufferSize();
    }

    public PolarxPhysicalBackfill.FileManageOperatorResponse execCloneFile(XConnection connection,
                                                                           PolarxPhysicalBackfill.FileManageOperator.Builder builder)
        throws SQLException {
        final XDataSource dataSource = connection.getDataSource();
        if (dataSource != null) {
            dataSource.getTsoCount().getAndIncrement();
        }
        perfCollection.getTsoCount().getAndIncrement();

        initForRequest();

        final String SQL_MARK = "cloneFile";
        if (XLog.XProtocolLogger.isDebugEnabled()) {
            historySql.add(SQL_MARK);
            sessionSql.add(SQL_MARK);
        }

        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = false;
        }

        final XPacket packet =
            new XPacket(sessionId, Polarx.ClientMessages.Type.FILE_OPERATION_FILE_MANAGE_VALUE, builder.build());
        final XResult result;
        try {
            final long startNanos = System.nanoTime();
            // Caused by special optimize of get TSO(execute on receive thread).
            // We should finish pipeline first.
            if (XConfig.GALAXY_X_PROTOCOL && lastIgnore) {
                // Need add close expect message.
                client.send(new XPacket(sessionId, Polarx.ClientMessages.Type.EXPECT_CLOSE_VALUE,
                    PolarxExpect.Close.newBuilder().build()), true);
                lastIgnore = false;
                final XResult lastResult = new XResult(connection, packetQueue, lastRequest.get(),
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + connection.actualTimeoutNanos(), // for TSO never wait too long
                    true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect_close"),
                    connection.getDefaultTokenKb(), null, null);
                lastRequest.set(lastResult);
                lastResult.finishBlockMode();
            }
            // Then normal TSO request.
            client.send(packet, true);
            addActive();
            result = new XResult(connection, packetQueue, lastRequest.get(), startNanos,
                startNanos + connection.actualTimeoutNanos(), startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                false, XResult.RequestType.CLONE_FILE, BytesSql.getBytesSql(SQL_MARK),
                connection.getDefaultTokenKb(), null, null);
            lastUserRequest.set(result);
            lastRequest.set(result);
        } catch (Throwable t) {
            lastException.set(t); // This exception should drop the connection.
            throw t;
        }
        // Finish block mode outside here so exception will not fatal the session.
        result.finishBlockMode();

        return result.getFileManageOperatorResponse();
    }

    public PolarxPhysicalBackfill.FileManageOperatorResponse execDeleteTempIbdFile(XConnection connection,
                                                                                   PolarxPhysicalBackfill.FileManageOperator.Builder builder)
        throws SQLException {
        final XDataSource dataSource = connection.getDataSource();
        if (dataSource != null) {
            dataSource.getTsoCount().getAndIncrement();
        }
        perfCollection.getTsoCount().getAndIncrement();

        initForRequest();

        final String SQL_MARK = "deleteTempFile";
        if (XLog.XProtocolLogger.isDebugEnabled()) {
            historySql.add(SQL_MARK);
            sessionSql.add(SQL_MARK);
        }

        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = false;
        }

        final XPacket packet =
            new XPacket(sessionId, Polarx.ClientMessages.Type.FILE_OPERATION_FILE_MANAGE_VALUE, builder.build());
        final XResult result;
        try {
            final long startNanos = System.nanoTime();
            // Caused by special optimize of get TSO(execute on receive thread).
            // We should finish pipeline first.
            if (XConfig.GALAXY_X_PROTOCOL && lastIgnore) {
                // Need add close expect message.
                client.send(new XPacket(sessionId, Polarx.ClientMessages.Type.EXPECT_CLOSE_VALUE,
                    PolarxExpect.Close.newBuilder().build()), true);
                lastIgnore = false;
                final XResult lastResult = new XResult(connection, packetQueue, lastRequest.get(),
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + connection.actualTimeoutNanos(), // for TSO never wait too long
                    true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect_close"),
                    connection.getDefaultTokenKb(), null, null);
                lastRequest.set(lastResult);
                lastResult.finishBlockMode();
            }
            // Then normal TSO request.
            client.send(packet, true);
            addActive();
            result = new XResult(connection, packetQueue, lastRequest.get(), startNanos,
                startNanos + connection.actualTimeoutNanos(), startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                false, XResult.RequestType.DELETE_FILE, BytesSql.getBytesSql(SQL_MARK),
                connection.getDefaultTokenKb(), null, null);
            lastUserRequest.set(result);
            lastRequest.set(result);
        } catch (Throwable t) {
            lastException.set(t); // This exception should drop the connection.
            throw t;
        }
        // Finish block mode outside here so exception will not fatal the session.
        result.finishBlockMode();

        return result.getFileManageOperatorResponse();
    }

    public PolarxPhysicalBackfill.FileManageOperatorResponse execFallocateIbdFile(XConnection connection,
                                                                                  PolarxPhysicalBackfill.FileManageOperator.Builder builder)
        throws SQLException {
        final XDataSource dataSource = connection.getDataSource();
        if (dataSource != null) {
            dataSource.getTsoCount().getAndIncrement();
        }
        perfCollection.getTsoCount().getAndIncrement();

        initForRequest();

        final String SQL_MARK = "fallocateIdbFile";
        if (XLog.XProtocolLogger.isDebugEnabled()) {
            historySql.add(SQL_MARK);
            sessionSql.add(SQL_MARK);
        }

        if (!XConfig.GALAXY_X_PROTOCOL) {
            lastIgnore = false;
        }

        final XPacket packet =
            new XPacket(sessionId, Polarx.ClientMessages.Type.FILE_OPERATION_FILE_MANAGE_VALUE, builder.build());
        final XResult result;
        try {
            final long startNanos = System.nanoTime();
            // Caused by special optimize of get TSO(execute on receive thread).
            // We should finish pipeline first.
            if (XConfig.GALAXY_X_PROTOCOL && lastIgnore) {
                // Need add close expect message.
                client.send(new XPacket(sessionId, Polarx.ClientMessages.Type.EXPECT_CLOSE_VALUE,
                    PolarxExpect.Close.newBuilder().build()), true);
                lastIgnore = false;
                final XResult lastResult = new XResult(connection, packetQueue, lastRequest.get(),
                    startNanos, startNanos + connection.actualTimeoutNanos(),
                    startNanos + connection.actualTimeoutNanos(), // for TSO never wait too long
                    true, XResult.RequestType.EXPECTATIONS, BytesSql.getBytesSql("expect_close"),
                    connection.getDefaultTokenKb(), null, null);
                lastRequest.set(lastResult);
                lastResult.finishBlockMode();
            }
            // Then normal TSO request.
            client.send(packet, true);
            addActive();
            result = new XResult(connection, packetQueue, lastRequest.get(), startNanos,
                startNanos + connection.actualTimeoutNanos(), startNanos + XConfig.DEFAULT_TOTAL_TIMEOUT_NANOS,
                false, XResult.RequestType.FALLOCATE_FILE, BytesSql.getBytesSql(SQL_MARK),
                connection.getDefaultTokenKb(), null, null);
            lastUserRequest.set(result);
            lastRequest.set(result);
        } catch (Throwable t) {
            lastException.set(t); // This exception should drop the connection.
            throw t;
        }
        // Finish block mode outside here so exception will not fatal the session.
        result.finishBlockMode();

        return result.getFileManageOperatorResponse();
    }

    public boolean supportMarkDistributed() {
        if (flagMarkDistributed != null) {
            return flagMarkDistributed;
        }

        if (!client.isActive()) {
            return false; // Initializing.
        }

        final Object markDistributed = client.getSessionVariablesL().get("innodb_mark_distributed");
        if (null != markDistributed) {
            return flagMarkDistributed = true;
        }
        return flagMarkDistributed = false;
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
