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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.XLog;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.packet.XPacket;
import com.alibaba.polardbx.rpc.packet.XPacketBuilder;
import com.alibaba.polardbx.rpc.packet.XPacketQueue;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxNotice;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.mysql.cj.x.protobuf.Polarx;
import com.mysql.cj.x.protobuf.PolarxDatatypes;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @version 1.0
 * ** Not thread-safe **
 */
public class XResult implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(XResult.class);

    private final XConnection connection;
    private final XSession session;
    private final XPacketQueue pipe;
    private XResult previous;
    private final long startNanos;
    private final long queryTimeoutNanos; // First line should come before this time.
    private final long totalTimeoutNanos; // All fetch operation should finish before this time.
    private final boolean ignoreResult;
    private boolean isFatalOnIgnorable = true; // Default fatal when error on ignorable.

    public enum RequestType {
        SQL_QUERY("sql_query"),
        SQL_UPDATE("sql_update"),
        PLAN_QUERY("plan_query"),
        PLAN_UPDATE("plan_update"),
        FETCH_TSO("fetch_tso"),
        EXPECTATIONS("expectations");

        private final String name;

        RequestType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private final RequestType requestType;
    private final String extra;

    // For log & dump analysis.
    private final String sql;
    // For cache miss retransmit.
    private XPacketBuilder retransmitPacketBuilder;

    // Data written internal.
    private final List<PolarxResultset.ColumnMetaData> metaData = new ArrayList<>(16);
    private XResultObject pipeCache = null;
    private long fetchCount = 0; // Rows fetched from pipe.
    private long tokenCount = 0;
    private long rowsAffected = 0;
    private long generatedInsertId = 0;
    private boolean haveGeneratedInsertId = false;
    private final List<PolarxNotice.Warning> warnings = new ArrayList<>();
    private int tsoErrorNo = XResultUtil.GTS_PROTOCOL_NOT_INITED;
    private long tsoValue = 0;
    private volatile long pktResponseNanos = -1;
    private long responseNanos = -1;
    private long finishNanos = -1;

    // Slow XRequest.
    private AtomicBoolean logged = new AtomicBoolean(false);

    // Read index(Next row to read).
    private int idx = 0; // 0 is first row.

    // For block mode(All data fetched in constructor).
    private List<List<ByteString>> rows;

    // For data notify.
    private volatile Runnable dataNotify = null;

    // For token debug & perf.
    private boolean allowPrefetchToken = true;
    private long tokenDoneCount = 0;
    private long activeOfferTokenCount = 0;
    private final long startMillis = System.currentTimeMillis();
    private boolean resultChunk = false;
    private boolean retrans = false;
    private final boolean goCache;

    // Feedback.
    private long examinedRowCount = -1;
    private List<String[]> chosenIndexes = null;

    public XResult(XConnection connection, XPacketQueue pipe, XResult previous,
                   long startNanos, long queryTimeoutNanos, long totalTimeoutNanos, boolean ignoreResult,
                   RequestType requestType, String sql, long tokenCount,
                   XPacketBuilder retransmitPacketBuilder, String extra)
        throws SQLException {
        this.connection = connection;
        this.session = connection.getSessionUncheck();
        this.pipe = pipe;
        this.previous = previous;
        this.startNanos = startNanos;
        this.queryTimeoutNanos = queryTimeoutNanos;
        this.totalTimeoutNanos =
            totalTimeoutNanos - startNanos > queryTimeoutNanos - startNanos ? totalTimeoutNanos : queryTimeoutNanos;
        this.ignoreResult = ignoreResult;
        this.requestType = requestType;
        this.extra = extra;
        this.sql = sql.length() > 4096 ? sql.substring(0, 4096) + "..." : sql;
        this.retransmitPacketBuilder = retransmitPacketBuilder;
        this.goCache = retransmitPacketBuilder != null;
        this.tokenCount = tokenCount;
        rows = null;
    }

    public void finishBlockMode() throws SQLException {
        // Fetch all data when not stream mode.
        try {
            final List<List<ByteString>> tmpRows = new ArrayList<>();
            XResultObject res;
            while ((res = internalFetchOneObject()) != null) {
                if (null == res.getRow()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                        "Chunk can only with stream mode.");
                }
                tmpRows.add(res.getRow());
            }
            rows = tmpRows;
        } finally {
            if (finishNanos < 0) {
                finishNanos = System.nanoTime() - startNanos;
            }
            doLog();
        }
    }

    public XConnection getConnection() {
        return connection;
    }

    public XSession getSession() {
        return session;
    }

    public void setFatalOnIgnorable(boolean fatalOnIgnorable) {
        isFatalOnIgnorable = fatalOnIgnorable;
    }

    public String getSql() {
        return sql;
    }

    public String getExtra() {
        return extra;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public ResultStatus getStatus() {
        return status;
    }

    public long getTokenCount() {
        return tokenCount;
    }

    public void recordPktResponse() {
        if (-1 == pktResponseNanos) {
            pktResponseNanos = System.nanoTime() - startNanos;
        }
    }

    public long getPktResponseNanos() {
        return pktResponseNanos;
    }

    public long getResponseNanos() {
        return responseNanos;
    }

    public long getFinishNanos() {
        return finishNanos;
    }

    public long getActiveOfferTokenCount() {
        return activeOfferTokenCount;
    }

    public long getStartMillis() {
        return startMillis;
    }

    public boolean isResultChunk() {
        return resultChunk;
    }

    public boolean isRetrans() {
        return retrans;
    }

    public boolean isGoCache() {
        return goCache;
    }

    public long getStartNanos() {
        return startNanos;
    }

    public long getQueryTimeoutNanos() {
        return queryTimeoutNanos;
    }

    public Runnable getDataNotify() {
        return dataNotify;
    }

    public void setDataNotify(Runnable dataNotify) {
        this.dataNotify = dataNotify;
    }

    public boolean isAllowPrefetchToken() {
        return allowPrefetchToken;
    }

    public void setAllowPrefetchToken(boolean allowPrefetchToken) {
        this.allowPrefetchToken = allowPrefetchToken;
    }

    public long getTokenDoneCount() {
        return tokenDoneCount;
    }

    public boolean isDataReady() throws SQLException {
        if (status != ResultStatus.XResultStart && status != ResultStatus.XResultMeta) {
            return true;
        }
        // Whether success or not, all meta data loaded.
        try {
            final XResultObject res = internalFetchOneObject(false);
            if (null == res) { // Finish.
                pipeCache = null;
                doLog();
            } else if (res.isPending()) { // Pending.
                return false;
            } else {
                pipeCache = res; // Data.
            }
        } catch (Exception e) {
            if (finishNanos < 0) {
                finishNanos = System.nanoTime() - startNanos;
            }
            doLog();
            throw e;
        }
        return true;
    }

    /**
     * Internal data.
     */

    public List<PolarxResultset.ColumnMetaData> getMetaData() throws SQLException {
        initMeta();
        return metaData;
    }

    public long getFetchCount() {
        return fetchCount;
    }

    public long getRowsAffected() {
        return rowsAffected;
    }

    public long getGeneratedInsertId() {
        return generatedInsertId;
    }

    public boolean isHaveGeneratedInsertId() {
        return haveGeneratedInsertId;
    }

    public PolarxNotice.Warning getWarning() {
        return warnings.isEmpty() ? null : warnings.get(0);
    }

    public List<PolarxNotice.Warning> getWarnings() {
        return warnings;
    }

    public int getTsoErrorNo() {
        return tsoErrorNo;
    }

    public long getTsoValue() {
        return tsoValue;
    }

    public long getExaminedRowCount() {
        return examinedRowCount;
    }

    public List<String[]> getChosenIndexes() {
        return chosenIndexes;
    }

    @Override
    public void close() {
        // Force close.
        try {
            if (!isGoodAndDone()) {
                logger.info("Query canceling: " + sql);
                try {
                    connection.cancel();
                } catch (Throwable ignore) {
                }
                // Cancel is ok because ignorable result will never return to user.
                // All non-ignorable result should be consumed.

                // A session is reusable iff this is the last request on session and is not ignorable.
                try {
                    if (connection.getLastUserRequest() == this && !ignoreResult) {
                        while (next() != null) {
                            ;
                        }
                    }
                } catch (SQLException e) {
                    if (e.getMessage().contains("Query execution was interrupted")) {
                        logger.info("Query was canceled: " + sql);
                        try {
                            final Throwable throwable = connection.getLastException();
                            if (throwable != null && throwable.getMessage().contains("Query was canceled")) {
                                // Reset the exception and let session reuse.
                                connection.setLastException(null);
                            }
                        } catch (Throwable ignore) {
                        }
                    } else {
                        logger.warn(e);
                        logger.warn("Query canceled with unexpected error. " + sql);
                    }
                }
            }
        } finally {
            doLog();
        }
    }

    void doLog() {
        final boolean prev = logged.getAndSet(true);
        if (prev) {
            return;
        }

        // Sub active count.
        if (!ignoreResult) {
            session.subActive();
        }

        if (connection == null || connection.getDataSource() == null) {
            return;
        }

        // Record respond and physical time. Caution: Change time unit to us to prevent round.
        try {
            if (responseNanos >= 0) {
                connection.getDataSource().getTotalRespondTime().getAndAdd(responseNanos / 1000L);
            } else if (finishNanos >= 0) {
                connection.getDataSource().getTotalRespondTime().getAndAdd(finishNanos / 1000L);
            }
            if (finishNanos >= 0) {
                connection.getDataSource().getTotalPhysicalTime().getAndAdd(finishNanos / 1000L);
            }
        } catch (Throwable ignore) {
        }

        if (XLog.XRequestLogger.isInfoEnabled()
            && finishNanos >= XConnectionManager.getInstance().getSlowThresholdNanos()) {
            try {
                final XSession session = connection.getSession();
                XLog.XRequestLogger.info(XLog.XRequestFormat.format(new Object[] {
                        sql, connection.getDataSource().getName().replace('#', '$'), goCache ? "cache" : "normal",
                        goCache ? (retrans ? "miss" : "hit") : "N/A", resultChunk ? "chunk" : "row",
                        Long.toString(startMillis), Long.toString(responseNanos / 1000),
                        Long.toString(finishNanos / 1000), fetchCount, tokenDoneCount, activeOfferTokenCount,
                        connection.getTraceId(), requestType.getName(),
                        null == session ? "" : Long.toString(session.getSessionId()), null == extra ? "" : extra})
                    .replace('\n', ' '));
            } catch (Throwable ignore) {
            }
        }
    }

    /**
     * For query result set packet.
     * <p>
     * Case 1(With multiple rows):
     * {
     * RESULTSET_COLUMN_META_DATA * n
     * RESULTSET_ROW * n
     * RESULTSET_FETCH_DONE
     * (NOTIFY ROWS_AFFECTED)
     * } * n
     * SQL_STMT_EXECUTE_OK
     * <p>
     * Case 2(No rows):
     * {
     * RESULTSET_COLUMN_META_DATA * n
     * RESULTSET_FETCH_DONE
     * (NOTIFY ROWS_AFFECTED)
     * } * n
     * SQL_STMT_EXECUTE_OK
     * <p>
     * ERROR may in any step.
     * <p>
     * For update result packet.
     * (NOTIFY ROWS_AFFECTED) * n
     * SQL_STMT_EXECUTE_OK
     */

    public enum ResultStatus {
        XResultStart,
        XResultMeta,
        XResultRows,
        XResultFetchDone,
        XResultAffected,
        XResultFinish,
        XResultError,
        XResultFatal
    }

    private ResultStatus status = ResultStatus.XResultStart;

    public boolean isDone() {
        return status == ResultStatus.XResultFinish ||
            status == ResultStatus.XResultError ||
            status == ResultStatus.XResultFatal;
    }

    public boolean isGoodAndDone() {
        return status == ResultStatus.XResultFinish || status == ResultStatus.XResultError;
    }

    public boolean isIgnorable() {
        return ignoreResult;
    }

    public XResult getPrevious() {
        return previous;
    }

    public boolean isGoodIgnorable() {
        return ignoreResult && status != ResultStatus.XResultFatal;
    }

    public boolean isIgnorableNotFinish() {
        return ignoreResult &&
            status != ResultStatus.XResultFatal &&
            status != ResultStatus.XResultFinish &&
            status != ResultStatus.XResultError;
    }

    public boolean waitFinish(boolean wait) throws InterruptedException, SQLException {
        if (ignoreResult) {
            // Ignore result but throw error if found.
            try {
                XResultObject resultObject;
                while ((resultObject = internalFetchOneObject(wait)) != null) {
                    if (resultObject.isPending()) {
                        return false;
                    }
                }
            } catch (Exception e) {
                if (finishNanos < 0) {
                    finishNanos = System.nanoTime() - startNanos;
                }
                doLog();
                throw e;
            }
            doLog();
            return true;
        } else {
            if (status != ResultStatus.XResultFinish && status != ResultStatus.XResultError) {
                if (ResultStatus.XResultFatal == status) {
                    throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION,
                        "XResult fatal error caused by previous fatal.");
                }
                logger.error("Previous unfinished query: " + sql);
                // Wait non-ignorable is not allowed.
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION,
                    "Fetch next with previous unfinished.");
            }
            return true;
        }
    }

    public int getQueuedDepth() {
        int depth = 0;
        XResult prob = this;
        while (prob != null) {
            if (prob.isDone()) {
                break;
            }
            ++depth;
            prob = prob.previous;
        }
        return depth;
    }

    private XResultObject internalFetchOneObject() throws SQLException {
        return internalFetchOneObject(true);
    }

    private XResultObject internalFetchOneObject(boolean wait) throws SQLException {
        if (ResultStatus.XResultError == status ||
            ResultStatus.XResultFatal == status) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION,
                "Fetch next on a XResult with error or fatal state.");
        } else if (ResultStatus.XResultFinish == status) {
            return null;
        }

        // Wait previous request before fetch packet.
        if (previous != null) {
            boolean previousFinish = false;
            try {
                previousFinish = previous.waitFinish(wait);
            } catch (Exception e) {
                final ResultStatus previousState = previous.status;
                if (ResultStatus.XResultFatal == previousState) {
                    // Cause myself fatal.
                    status = ResultStatus.XResultFatal;
                    throw GeneralUtil.nestedException(connection.setLastException(e));
                } else if (previousState != ResultStatus.XResultFinish && previousState != ResultStatus.XResultError) {
                    logger.error(
                        "Prev unfinished: " + previous.sql + " status: " + previousState.name() + " now: " + sql
                            + " status: " + status.name());
                    throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION,
                        "Fetch next with previous unfinished."); // This also happens when timeout.
                }
                // Because we add cascade error, so don't need to care about previous error.
                previousFinish = true;
            }
            if (!previousFinish) {
                if (wait) {
                    throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION,
                        "Fetch next with previous pending and block mode.");
                } else {
                    return new XResultObject(); // Pending.
                }
            }
            previous = null;
        }

        // Caution: Protect the state machine with synchronized. Because MPP may cause concurrent.
        synchronized (this) {
            if (ResultStatus.XResultError == status ||
                ResultStatus.XResultFatal == status) {
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_SESSION,
                    "Fetch next on a XResult with error or fatal state.");
            } else if (ResultStatus.XResultFinish == status) {
                return null;
            }

            loop:
            while (true) {
                final long nowNanos = System.nanoTime();
                final boolean queryStart = status == ResultStatus.XResultStart || status == ResultStatus.XResultMeta;
                final long timeoutNanos = queryStart ? queryTimeoutNanos : totalTimeoutNanos;
                final long waitNanos;
                if (nowNanos - timeoutNanos >= 0) {
                    waitNanos = 0;
                } else {
                    waitNanos = timeoutNanos - nowNanos;
                }
                final XPacket packet;
                try {
                    packet = pipe.poll(wait ? waitNanos : 0, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    throw GeneralUtil.nestedException(connection.setLastException(e));
                }
                if (null == packet) {
                    if (!wait) {
                        return new XResultObject(); // Pending.
                    }
                    throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                        "XResult stream fetch result timeout. " + (queryStart ? "Query timeout." : "Total timeout."));
                }

                final long gotPktNanos = System.nanoTime();
                switch (status) {
                case XResultStart:
                    switch (packet.getType()) {
                    case Polarx.ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE:
                        if (XConfig.GALAXY_X_PROTOCOL) {
                            metaData.add(XResultUtil.compatibleMetaConvert(
                                (PolarxResultset.ColumnMetaDataCompatible) packet.getPacket()));
                        } else {
                            metaData.add((PolarxResultset.ColumnMetaData) packet.getPacket());
                        }
                        status = ResultStatus.XResultMeta;
                        responseNanos = gotPktNanos - startNanos;
                        retransmitPacketBuilder = null; // Remove reference to retransmit packet for GC.
                        continue loop;

                    case Polarx.ServerMessages.Type.NOTICE_VALUE:
                        // ROWS_AFFECTED.
                        try {
                            final PolarxNotice.Frame frame = (PolarxNotice.Frame) packet.getPacket();
                            if (3 == frame.getType() && PolarxNotice.Frame.Scope.LOCAL == frame.getScope()) {
                                final PolarxNotice.SessionStateChanged sessionStateChanged =
                                    PolarxNotice.SessionStateChanged.parseFrom(frame.getPayload());
                                if (sessionStateChanged.getParam()
                                    == PolarxNotice.SessionStateChanged.Parameter.ROWS_AFFECTED) {
                                    if (sessionStateChanged.hasValue() &&
                                        PolarxDatatypes.Scalar.Type.V_UINT == sessionStateChanged.getValue()
                                            .getType()) {
                                        rowsAffected = sessionStateChanged.getValue().getVUnsignedInt();
                                        status = ResultStatus.XResultAffected;
                                        responseNanos = gotPktNanos - startNanos;
                                        retransmitPacketBuilder = null; // Remove reference to retransmit packet for GC.
                                        continue loop;
                                    }
                                }
                            }
                        } catch (Throwable ignore) {
                        }
                        break;

                    case Polarx.ServerMessages.Type.RESULTSET_TSO_VALUE:
                        if (requestType == RequestType.FETCH_TSO) {
                            final PolarxExecPlan.ResultTSO tso = (PolarxExecPlan.ResultTSO) packet.getPacket();
                            tsoErrorNo = tso.getErrorNo();
                            tsoValue = tso.getTs();
                            status = ResultStatus.XResultFinish;
                            responseNanos = gotPktNanos - startNanos;
                            finishNanos = gotPktNanos - startNanos;
                            return null;
                        }
                        break;

                    case Polarx.ServerMessages.Type.OK_VALUE:
                        if (requestType == RequestType.EXPECTATIONS) {
                            status = ResultStatus.XResultFinish;
                            responseNanos = gotPktNanos - startNanos;
                            finishNanos = gotPktNanos - startNanos;
                            return null;
                        }
                    }
                    break;

                case XResultMeta:
                    switch (packet.getType()) {
                    case Polarx.ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE:
                        if (XConfig.GALAXY_X_PROTOCOL) {
                            metaData.add(XResultUtil.compatibleMetaConvert(
                                (PolarxResultset.ColumnMetaDataCompatible) packet.getPacket()));
                        } else {
                            metaData.add((PolarxResultset.ColumnMetaData) packet.getPacket());
                        }
                        continue loop;

                    case Polarx.ServerMessages.Type.RESULTSET_ROW_VALUE:
                        status = ResultStatus.XResultRows;
                        ++fetchCount;
                        --tokenCount;
                        return new XResultObject(((PolarxResultset.Row) packet.getPacket()).getFieldList());

                    case Polarx.ServerMessages.Type.RESULTSET_CHUNK_VALUE: {
                        final PolarxResultset.Chunk chunk = (PolarxResultset.Chunk) packet.getPacket();
                        status = ResultStatus.XResultRows;
                        fetchCount += chunk.getRowCount();
                        tokenCount -= chunk.getRowCount();
                        resultChunk = true;
                        return new XResultObject((PolarxResultset.Chunk) packet.getPacket());
                    }

                    case Polarx.ServerMessages.Type.RESULTSET_FETCH_DONE_VALUE: {
                        final PolarxResultset.FetchDone fetchDone = (PolarxResultset.FetchDone) packet.getPacket();
                        if (fetchDone.hasExaminedRowCount()) {
                            examinedRowCount = fetchDone.getExaminedRowCount();
                        }
                        if (fetchDone.hasChosenIndex()) {
                            final String indexes = fetchDone.getChosenIndex().toStringUtf8();
                            final String[] indexArray = indexes.split(";");
                            if (indexArray.length > 0) {
                                if (null == chosenIndexes) {
                                    chosenIndexes = new ArrayList<>(indexArray.length);
                                }
                                for (String index : indexArray) {
                                    chosenIndexes.add(index.split("\\."));
                                }
                            }
                        }
                        status = ResultStatus.XResultFetchDone;
                        continue loop;
                    }

                    default:
                        break;
                    }
                    break;

                case XResultRows:
                    switch (packet.getType()) {
                    case Polarx.ServerMessages.Type.RESULTSET_ROW_VALUE: {
                        ++fetchCount;
                        --tokenCount;
                        final long tokenLow = XConnectionManager.getInstance().getDefaultQueryToken() / 5;
                        if (!XConfig.GALAXY_X_PROTOCOL &&
                            allowPrefetchToken && tokenCount < tokenLow && pipe.count() < tokenLow) {
                            connection.tokenOffer();
                            tokenCount = connection.getDefaultTokenCount();
                        }
                        return new XResultObject(((PolarxResultset.Row) packet.getPacket()).getFieldList());
                    }

                    case Polarx.ServerMessages.Type.RESULTSET_CHUNK_VALUE: {
                        final PolarxResultset.Chunk chunk = (PolarxResultset.Chunk) packet.getPacket();
                        fetchCount += chunk.getRowCount();
                        tokenCount -= chunk.getRowCount();
                        // May multi chunk for columns more than 10, but we don't care. Because we also count the pipe depth.
                        final long tokenLow = XConnectionManager.getInstance().getDefaultQueryToken() / 5;
                        if (!XConfig.GALAXY_X_PROTOCOL &&
                            allowPrefetchToken && tokenCount < tokenLow && pipe.count() < tokenLow) {
                            connection.tokenOffer();
                            tokenCount = connection.getDefaultTokenCount();
                            ++activeOfferTokenCount;
                        }
                        resultChunk = true;
                        return new XResultObject((PolarxResultset.Chunk) packet.getPacket());
                    }

                    case Polarx.ServerMessages.Type.RESULTSET_FETCH_DONE_VALUE: {
                        final PolarxResultset.FetchDone fetchDone = (PolarxResultset.FetchDone) packet.getPacket();
                        if (fetchDone.hasExaminedRowCount()) {
                            examinedRowCount = fetchDone.getExaminedRowCount();
                        }
                        if (fetchDone.hasChosenIndex()) {
                            final String indexes = fetchDone.getChosenIndex().toStringUtf8();
                            final String[] indexArray = indexes.split(";");
                            if (indexArray.length > 0) {
                                if (null == chosenIndexes) {
                                    chosenIndexes = new ArrayList<>(indexArray.length);
                                }
                                for (String index : indexArray) {
                                    chosenIndexes.add(index.split("\\."));
                                }
                            }
                        }
                        status = ResultStatus.XResultFetchDone;
                        continue loop;
                    }

                    case Polarx.ServerMessages.Type.RESULTSET_TOKEN_DONE_VALUE:
                        connection.tokenOffer();
                        ++tokenDoneCount;
                        continue loop;

                    case Polarx.ServerMessages.Type.RESULTSET_FETCH_DONE_MORE_RESULTSETS_VALUE:
                        status = ResultStatus.XResultFatal;
                        finishNanos = gotPktNanos - startNanos;
                        throw GeneralUtil.nestedException(
                            connection.setLastException(new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                                "Fatal error more result set not supported.")));

                    default:
                        break;
                    }
                    break;

                case XResultFetchDone:
                    switch (packet.getType()) {
                    case Polarx.ServerMessages.Type.SQL_STMT_EXECUTE_OK_VALUE:
                        status = ResultStatus.XResultFinish;
                        finishNanos = gotPktNanos - startNanos;
                        return null;

                    case Polarx.ServerMessages.Type.NOTICE_VALUE:
                        // ROWS_AFFECTED.
                        try {
                            final PolarxNotice.Frame frame = (PolarxNotice.Frame) packet.getPacket();
                            if (3 == frame.getType()) {
                                final PolarxNotice.SessionStateChanged sessionStateChanged =
                                    PolarxNotice.SessionStateChanged.parseFrom(frame.getPayload());
                                if (sessionStateChanged.getParam()
                                    == PolarxNotice.SessionStateChanged.Parameter.ROWS_AFFECTED) {
                                    if (sessionStateChanged.hasValue() &&
                                        PolarxDatatypes.Scalar.Type.V_UINT == sessionStateChanged.getValue()
                                            .getType()) {
                                        rowsAffected = sessionStateChanged.getValue().getVUnsignedInt();
                                        status = ResultStatus.XResultAffected;
                                        continue loop;
                                    }
                                }
                            }
                        } catch (Throwable ignore) {
                        }
                        break;

                    default:
                        break;
                    }
                    break;

                case XResultAffected:
                    if (Polarx.ServerMessages.Type.SQL_STMT_EXECUTE_OK_VALUE == packet.getType()) {
                        status = ResultStatus.XResultFinish;
                        finishNanos = gotPktNanos - startNanos;
                        return null;
                    }
                    break;

                default:
                    break;
                }

                // General error.
                if (Polarx.ServerMessages.Type.ERROR_VALUE == packet.getType()) {
                    final Polarx.Error error = (Polarx.Error) packet.getPacket();
                    // Ignorable request must success.
                    if (error.getSeverity() == Polarx.Error.Severity.ERROR && (!ignoreResult || !isFatalOnIgnorable)) {
                        // Cache miss?
                        if (ResultStatus.XResultStart == status && 6000 == error.getCode()
                            && retransmitPacketBuilder != null && !ignoreResult) {
                            // Try retransmit.
                            try {
                                boolean badUsage = false;
                                XResult last = connection.getSession().getLastRequest();
                                if (last != this) {
                                    while (last != null) {
                                        if (last == this) {
                                            badUsage = true;
                                            break;
                                        }
                                        last = last.previous;
                                    }
                                }
                                if (!badUsage) {
                                    logger.info("Sql/plan cache miss.");
                                    if (connection.getDataSource() != null) {
                                        if (retransmitPacketBuilder.isPlan()) {
                                            connection.getDataSource().getCachePlanMiss().getAndIncrement();
                                        } else {
                                            connection.getDataSource().getCacheSqlMiss().getAndIncrement();
                                        }
                                    }
                                    connection.getSession().getClient().send(retransmitPacketBuilder.build(), true);
                                    retransmitPacketBuilder = null;
                                    retrans = true;
                                    continue loop;
                                }
                            } catch (Throwable t) {
                                throw GeneralUtil.nestedException(connection.setLastException(t));
                            }
                            // Only bad usage get here.
                            status = ResultStatus.XResultError;
                            finishNanos = gotPktNanos - startNanos;
                            throw new SQLException("Sql cache mismatch while pipeline is not empty.",
                                error.getSqlState(), error.getCode());
                        }
                        // Normal error caused by this SQL.
                        // Switch to finish.
                        status = ResultStatus.XResultError;
                        finishNanos = gotPktNanos - startNanos;
                        // Throw this error like driver.
                        if (error.getCode() == 1062) {
                            throw new MySQLIntegrityConstraintViolationException(
                                error.getMsg(), error.getSqlState(), error.getCode());
                        } else {
                            throw new SQLException(error.getMsg(), error.getSqlState(), error.getCode());
                        }
                    } else {
                        // Fatal error.
                        status = ResultStatus.XResultFatal;
                        finishNanos = gotPktNanos - startNanos;
                        final String fatalMessage = ((Polarx.Error) packet.getPacket()).hasMsg() ?
                            ((Polarx.Error) packet.getPacket()).getMsg() : "Unknown error.";
                        final String fatalState = ((Polarx.Error) packet.getPacket()).hasSqlState() ?
                            ((Polarx.Error) packet.getPacket()).getSqlState() : "Unknown state.";
                        throw GeneralUtil.nestedException(
                            connection.setLastException(new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                                "Fatal error when fetch data: " + fatalMessage + " " + fatalState)));
                    }
                } else if (Polarx.ServerMessages.Type.NOTICE_VALUE == packet.getType()) {
                    // TODO: Other notice.
                    try {
                        final PolarxNotice.Frame frame = (PolarxNotice.Frame) packet.getPacket();
                        switch (frame.getType()) {
                        case PolarxNotice.Frame.Type.WARNING_VALUE:
                            if (PolarxNotice.Frame.Scope.LOCAL == frame.getScope()) {
                                warnings.add(PolarxNotice.Warning.parseFrom(frame.getPayload()));
                            }
                            break;

                        case PolarxNotice.Frame.Type.SESSION_STATE_CHANGED_VALUE:
                            if (PolarxNotice.Frame.Scope.LOCAL == frame.getScope()) {
                                final PolarxNotice.SessionStateChanged sessionStateChanged =
                                    PolarxNotice.SessionStateChanged.parseFrom(frame.getPayload());
                                if (sessionStateChanged.getParam()
                                    == PolarxNotice.SessionStateChanged.Parameter.GENERATED_INSERT_ID) {
                                    if (sessionStateChanged.hasValue()) {
                                        switch (sessionStateChanged.getValue().getType()) {
                                        case V_SINT:
                                            generatedInsertId = sessionStateChanged.getValue().getVSignedInt();
                                            haveGeneratedInsertId = true;
                                            break;

                                        case V_UINT:
                                            generatedInsertId = sessionStateChanged.getValue().getVUnsignedInt();
                                            haveGeneratedInsertId = true;
                                            break;

                                        default:
                                            break;
                                        }
                                    }
                                }
                            }
                            break;

                        default:
                            break;
                        }
                    } catch (Throwable ignore) {
                    }
                    continue;
                }

                finishNanos = gotPktNanos - startNanos;
                throw GeneralUtil.nestedException(connection.setLastException(
                    new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                        "XResult unexpected packet type " + packet.getType() + " at status " + status.name() + ".")));
            }
        }
    }

    private void initMeta() throws SQLException {
        if (status != ResultStatus.XResultStart && status != ResultStatus.XResultMeta) {
            return;
        }
        // Whether success or not, all meta data loaded.
        try {
            pipeCache = internalFetchOneObject();
            if (null == pipeCache) {
                doLog();
            }
        } catch (Exception e) {
            if (finishNanos < 0) {
                finishNanos = System.nanoTime() - startNanos;
            }
            doLog();
            throw e;
        }
    }

    public XResultObject current() {
        return 0 == idx ? null : pipeCache;
    }

    public XResultObject next() throws SQLException {
        if (rows != null) {
            // Block mode.
            if (idx < rows.size()) {
                pipeCache = new XResultObject(rows.get(idx));
                ++idx;
            } else {
                pipeCache = null;
            }
            return current();
        }

        // Stream mode.
        if (ResultStatus.XResultFinish == status) {
            return null;
        }

        // Just return if fetch meta before.
        if (pipeCache != null && 0 == idx) {
            ++idx;
            return current();
        }

        try {
            pipeCache = internalFetchOneObject();
            if (null == pipeCache) {
                doLog();
            }
        } catch (Exception e) {
            if (finishNanos < 0) {
                finishNanos = System.nanoTime() - startNanos;
            }
            doLog();
            throw e;
        }
        ++idx;
        return current();
    }

    public XResultObject mergeNext(XResultObject current) throws SQLException {
        assert current != null;
        final XResultObject next = next();
        if (null == next.getChunk()) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT, "Error merge on non-chunk result.");
        }
        current.addSecondChunk(next.getChunk());
        pipeCache = current;
        return current();
    }

    public long currentPipeSize() {
        if (pipeCache != null) {
            if (pipeCache.getRow() != null) {
                return pipeCache.getRow().stream().mapToLong(ByteString::size).sum();
            } else {
                return pipeCache.getChunk().getColumnsList().stream().mapToLong(
                    c -> (c.hasNullBitmap() ? c.getNullBitmap().size() : 0) + (c.hasFixedSizeColumn() ?
                        c.getFixedSizeColumn().getValue().size() : 0) + (c.hasVariableSizeColumn() ?
                        c.getVariableSizeColumn().getValue().size() : 0)).sum();
            }
        } else {
            return 0;
        }
    }
}
