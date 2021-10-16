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

package com.alibaba.polardbx.rpc.perf;

import com.alibaba.polardbx.rpc.client.XSession;

/**
 * @version 1.0
 */
public class SessionPerfItem {

    // Session level data.

    private String dnTag;
    private String tcpTag;
    private long sessionId = 0;
    private XSession.Status status = XSession.Status.Init;

    private long planCount = 0;
    private long queryCount = 0;
    private long updateCount = 0;
    private long tsoCount = 0;

    private long liveNanos = 0;
    private long idleNanosSinceLastPacket = 0;

    private String charsetClient;
    private String charsetResult;
    private String timezone;
    private String isolation;
    private boolean autoCommit;
    private String variablesChanged;
    private String lastRequestDB;

    private long queuedRequestDepth = 0;

    private String lastRequestTraceId;
    private String lastRequestSql;
    private String lastRequestExtra;
    private String lastRequestType;
    private String lastRequestStatus;
    private long lastRequestFetchCount = 0;
    private long lastRequestTokenCount = 0;
    private long lastRequestWorkingNanos = 0;
    private long lastRequestDataPktResponseNanos = 0;
    private long lastRequestResponseNanos = 0;
    private long lastRequestFinishNanos = 0;
    private long lastRequestTokenDoneCount = 0;
    private long lastRequestActiveOfferTokenCount = 0;
    private long lastRequestStartMillis = 0;
    private boolean lastRequestResultChunk = false;
    private boolean lastRequestRetrans = false;
    private boolean lastRequestGoCache = false;

    public String getDnTag() {
        return dnTag;
    }

    public void setDnTag(String dnTag) {
        this.dnTag = dnTag;
    }

    public String getTcpTag() {
        return tcpTag;
    }

    public void setTcpTag(String tcpTag) {
        this.tcpTag = tcpTag;
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public XSession.Status getStatus() {
        return status;
    }

    public void setStatus(XSession.Status status) {
        this.status = status;
    }

    public long getPlanCount() {
        return planCount;
    }

    public void setPlanCount(long planCount) {
        this.planCount = planCount;
    }

    public long getQueryCount() {
        return queryCount;
    }

    public void setQueryCount(long queryCount) {
        this.queryCount = queryCount;
    }

    public long getUpdateCount() {
        return updateCount;
    }

    public void setUpdateCount(long updateCount) {
        this.updateCount = updateCount;
    }

    public long getTsoCount() {
        return tsoCount;
    }

    public void setTsoCount(long tsoCount) {
        this.tsoCount = tsoCount;
    }

    public long getLiveNanos() {
        return liveNanos;
    }

    public void setLiveNanos(long liveNanos) {
        this.liveNanos = liveNanos;
    }

    public long getIdleNanosSinceLastPacket() {
        return idleNanosSinceLastPacket;
    }

    public void setIdleNanosSinceLastPacket(long idleNanosSinceLastPacket) {
        this.idleNanosSinceLastPacket = idleNanosSinceLastPacket;
    }

    public String getCharsetClient() {
        return charsetClient;
    }

    public void setCharsetClient(String charsetClient) {
        this.charsetClient = charsetClient;
    }

    public String getCharsetResult() {
        return charsetResult;
    }

    public void setCharsetResult(String charsetResult) {
        this.charsetResult = charsetResult;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getIsolation() {
        return isolation;
    }

    public void setIsolation(String isolation) {
        this.isolation = isolation;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public String getVariablesChanged() {
        return variablesChanged;
    }

    public void setVariablesChanged(String variablesChanged) {
        this.variablesChanged = variablesChanged;
    }

    public String getLastRequestDB() {
        return lastRequestDB;
    }

    public void setLastRequestDB(String lastRequestDB) {
        this.lastRequestDB = lastRequestDB;
    }

    public long getQueuedRequestDepth() {
        return queuedRequestDepth;
    }

    public void setQueuedRequestDepth(long queuedRequestDepth) {
        this.queuedRequestDepth = queuedRequestDepth;
    }

    public String getLastRequestTraceId() {
        return lastRequestTraceId;
    }

    public void setLastRequestTraceId(String lastRequestTraceId) {
        this.lastRequestTraceId = lastRequestTraceId;
    }

    public String getLastRequestSql() {
        return lastRequestSql;
    }

    public void setLastRequestSql(String lastRequestSql) {
        this.lastRequestSql = lastRequestSql;
    }

    public String getLastRequestExtra() {
        return lastRequestExtra;
    }

    public void setLastRequestExtra(String lastRequestExtra) {
        this.lastRequestExtra = lastRequestExtra;
    }

    public String getLastRequestType() {
        return lastRequestType;
    }

    public void setLastRequestType(String lastRequestType) {
        this.lastRequestType = lastRequestType;
    }

    public String getLastRequestStatus() {
        return lastRequestStatus;
    }

    public void setLastRequestStatus(String lastRequestStatus) {
        this.lastRequestStatus = lastRequestStatus;
    }

    public long getLastRequestFetchCount() {
        return lastRequestFetchCount;
    }

    public void setLastRequestFetchCount(long lastRequestFetchCount) {
        this.lastRequestFetchCount = lastRequestFetchCount;
    }

    public long getLastRequestTokenCount() {
        return lastRequestTokenCount;
    }

    public void setLastRequestTokenCount(long lastRequestTokenCount) {
        this.lastRequestTokenCount = lastRequestTokenCount;
    }

    public long getLastRequestWorkingNanos() {
        return lastRequestWorkingNanos;
    }

    public void setLastRequestWorkingNanos(long lastRequestWorkingNanos) {
        this.lastRequestWorkingNanos = lastRequestWorkingNanos;
    }

    public long getLastRequestDataPktResponseNanos() {
        return lastRequestDataPktResponseNanos;
    }

    public void setLastRequestDataPktResponseNanos(long lastRequestDataPktResponseNanos) {
        this.lastRequestDataPktResponseNanos = lastRequestDataPktResponseNanos;
    }

    public long getLastRequestResponseNanos() {
        return lastRequestResponseNanos;
    }

    public void setLastRequestResponseNanos(long lastRequestResponseNanos) {
        this.lastRequestResponseNanos = lastRequestResponseNanos;
    }

    public long getLastRequestFinishNanos() {
        return lastRequestFinishNanos;
    }

    public void setLastRequestFinishNanos(long lastRequestFinishNanos) {
        this.lastRequestFinishNanos = lastRequestFinishNanos;
    }

    public long getLastRequestTokenDoneCount() {
        return lastRequestTokenDoneCount;
    }

    public void setLastRequestTokenDoneCount(long lastRequestTokenDoneCount) {
        this.lastRequestTokenDoneCount = lastRequestTokenDoneCount;
    }

    public long getLastRequestActiveOfferTokenCount() {
        return lastRequestActiveOfferTokenCount;
    }

    public void setLastRequestActiveOfferTokenCount(long lastRequestActiveOfferTokenCount) {
        this.lastRequestActiveOfferTokenCount = lastRequestActiveOfferTokenCount;
    }

    public long getLastRequestStartMillis() {
        return lastRequestStartMillis;
    }

    public void setLastRequestStartMillis(long lastRequestStartMillis) {
        this.lastRequestStartMillis = lastRequestStartMillis;
    }

    public boolean isLastRequestResultChunk() {
        return lastRequestResultChunk;
    }

    public void setLastRequestResultChunk(boolean lastRequestResultChunk) {
        this.lastRequestResultChunk = lastRequestResultChunk;
    }

    public boolean isLastRequestRetrans() {
        return lastRequestRetrans;
    }

    public void setLastRequestRetrans(boolean lastRequestRetrans) {
        this.lastRequestRetrans = lastRequestRetrans;
    }

    public boolean isLastRequestGoCache() {
        return lastRequestGoCache;
    }

    public void setLastRequestGoCache(boolean lastRequestGoCache) {
        this.lastRequestGoCache = lastRequestGoCache;
    }
}
