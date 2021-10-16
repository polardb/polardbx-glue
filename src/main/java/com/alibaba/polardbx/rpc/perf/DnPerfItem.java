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

/**
 * @version 1.0
 */
public class DnPerfItem {

    // DN level data.
    private String dnTag; // Name.

    // Network perf.
    private long sendMsgCount = 0;
    private long sendFlushCount = 0;
    private long sendSize = 0;
    private long recvMsgCount = 0;
    private long recvNetCount = 0;
    private long recvSize = 0;

    // Session perf.
    private long sessionCreateCount = 0;
    private long sessionDropCount = 0;
    private long sessionCreateSuccessCount = 0;
    private long sessionCreateFailCount = 0;
    private long sessionActiveCount = 0;
    private long getConnectionCount = 0;

    // Delta.
    private double sendMsgRate = 0.;
    private double sendFlushRate = 0.;
    private double sendRate = 0.;
    private double recvMsgRate = 0.;
    private double recvNetRate = 0.;
    private double recvRate = 0.;
    private double sessionCreateRate = 0.;
    private double sessionDropRate = 0.;
    private double sessionCreateSuccessRate = 0.;
    private double sessionCreateFailRate = 0.;
    private double gatherTimeDelta = 0.;

    // DN working statue.
    private long tcpTotalCount = 0;
    private long tcpAgingCount = 0;
    private long sessionCount = 0;
    private long sessionIdleCount = 0;

    private long refCount = 0;

    public String getDnTag() {
        return dnTag;
    }

    public void setDnTag(String dnTag) {
        this.dnTag = dnTag;
    }

    public long getSendMsgCount() {
        return sendMsgCount;
    }

    public void setSendMsgCount(long sendMsgCount) {
        this.sendMsgCount = sendMsgCount;
    }

    public long getSendFlushCount() {
        return sendFlushCount;
    }

    public void setSendFlushCount(long sendFlushCount) {
        this.sendFlushCount = sendFlushCount;
    }

    public long getSendSize() {
        return sendSize;
    }

    public void setSendSize(long sendSize) {
        this.sendSize = sendSize;
    }

    public long getRecvMsgCount() {
        return recvMsgCount;
    }

    public void setRecvMsgCount(long recvMsgCount) {
        this.recvMsgCount = recvMsgCount;
    }

    public long getRecvNetCount() {
        return recvNetCount;
    }

    public void setRecvNetCount(long recvNetCount) {
        this.recvNetCount = recvNetCount;
    }

    public long getRecvSize() {
        return recvSize;
    }

    public void setRecvSize(long recvSize) {
        this.recvSize = recvSize;
    }

    public long getSessionCreateCount() {
        return sessionCreateCount;
    }

    public void setSessionCreateCount(long sessionCreateCount) {
        this.sessionCreateCount = sessionCreateCount;
    }

    public long getSessionDropCount() {
        return sessionDropCount;
    }

    public void setSessionDropCount(long sessionDropCount) {
        this.sessionDropCount = sessionDropCount;
    }

    public long getSessionCreateSuccessCount() {
        return sessionCreateSuccessCount;
    }

    public void setSessionCreateSuccessCount(long sessionCreateSuccessCount) {
        this.sessionCreateSuccessCount = sessionCreateSuccessCount;
    }

    public long getSessionCreateFailCount() {
        return sessionCreateFailCount;
    }

    public void setSessionCreateFailCount(long sessionCreateFailCount) {
        this.sessionCreateFailCount = sessionCreateFailCount;
    }

    public long getSessionActiveCount() {
        return sessionActiveCount;
    }

    public void setSessionActiveCount(long sessionActiveCount) {
        this.sessionActiveCount = sessionActiveCount;
    }

    public long getGetConnectionCount() {
        return getConnectionCount;
    }

    public void setGetConnectionCount(long getConnectionCount) {
        this.getConnectionCount = getConnectionCount;
    }

    public double getSendMsgRate() {
        return sendMsgRate;
    }

    public void setSendMsgRate(double sendMsgRate) {
        this.sendMsgRate = sendMsgRate;
    }

    public double getSendFlushRate() {
        return sendFlushRate;
    }

    public void setSendFlushRate(double sendFlushRate) {
        this.sendFlushRate = sendFlushRate;
    }

    public double getSendRate() {
        return sendRate;
    }

    public void setSendRate(double sendRate) {
        this.sendRate = sendRate;
    }

    public double getRecvMsgRate() {
        return recvMsgRate;
    }

    public void setRecvMsgRate(double recvMsgRate) {
        this.recvMsgRate = recvMsgRate;
    }

    public double getRecvNetRate() {
        return recvNetRate;
    }

    public void setRecvNetRate(double recvNetRate) {
        this.recvNetRate = recvNetRate;
    }

    public double getRecvRate() {
        return recvRate;
    }

    public void setRecvRate(double recvRate) {
        this.recvRate = recvRate;
    }

    public double getSessionCreateRate() {
        return sessionCreateRate;
    }

    public void setSessionCreateRate(double sessionCreateRate) {
        this.sessionCreateRate = sessionCreateRate;
    }

    public double getSessionDropRate() {
        return sessionDropRate;
    }

    public void setSessionDropRate(double sessionDropRate) {
        this.sessionDropRate = sessionDropRate;
    }

    public double getSessionCreateSuccessRate() {
        return sessionCreateSuccessRate;
    }

    public void setSessionCreateSuccessRate(double sessionCreateSuccessRate) {
        this.sessionCreateSuccessRate = sessionCreateSuccessRate;
    }

    public double getSessionCreateFailRate() {
        return sessionCreateFailRate;
    }

    public void setSessionCreateFailRate(double sessionCreateFailRate) {
        this.sessionCreateFailRate = sessionCreateFailRate;
    }

    public double getGatherTimeDelta() {
        return gatherTimeDelta;
    }

    public void setGatherTimeDelta(double gatherTimeDelta) {
        this.gatherTimeDelta = gatherTimeDelta;
    }

    public long getTcpTotalCount() {
        return tcpTotalCount;
    }

    public void setTcpTotalCount(long tcpTotalCount) {
        this.tcpTotalCount = tcpTotalCount;
    }

    public long getTcpAgingCount() {
        return tcpAgingCount;
    }

    public void setTcpAgingCount(long tcpAgingCount) {
        this.tcpAgingCount = tcpAgingCount;
    }

    public long getSessionCount() {
        return sessionCount;
    }

    public void setSessionCount(long sessionCount) {
        this.sessionCount = sessionCount;
    }

    public long getSessionIdleCount() {
        return sessionIdleCount;
    }

    public void setSessionIdleCount(long sessionIdleCount) {
        this.sessionIdleCount = sessionIdleCount;
    }

    public long getRefCount() {
        return refCount;
    }

    public void setRefCount(long refCount) {
        this.refCount = refCount;
    }
}
