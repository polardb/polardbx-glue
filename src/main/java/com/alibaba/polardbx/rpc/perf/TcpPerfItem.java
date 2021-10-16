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

import com.alibaba.polardbx.rpc.client.XClient;

/**
 * @version 1.0
 */
public class TcpPerfItem {

    // TCP level data.
    private String tcpTag; // Name.
    private String dnTag;

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

    private long sessionCount = 0;

    // Other info.
    private XClient.ClientState clientState = XClient.ClientState.Unknown;
    private long idleNanosSinceLastPacket = 0;
    private long liveNanos = 0;
    private String fatalError = null;
    private long authId = 0;
    private long nanosSinceLastVariablesFlush = 0;

    public enum TcpState {
        Working,
        Aging
    }

    private TcpState tcpState;

    // TCP physical info.
    private long socketSendBufferSize;
    private long socketRecvBufferSize;
    private long readDirectBuffers;
    private long readHeapBuffers;
    private long writeDirectBuffers;
    private long writeHeapBuffers;
    private boolean reactorRegistered;
    private boolean socketClosed;

    public String getTcpTag() {
        return tcpTag;
    }

    public String getDnTag() {
        return dnTag;
    }

    public void setDnTag(String dnTag) {
        this.dnTag = dnTag;
    }

    public void setTcpTag(String tcpTag) {
        this.tcpTag = tcpTag;
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

    public long getSessionCount() {
        return sessionCount;
    }

    public void setSessionCount(long sessionCount) {
        this.sessionCount = sessionCount;
    }

    public XClient.ClientState getClientState() {
        return clientState;
    }

    public void setClientState(XClient.ClientState clientState) {
        this.clientState = clientState;
    }

    public long getIdleNanosSinceLastPacket() {
        return idleNanosSinceLastPacket;
    }

    public void setIdleNanosSinceLastPacket(long idleNanosSinceLastPacket) {
        this.idleNanosSinceLastPacket = idleNanosSinceLastPacket;
    }

    public long getLiveNanos() {
        return liveNanos;
    }

    public void setLiveNanos(long liveNanos) {
        this.liveNanos = liveNanos;
    }

    public String getFatalError() {
        return fatalError;
    }

    public void setFatalError(String fatalError) {
        this.fatalError = fatalError;
    }

    public long getAuthId() {
        return authId;
    }

    public void setAuthId(long authId) {
        this.authId = authId;
    }

    public long getNanosSinceLastVariablesFlush() {
        return nanosSinceLastVariablesFlush;
    }

    public void setNanosSinceLastVariablesFlush(long nanosSinceLastVariablesFlush) {
        this.nanosSinceLastVariablesFlush = nanosSinceLastVariablesFlush;
    }

    public TcpState getTcpState() {
        return tcpState;
    }

    public void setTcpState(TcpState tcpState) {
        this.tcpState = tcpState;
    }

    public long getSocketSendBufferSize() {
        return socketSendBufferSize;
    }

    public void setSocketSendBufferSize(long socketSendBufferSize) {
        this.socketSendBufferSize = socketSendBufferSize;
    }

    public long getSocketRecvBufferSize() {
        return socketRecvBufferSize;
    }

    public void setSocketRecvBufferSize(long socketRecvBufferSize) {
        this.socketRecvBufferSize = socketRecvBufferSize;
    }

    public long getReadDirectBuffers() {
        return readDirectBuffers;
    }

    public void setReadDirectBuffers(long readDirectBuffers) {
        this.readDirectBuffers = readDirectBuffers;
    }

    public long getReadHeapBuffers() {
        return readHeapBuffers;
    }

    public void setReadHeapBuffers(long readHeapBuffers) {
        this.readHeapBuffers = readHeapBuffers;
    }

    public long getWriteDirectBuffers() {
        return writeDirectBuffers;
    }

    public void setWriteDirectBuffers(long writeDirectBuffers) {
        this.writeDirectBuffers = writeDirectBuffers;
    }

    public long getWriteHeapBuffers() {
        return writeHeapBuffers;
    }

    public void setWriteHeapBuffers(long writeHeapBuffers) {
        this.writeHeapBuffers = writeHeapBuffers;
    }

    public boolean isReactorRegistered() {
        return reactorRegistered;
    }

    public void setReactorRegistered(boolean reactorRegistered) {
        this.reactorRegistered = reactorRegistered;
    }

    public boolean isSocketClosed() {
        return socketClosed;
    }

    public void setSocketClosed(boolean socketClosed) {
        this.socketClosed = socketClosed;
    }
}
