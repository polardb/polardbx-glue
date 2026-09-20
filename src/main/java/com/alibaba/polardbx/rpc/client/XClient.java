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

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.encrypt.SecurityUtil;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.XLog;
import com.alibaba.polardbx.rpc.net.NIOClient;
import com.alibaba.polardbx.rpc.net.NIOProcessor;
import com.alibaba.polardbx.rpc.net.NIOWorker;
import com.alibaba.polardbx.rpc.packet.XPacket;
import com.alibaba.polardbx.rpc.perf.SessionPerfItem;
import com.alibaba.polardbx.rpc.perf.TcpPerfCollection;
import com.alibaba.polardbx.rpc.perf.TcpPerfItem;
import com.alibaba.polardbx.rpc.pool.XClientPool;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxConnection;
import com.mysql.cj.polarx.protobuf.PolarxNotice;
import com.mysql.cj.polarx.protobuf.PolarxSession;
import com.mysql.cj.x.protobuf.Polarx;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * @version 1.0
 */
public class XClient implements AutoCloseable {

    private final static String AUTH_OK_MARK = "Auth ok.";

    // Server info.
    private final XClientPool pool;

    // For TCP connection manager.
    private final NIOWorker nioWorker;
    private final NIOClient nioClient;
    private final long connectTimeoutNanos;

    // Working session on this connection.
    private final Map<Long, XSession> workingSessionMap = new ConcurrentHashMap<>();

    // Connection data.
    private final AtomicLong lastPacketNanos = new AtomicLong(0);
    private final AtomicBoolean connected = new AtomicBoolean(false);

    // Creation time for aging.
    private final long createNanos = System.nanoTime();
    // Random delay in 10min.
    private final long randomDelay = ThreadLocalRandom.current().nextLong(600_000_000_000L);
    // Reusable mark.
    private final AtomicBoolean reusable = new AtomicBoolean(true);

    // Fatal error record.
    private final AtomicReference<Throwable> fatalError = new AtomicReference<>(null);

    // For auth wait and message.
    private final AtomicReference<String> authMessage = new AtomicReference<>(null);
    private long authClientId = -1; // Return by auth.

    // Perf data.
    private final TcpPerfCollection perfCollection = new TcpPerfCollection();

    public enum ClientState {
        Unknown,
        Initializing,
        Connecting,
        Registering,
        Connected,
        Authing,
        Authed,
        LoadVariables,
        Ready
    }

    private volatile ClientState state = ClientState.Initializing;

    private final AtomicInteger probeFailTimes = new AtomicInteger(0);
    private final AtomicBoolean probeEvent = new AtomicBoolean(false);

    public XClient(NIOWorker nioWorker, XClientPool pool, BiFunction<XClient, XPacket, Boolean> filter,
                   long connectTimeoutNanos) {
        this.pool = pool;

        this.nioWorker = nioWorker;
        this.nioClient = new NIOClient(msgs -> {
            final boolean logEnabled = XLog.XProtocolLogger.isInfoEnabled();
            final Map savedMdcContext;

            if (logEnabled) {
                // Only used in log packet.
                savedMdcContext = MDC.getCopyOfContextMap();
            } else {
                savedMdcContext = null;
            }

            try {
                final long nowNanos = System.nanoTime();

                // Record last packet nanos.
                lastPacketNanos.set(nowNanos);

                // Last info for collection.
                XSession lastSession = null;
                ArrayList<XPacket> packets = null;

                for (XPacket msg : msgs) {
                    if (filter != null) {
                        if (!filter.apply(XClient.this, msg)) {
                            return; // Reject.
                        }
                    }

                    final long sessionId = msg.getSid();

                    if (0 == sessionId ||
                        (XConfig.GALAXY_X_PROTOCOL && ClientState.Authing == state && 1 == sessionId)) {
                        if (logEnabled) {
                            logPacket(msg, false, false);
                        }

                        // Dealing with auth.
                        switch (msg.getType()) {
                        case Polarx.ServerMessages.Type.CONN_CAPABILITIES_VALUE:
                            synchronized (probeEvent) {
                                probeEvent.set(true);
                                probeEvent.notify();
                            }
                            break;

                        case Polarx.ServerMessages.Type.SESS_AUTHENTICATE_CONTINUE_VALUE:
                            scramble(sessionId, ((PolarxSession.AuthenticateContinue) msg.getPacket()).getAuthData());
                            break;

                        case Polarx.ServerMessages.Type.SESS_AUTHENTICATE_OK_VALUE:
                            final PolarxSession.AuthenticateOk authenticateOk =
                                ((PolarxSession.AuthenticateOk) msg.getPacket());
                            final String authData =
                                authenticateOk.hasAuthData() ? authenticateOk.getAuthData().toStringUtf8() : "";
                            XLog.XLogLogger.info(XClient.this + " auth ok." +
                                (authData.isEmpty() ? "" : " msg: " + authData));
                            synchronized (authMessage) {
                                authMessage.set(AUTH_OK_MARK);
                                authMessage.notify();
                            }
                            break;

                        case Polarx.ServerMessages.Type.NOTICE_VALUE:
                            final PolarxNotice.Frame frame = (PolarxNotice.Frame) msg.getPacket();
                            if (3 == frame.getType() && frame.hasPayload() && frame.getPayload().size() > 0) {
                                try {
                                    final PolarxNotice.SessionStateChanged sessionStateChanged =
                                        PolarxNotice.SessionStateChanged.parseFrom(frame.getPayload());
                                    if (sessionStateChanged.getParam()
                                        .equals(PolarxNotice.SessionStateChanged.Parameter.CLIENT_ID_ASSIGNED)) {
                                        if (sessionStateChanged.hasValue()) {
                                            switch (sessionStateChanged.getValue().getType()) {
                                            case V_SINT:
                                                authClientId = sessionStateChanged.getValue().getVSignedInt();
                                                break;

                                            case V_UINT:
                                                authClientId = sessionStateChanged.getValue().getVUnsignedInt();
                                                break;
                                            }
                                        }
                                    }
                                } catch (Exception e) {
                                    XLog.XLogLogger.error(e);
                                }
                            }
                            break;

                        case Polarx.ServerMessages.Type.ERROR_VALUE:
                            final Polarx.Error error = (Polarx.Error) msg.getPacket();
                            synchronized (authMessage) {
                                authMessage.set(error.getMsg());
                                authMessage.notify();
                            }
                            XLog.XLogLogger.error(XClient.this + " auth fatal. msg: " + error.getMsg());
                            break;
                        }
                        continue;
                    }

                    if (null == lastSession || sessionId != lastSession.getSessionId()) {
                        // Not initialized or session changed.
                        if (lastSession != null) {
                            lastSession.setLastPacketNanos(nowNanos);
                            lastSession.pushPackets(packets);
                        }

                        // Init.
                        lastSession = workingSessionMap.get(sessionId);
                        if (null == lastSession) {
                            continue;
                        }
                        packets = new ArrayList<>(16);

                        // Attach.
                        if (logEnabled) {
                            // Apply thread context.
                            lastSession.applyThreadContext();
                            logPacket(msg, false, false);
                        }
                        packets.add(msg);
                    } else {
                        if (logEnabled) {
                            logPacket(msg, false, false);
                        }
                        packets.add(msg);
                    }
                }

                // Dealing rest.
                if (lastSession != null) {
                    lastSession.setLastPacketNanos(nowNanos);
                    lastSession.pushPackets(packets);
                }
            } catch (Throwable e) {
                XLog.XLogLogger.error(e);
                setFatalError(e);
            } finally {
                if (logEnabled) {
                    // Restore.
                    MDC.setContextMap(savedMdcContext);
                }
            }
        }, this::setFatalError, this);
        this.connectTimeoutNanos = connectTimeoutNanos;
    }

    public XClientPool getPool() {
        return pool;
    }

    public int getWorkingSessionCount() {
        return workingSessionMap.size();
    }

    public Map<Long, XSession> getWorkingSessionMap() {
        return workingSessionMap;
    }

    public Throwable getFatalError() {
        return fatalError.get();
    }

    public void setFatalError(Throwable fatalError) {
        this.fatalError.set(fatalError);
        if (fatalError != null) {
            // Shutdown all connection on this client by insert a fatal message.
            for (XSession session : workingSessionMap.values()) {
                session.pushFatal(new XPacket(-1, Polarx.ServerMessages.Type.ERROR_VALUE,
                    Polarx.Error.newBuilder().setSeverity(Polarx.Error.Severity.FATAL).setCode(-1)
                        .setSqlState(this + " fatal.")
                        .setMsg(fatalError.getMessage()).build()));
            }
        }
        // Close anyway.
        close();
    }

    public TcpPerfCollection getPerfCollection() {
        return perfCollection;
    }

    public String getTcpTag() {
        final String tag = nioClient.getTag();
        return null == tag ? "<unestablished>" : tag;
    }

    public TcpPerfItem getPerfItem() {
        final TcpPerfItem item = new TcpPerfItem();

        item.setTcpTag(getTcpTag());
        item.setDnTag(pool.getDnTag());

        item.setSendMsgCount(perfCollection.getSendMsgCount().get());
        item.setSendFlushCount(perfCollection.getSendFlushCount().get());
        item.setSendSize(perfCollection.getSendSize().get());
        item.setRecvMsgCount(perfCollection.getRecvMsgCount().get());
        item.setRecvNetCount(perfCollection.getRecvNetCount().get());
        item.setRecvSize(perfCollection.getRecvSize().get());

        item.setSessionCreateCount(perfCollection.getSessionCreateCount().get());
        item.setSessionDropCount(perfCollection.getSessionDropCount().get());
        item.setSessionCreateSuccessCount(perfCollection.getSessionCreateSuccessCount().get());
        item.setSessionCreateFailCount(perfCollection.getSessionCreateFailCount().get());
        item.setSessionActiveCount(perfCollection.getSessionActiveCount().get());

        item.setSessionCount(workingSessionMap.size());

        final long nowNanos = System.nanoTime();
        item.setClientState(state);
        final long lastPkg = lastPacketNanos.get();
        item.setIdleNanosSinceLastPacket(0 == lastPkg ? 0 : nowNanos - lastPkg);
        item.setLiveNanos(nowNanos - createNanos);
        final Throwable t = fatalError.get();
        item.setFatalError(null == t ? null : t.getMessage());
        item.setAuthId(authClientId);
        long lastVar = pool.getLastVariablesNanos().get();
        if (0 == lastVar || createNanos - lastVar > 0) {
            lastVar = createNanos;
        }
        item.setNanosSinceLastVariablesFlush(nowNanos - lastVar);

        nioClient.fillTcpPhysicalInfo(item);
        return item;
    }

    public void gatherSessionPerf(List<SessionPerfItem> list) {
        final XSession[] sessions = workingSessionMap.values().toArray(new XSession[0]);
        for (XSession session : sessions) {
            list.add(session.getPerfItem());
        }
    }

    @Override
    public void close() {
        nioClient.close();
    }

    private void scramble(long sessionId, ByteString challenge) throws Exception {
        boolean useSHA2 = DynamicConfig.getInstance().getUseSha2PasswordForBackend();
        final String encoding = "UTF8";
        final byte[] userBytes = pool.getUsername() == null ? new byte[] {} : pool.getUsername().getBytes(encoding);
        final byte[] passwordBytes =
            pool.getPassword() == null || pool.getPassword().length() == 0 ? new byte[] {} :
                pool.getPassword().getBytes(encoding);
        final byte[] databaseBytes = new byte[] {};

        byte[] hashedPassword = passwordBytes;
        if (pool.getPassword() != null && pool.getPassword().length() > 0) {
            // use challenge last byte to identify if we should use sha2
            // old dn's challenge only have 20byte salt
            useSHA2 = useSHA2 ? (challenge.size() > 20 && challenge.byteAt(challenge.size() - 1) == '1') : useSHA2;
            if (useSHA2) {
                // extract challenge , rounds and salt
                // 1{20 byte challenge}{3 byte roundsByte}${20 byte salt}
                // rounds = roundsByte * 1000
                byte[] challengeByte = challenge.substring(0, 20).toByteArray();
                byte[] roundsByte = challenge.substring(20, 23).toByteArray();
                int rounds = Integer.parseInt(new String(roundsByte, encoding)) * 1000;
                byte[] saltByte = challenge.substring(24, challenge.size() - 1).toByteArray();
                byte[] sha2AsPassword = SecurityUtil.scrambleSHA2(passwordBytes, saltByte, rounds);
                // use sha2 result as password
                hashedPassword = SecurityUtil.scramble411(sha2AsPassword, challengeByte);
            } else {
                hashedPassword =
                    SecurityUtil.scramble411(passwordBytes, challenge.substring(0, 20).toByteArray());
            }
            // protocol dictates *-prefixed hex string as hashed password
            hashedPassword = String.format("*%040x", new java.math.BigInteger(1, hashedPassword)).getBytes();
        }

        // this is what would happen in the SASL provider but we don't need the overhead of all the plumbing.
        final byte[] reply = new byte[databaseBytes.length + userBytes.length + hashedPassword.length + 2];

        // reply is length-prefixed when sent so we just separate fields by \0
        System.arraycopy(databaseBytes, 0, reply, 0, databaseBytes.length);
        int pos = databaseBytes.length;
        reply[pos++] = 0;
        System.arraycopy(userBytes, 0, reply, pos, userBytes.length);
        pos += userBytes.length;
        reply[pos++] = 0;
        System.arraycopy(hashedPassword, 0, reply, pos, hashedPassword.length);

        PolarxSession.AuthenticateContinue.Builder builder = PolarxSession.AuthenticateContinue.newBuilder();
        builder.setAuthData(ByteString.copyFrom(reply));
        final XPacket packet =
            new XPacket(sessionId, Polarx.ClientMessages.Type.SESS_AUTHENTICATE_CONTINUE_VALUE, builder.build());
        send(packet, true);
    }

    /**
     * Client functions.
     */

    private void logPacket(XPacket packet, boolean send, boolean flush) {
        final StringBuilder builder = new StringBuilder();
        builder.append('\n')
            .append("{ ").append(toString())
            .append(" ").append(send ? "send" : "recv")
            .append(flush ? " flush" : "").append('\n');
        builder.append("sid: ").append(packet.getSid())
            .append(" type: ").append(packet.getType()).append('\n')
            .append(packet.getPacket());
        try {
            if (packet.getType() == Polarx.ServerMessages.Type.NOTICE_VALUE) {
                PolarxNotice.Frame frame = (PolarxNotice.Frame) packet.getPacket();
                switch (frame.getType()) {
                case 1:
                    builder.append(PolarxNotice.Warning.parseFrom(frame.getPayload()));
                    break;

                case 2:
                    builder.append(PolarxNotice.SessionVariableChanged.parseFrom(frame.getPayload()));
                    break;

                case 3:
                    builder.append(PolarxNotice.SessionStateChanged.parseFrom(frame.getPayload()));
                    break;
                }
            }
        } catch (Throwable ignore) {
        }
        builder.append('}');
        XLog.XProtocolLogger.info(builder.toString().replace('\n', ' '));
    }

    public boolean reuseSession(XSession session) {
        return pool.reuseSession(session);
    }

    public void dropSession(XSession session) {
        final XSession freed = workingSessionMap.remove(session.getSessionId());
        if (freed != null) {
            // Record on per TCP and DN gather.
            perfCollection.getSessionDropCount().getAndIncrement();
            pool.getPerfCollection().getSessionDropCount().getAndIncrement();
            // Close it anyway.
            freed.close();
        }
    }

    public XSession newXSession(AtomicLong sessionIdGenerator, boolean autoCommit) {
        final XSession session;
        session = new XSession(
            this,
            sessionIdGenerator.getAndIncrement() | (
                autoCommit && XConnectionManager.getInstance().isEnableAutoCommitOptimize() ?
                    0x8000_0000_0000_0000L : 0),
            autoCommit && XConnectionManager.getInstance().isEnableAutoCommitOptimize() ?
                XSession.Status.AutoCommit :
                XSession.Status.Init);

        // Got good session.
        try {
            if (XLog.XProtocolLogger.isInfoEnabled()) {
                session.recordThreadContext();
            }
            final XSession previous = workingSessionMap.put(session.getSessionId(), session);
            // Record on per TCP and DN gather.
            perfCollection.getSessionCreateCount().getAndIncrement();
            pool.getPerfCollection().getSessionCreateCount().getAndIncrement();
            assert null == previous;
            XLog.XLogLogger.info("New session: " + session);
            return session;
        } catch (Throwable e) {
            final XSession removed = workingSessionMap.remove(session.getSessionId());
            if (removed != null) {
                // Record on per TCP and DN gather.
                perfCollection.getSessionDropCount().getAndIncrement();
                pool.getPerfCollection().getSessionDropCount().getAndIncrement();
            }
            session.close();
            XLog.XLogLogger.error(e);
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT,
                this + " create XSession error: " + e.getMessage());
        }
    }

    public XConnection newXConnection(AtomicLong sessionIdGenerator) {
        return newXConnection(sessionIdGenerator, false);
    }

    public XConnection newXConnection(AtomicLong sessionIdGenerator, boolean autoCommit) {
        final XSession session = newXSession(sessionIdGenerator, autoCommit);

        // Got good session.
        try {
            return new XConnection(session);
        } catch (Throwable e) {
            final XSession removed = workingSessionMap.remove(session.getSessionId());
            if (removed != null) {
                // Record on per TCP and DN gather.
                perfCollection.getSessionDropCount().getAndIncrement();
                pool.getPerfCollection().getSessionDropCount().getAndIncrement();
            }
            session.close();
            XLog.XLogLogger.error(e);
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT,
                this + " create XConnection error: " + e.getMessage());
        }
    }

    public void send(XPacket packet, boolean flush) {
        if (XLog.XProtocolLogger.isInfoEnabled()) {
            logPacket(packet, true, flush);
        }
        try {
            nioClient.write(packet, flush);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void flush() {
        nioClient.flush();
    }

    public void shrinkBuffer() {
        nioClient.shrinkBuffer();
    }

    public boolean waitChannel() {
        boolean done = false;
        try {
            state = ClientState.Connecting;
            final long startTime = System.currentTimeMillis();
            nioClient.connect(new InetSocketAddress(pool.getHost(), pool.getPort()),
                (int) (connectTimeoutNanos / 1000_000L));
            state = ClientState.Registering;
            final NIOProcessor processor = nioWorker.getProcessor();
            nioClient.setProcessor(processor);
            processor.postRegister(nioClient);
            // Need wait valid.
            final long waitTime = System.currentTimeMillis();
            while (!nioClient.isValid()) {
                final long now = System.currentTimeMillis();
                if (now > startTime + (connectTimeoutNanos / 1000_000L) && now > waitTime + 1000) {
                    XLog.XLogLogger.error(this + " connect wait valid timeout, " + connectTimeoutNanos + "ns.");
                    return false; // Wait timeout.
                }
                Thread.yield(); // Wait till registered.
            }
            state = ClientState.Connected;
            done = true;
        } catch (Throwable e) {
            XLog.XLogLogger.error(e);
        } finally {
            connected.set(done);
        }
        return done;
    }

    private void authStart(long sessionId) throws UnsupportedEncodingException {
        final PolarxSession.AuthenticateStart.Builder builder = PolarxSession.AuthenticateStart.newBuilder();
        builder.setMechName("MYSQL41");
        boolean useSHA2 = DynamicConfig.getInstance().getUseSha2PasswordForBackend();
        if (useSHA2) {
            // sha2_password needs username
            final String encoding = "UTF8";
            final byte[] userBytes = pool.getUsername() == null ? new byte[] {} : pool.getUsername().getBytes(encoding);
            builder.setAuthData(ByteString.copyFrom(userBytes));
        }
        final XPacket packet =
            new XPacket(sessionId, Polarx.ClientMessages.Type.SESS_AUTHENTICATE_START_VALUE, builder.build());
        send(packet, true);
    }

    public void refreshVariables(AtomicLong sessionIdGenerator, long timeoutNanos, boolean isClientInit)
        throws Exception {
        // update on shared client pool variables stores
        if (pool.getGlobalVariablesL() != null && pool.getSessionVariablesL() != null) {
            if (isClientInit) {
                return; // just treated it as valid one
            }
            final long lastVariablesNanos = pool.getLastVariablesNanos().get();
            if (lastVariablesNanos != 0 && System.nanoTime() - lastVariablesNanos
                < XConnectionManager.getInstance().getSessionAgingNanos()) {
                return;
            }
        }

        // lock and update
        final long startNanos = System.nanoTime();
        final long endNanos;
        synchronized (pool.getLastVariablesNanos()) {
            // check again
            final long lastVariablesNanos = pool.getLastVariablesNanos().get();
            if (null == pool.getGlobalVariablesL() || pool.getSessionVariablesL() == null || 0 == lastVariablesNanos
                || System.nanoTime() - lastVariablesNanos >= XConnectionManager.getInstance().getSessionAgingNanos()) {
                // do update variables
                try {
                    Map<String, Object> variables = new HashMap<>();
                    try (final XConnection connection = newXConnection(sessionIdGenerator,
                        XConnectionManager.getInstance().isEnableAutoCommitOptimize())) {
                        connection.setNetworkTimeoutNanos(timeoutNanos);
                        connection.init(timeoutNanos);
                        final XResult result = connection.execQuery("show variables");
                        while (result.next() != null) {
                            String key = (String) XResultUtil.resultToObject(
                                result.getMetaData().get(0), result.current().getRow().get(0), true,
                                TimeZone.getDefault()).getKey(); // Ignore time zone.
                            Pair<Object, byte[]> val = XResultUtil.resultToObject(
                                result.getMetaData().get(1), result.current().getRow().get(1), true,
                                TimeZone.getDefault()); // Ignore time zone.
                            variables.put(key.toLowerCase(), val.getKey());
                        }
                    }
                    pool.setSessionVariablesL(Collections.unmodifiableMap(variables));

                    // Get global variable.
                    variables = new HashMap<>();
                    try (final XConnection connection = newXConnection(sessionIdGenerator,
                        XConnectionManager.getInstance().isEnableAutoCommitOptimize())) {
                        connection.setNetworkTimeoutNanos(timeoutNanos);
                        connection.init(timeoutNanos);
                        final XResult result = connection.execQuery("show global variables");
                        while (result.next() != null) {
                            String key = (String) XResultUtil.resultToObject(
                                result.getMetaData().get(0), result.current().getRow().get(0), true,
                                TimeZone.getDefault()).getKey(); // Ignore time zone.
                            Pair<Object, byte[]> val = XResultUtil.resultToObject(
                                result.getMetaData().get(1), result.current().getRow().get(1), true,
                                TimeZone.getDefault()); // Ignore time zone.
                            variables.put(key.toLowerCase(), val.getKey());
                        }
                    }
                    pool.setGlobalVariablesL(Collections.unmodifiableMap(variables));
                } finally {
                    // No matter success or not, refresh time first to prevent fail with timeout.
                    endNanos = System.nanoTime();
                    if (0 == lastVariablesNanos) {
                        pool.getLastVariablesNanos().set(endNanos - // Next will random among the lifetime.
                            ThreadLocalRandom.current()
                                .nextLong(XConnectionManager.getInstance().getSessionAgingNanos()));
                    } else {
                        pool.getLastVariablesNanos().set(endNanos);
                    }
                }
            } else {
                endNanos = lastVariablesNanos;
            }
        }

        // record slow log
        if (endNanos - startNanos > TimeUnit.MILLISECONDS.toNanos(100)) {
            XLog.XLogLogger.warn("Slow refresh variables on " + this + " cost " + (endNanos - startNanos) + "ns");
        }
    }

    public void initClient(AtomicLong sessionIdGenerator, long timeoutNanos) throws Exception {
        // Auth first.
        if (XConnectionManager.getInstance().isEnableAuth()) {
            state = ClientState.Authing;
            authStart(0);
            synchronized (authMessage) {
                authMessage.wait(timeoutNanos / 1000000L);
            }
            final String authResult = authMessage.get();
            if (null == authResult) {
                EventLogger.log(EventType.XRPC_AUTH_TIMEOUT, "Auth timeout " + this);
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT,
                    this + " auth timeout. " + timeoutNanos + "ns");
            }
            if (!authResult.equals(AUTH_OK_MARK)) {
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT, authResult);
            }
            state = ClientState.Authed;
        }

        // Init session variables.
        state = ClientState.LoadVariables;
        refreshVariables(sessionIdGenerator, timeoutNanos, true);
        state = ClientState.Ready;

        // show info to event log
        final boolean xrpc;
        final Map<String, Object> globalVariablesL = pool.getGlobalVariablesL();
        if (globalVariablesL != null) {
            final Object val = globalVariablesL.get("new_rpc");
            xrpc = val instanceof String && ((String) val).equalsIgnoreCase("ON");
        } else {
            xrpc = false;
        }
        final String instId = XConnectionManager.getInstance().getInstId().get();
        EventLogger.log(EventType.XRPC_NEW_VALID_CLIENT,
            "New authed " + this
                + " @@ " + String.join(",", pool.getInstInfo())
                + " @@ " + (null == instId ? "unknown" : instId)
                + " @@ " + (xrpc ? "XRPC" : "XPROTO"));
        XLog.XLogLogger.info("New client: " + this);
    }

    // Global variables(key with lower case).
    public Map<String, Object> getGlobalVariablesL() {
        return pool.getGlobalVariablesL();
    }

    // Session variables(key with lower case).
    public Map<String, Object> getSessionVariablesL() {
        return pool.getSessionVariablesL();
    }

    public enum DnBaseVersion {
        DN_UNKNOWN,
        DN_X_CLUSTER,
        DN_POLAR_DB_80_3AZ,
        DN_RDS_80_X_CLUSTER
    }

    private DnBaseVersion baseVersion = null;
    private String majorVersion = null;
    private String minorVersion = null;

    public void decodeVersion() {
        final String version = (String) pool.getGlobalVariablesL().get("version");
        final String xdbMark = "X-Cluster";
        // online: 5.7.38-X-Cluster-1.6.1.5-20240403-log or 5.7.14-AliSQL-X-Cluster-1.6.0.8-20210719-log
        // local: 5.7.38-X-Cluster-1.6.1.5-log or 5.7.14-AliSQL-X-Cluster-1.6.0.8-log
        final String polardb80Mark = "polardb-3az"; // eg. 8.0.13-polardb-3az-20210723-110821
        try {
            final int idx = version.indexOf(xdbMark);
            final int idx2 = version.indexOf(polardb80Mark);
            if (idx != -1) {
                final int midIdx = version.indexOf('-', idx + xdbMark.length() + 1);
                final int lastIdx = midIdx != -1 ? version.indexOf('-', midIdx + 1) : -1;
                baseVersion =
                    version.startsWith("5.7.") ? DnBaseVersion.DN_X_CLUSTER : DnBaseVersion.DN_RDS_80_X_CLUSTER;
                majorVersion = midIdx != -1 ? version.substring(idx + xdbMark.length() + 1, midIdx) :
                    version.substring(idx + xdbMark.length() + 1);
                minorVersion = lastIdx != -1 ? version.substring(midIdx + 1, lastIdx) : version.substring(midIdx + 1);
            } else if (idx2 != -1) {
                final int midIdx = version.indexOf('-', idx2 + polardb80Mark.length() + 1);
                final int lastIdx = midIdx != -1 ? version.indexOf('-', midIdx + 1) : -1;
                baseVersion = DnBaseVersion.DN_POLAR_DB_80_3AZ;
                majorVersion = midIdx != -1 ? version.substring(idx2 + polardb80Mark.length() + 1, midIdx) :
                    version.substring(idx2 + polardb80Mark.length() + 1);
                minorVersion = lastIdx != -1 ? version.substring(midIdx + 1, lastIdx) : version.substring(midIdx + 1);
            } else { // TODO: Add more DN base version here.
                baseVersion = DnBaseVersion.DN_UNKNOWN;
                majorVersion = minorVersion = "unknown";
            }
            XLog.XLogLogger.info(
                this + " get " + baseVersion.name() + " version: " + majorVersion + " " + minorVersion);
        } catch (Throwable t) {
            baseVersion = DnBaseVersion.DN_UNKNOWN;
            majorVersion = minorVersion = "unknown";
            XLog.XLogLogger.error(this + " get version error: " + version);
            XLog.XLogLogger.error(t);
        }
    }

    public DnBaseVersion getBaseVersion() {
        if (baseVersion != null) {
            return baseVersion;
        }

        decodeVersion();
        return baseVersion;
    }

    public String getMajorVersion() {
        if (majorVersion != null) {
            return majorVersion;
        }

        decodeVersion();
        return majorVersion;
    }

    public String getMinorVersion() {
        if (minorVersion != null) {
            return minorVersion;
        }

        decodeVersion();
        return minorVersion;
    }

    /**
     * Client state checker.
     */

    public boolean isOld() {
        return System.nanoTime() - createNanos - randomDelay >
            DynamicConfig.getInstance().getXprotoTcpAging() * 1000_000_000L;
    }

    public void markNotReusable() {
        reusable.set(false);
    }

    public boolean reusable() {
        return reusable.get();
    }

    public boolean isActive() {
        return null == getFatalError() && connected.get() && state == ClientState.Ready
            && pool.getGlobalVariablesL() != null && pool.getSessionVariablesL() != null;
    }

    public boolean isBad() {
        return getFatalError() != null;
    }

    public boolean needProb() {
        final boolean b = connected.get() // TCP connected.
            // Init completed.
            && state == ClientState.Ready && pool.getGlobalVariablesL() != null && pool.getSessionVariablesL() != null
            && nioClient.idleTime() > XConfig.DEFAULT_PROBE_IDLE_NANOS;
        if (!b) {
            // reset fail times.
            probeFailTimes.set(0);
        }
        return b;
    }

    private void capabilitiesGet(long sessionId) {
        final PolarxConnection.CapabilitiesGet.Builder builder = PolarxConnection.CapabilitiesGet.newBuilder();
        final XPacket packet =
            new XPacket(sessionId, Polarx.ClientMessages.Type.CON_CAPABILITIES_GET_VALUE, builder.build());
        send(packet, true);
    }

    public boolean probe(AtomicLong sessionIdGenerator, long timeoutNanos) {
        if (XConfig.GALAXY_X_PROTOCOL) {
            try (XConnection connection = newXConnection(sessionIdGenerator,
                XConnectionManager.getInstance().isEnableAutoCommitOptimize())) {
                connection.init(timeoutNanos);
                connection.setNetworkTimeoutNanos(timeoutNanos);
                connection.init(timeoutNanos);
                XResult result = connection.execQuery("/*X probe*/ select 1");
                long count = 0;
                while (result.next() != null) {
                    Number res = (Number) XResultUtil.resultToObject(
                        result.getMetaData().get(0), result.current().getRow().get(0), true,
                        TimeZone.getDefault()).getKey();
                    count += res.longValue();
                }
                if (1 == count) {
                    probeFailTimes.set(0);
                    return true;
                }
            } catch (Throwable t) {
                XLog.XLogLogger.error(t); // Just log and ignore.
            }
        } else {
            // use capabilities to check TCP
            try {
                probeEvent.set(false);
                capabilitiesGet(0);
                synchronized (probeEvent) {
                    if (!probeEvent.get()) {
                        probeEvent.wait(timeoutNanos / 1000000L);
                    }
                }
                if (probeEvent.get()) {
                    // good
                    probeFailTimes.set(0);
                    return true;
                }
            } catch (Throwable t) {
                XLog.XLogLogger.error(t); // Just log and ignore.
            }
        }
        final int failTimes = probeFailTimes.addAndGet(1);
        XLog.XLogLogger.warn(this + " probe fail times: " + failTimes + " idle: " + nioClient.idleTime() + "ns");
        return failTimes < XConfig.DEFAULT_PROBE_RETRY_TIMES;
    }

    @Override
    public String toString() {
        return "XClient of " + nioClient + " to " + pool.getDnTag();
    }
}
