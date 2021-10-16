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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.XLog;
import com.alibaba.polardbx.rpc.client.XClient;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.packet.XPacket;
import com.alibaba.polardbx.rpc.perf.DnPerfCollection;
import com.alibaba.polardbx.rpc.perf.DnPerfItem;
import com.alibaba.polardbx.rpc.perf.SessionPerfItem;
import com.alibaba.polardbx.rpc.perf.TcpPerfItem;
import com.alibaba.polardbx.rpc.result.XResult;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * @version 1.0
 */
public class XClientPool {

    // Base info.
    private final XConnectionManager manager;
    private final String host;
    private final int port;
    private final String username;
    private final String password;

    // XClient(TCP connections).
    private final List<XClient> clients = new CopyOnWriteArrayList<>();
    private final AtomicInteger clientRR = new AtomicInteger(0);

    // Aging connections.
    private final List<XClient> agingClients = new CopyOnWriteArrayList<>();

    // Reference count for multi datasource same instance.
    private final AtomicInteger refCount = new AtomicInteger(0);

    // For idle connections.
    private final Queue<XSession> idleSessions = new ConcurrentLinkedQueue<>();
    private final AtomicInteger idleCount = new AtomicInteger(0);

    // Perf collection.
    private final DnPerfCollection perfCollection = new DnPerfCollection();

    public XClientPool(XConnectionManager manager, String host, int port, String username, String password) {
        this.manager = manager;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getNowPoolSize() {
        return clients.size();
    }

    private int getAliveSessionCount() {
        int retry = 0;
        int count = 0;
        while (retry < 10) {
            try {
                final int clientSize = clients.size();
                for (int i = 0; i < clientSize; ++i) {
                    count += clients.get(i).getWorkingSessionCount();
                }
                return count;
            } catch (IndexOutOfBoundsException ignore) {
                count = 0;
                ++retry;
            }
        }
        return 0;
    }

    public static class XStatus {
        public int client;
        public int idleSession;
        public int workingSession;
    }

    public XStatus getStatus() {
        final XStatus status = new XStatus();
        status.client = clients.size();
        status.idleSession = idleCount.get();
        status.workingSession = getAliveSessionCount();
        if (status.workingSession >= status.idleSession) {
            status.workingSession -= status.idleSession;
        } else {
            status.workingSession = 0;
        }
        return status;
    }

    public AtomicInteger getRefCount() {
        return refCount;
    }

    public boolean isInUse() {
        return refCount.get() > 0; // Ref count will sub to 0 before shutdown.
    }

    public DnPerfCollection getPerfCollection() {
        return perfCollection;
    }

    private DnPerfItem lastItem = null;
    private long lastNanos = 0;

    public String getDnTag() {
        return XConnectionManager.digest(host, port, username);
    }

    public DnPerfItem getPerfItem() {
        final DnPerfItem item = new DnPerfItem();

        item.setDnTag(getDnTag());

        item.setSendMsgCount(perfCollection.getSendMsgCount().get());
        item.setSendFlushCount(perfCollection.getSendFlushCount().get());
        item.setSendSize(perfCollection.getSendSize().get());
        item.setRecvNetCount(perfCollection.getRecvNetCount().get());
        item.setRecvMsgCount(perfCollection.getRecvMsgCount().get());
        item.setRecvSize(perfCollection.getRecvSize().get());

        item.setSessionCreateCount(perfCollection.getSessionCreateCount().get());
        item.setSessionDropCount(perfCollection.getSessionDropCount().get());
        item.setSessionCreateSuccessCount(perfCollection.getSessionCreateSuccessCount().get());
        item.setSessionCreateFailCount(perfCollection.getSessionCreateFailCount().get());
        item.setSessionActiveCount(perfCollection.getSessionActiveCount().get());
        item.setGetConnectionCount(perfCollection.getGetConnectionCount().get());

        synchronized (this) {
            if (null == lastItem) {
                lastItem = item;
                lastNanos = System.nanoTime();
            } else {
                // Generate rate.
                final long nanos = System.nanoTime();
                final long delta = nanos - lastNanos;
                if (delta > 0) {
                    item.setSendMsgRate((item.getSendMsgCount() - lastItem.getSendMsgCount()) * 1e9d / delta);
                    item.setSendFlushRate((item.getSendFlushCount() - lastItem.getSendFlushCount()) * 1e9d / delta);
                    item.setSendRate((item.getSendSize() - lastItem.getSendSize()) * 1e9d / delta);
                    item.setRecvMsgRate((item.getRecvMsgCount() - lastItem.getRecvMsgCount()) * 1e9d / delta);
                    item.setRecvNetRate((item.getRecvNetCount() - lastItem.getRecvNetCount()) * 1e9d / delta);
                    item.setRecvRate((item.getRecvSize() - lastItem.getRecvSize()) * 1e9d / delta);
                    item.setSessionCreateRate(
                        (item.getSessionCreateCount() - lastItem.getSessionCreateCount()) * 1e9d / delta);
                    item.setSessionDropRate(
                        (item.getSessionDropCount() - lastItem.getSessionDropCount()) * 1e9d / delta);
                    item.setSessionCreateSuccessRate(
                        (item.getSessionCreateSuccessCount() - lastItem.getSessionCreateSuccessCount()) * 1e9d / delta);
                    item.setSessionCreateFailRate(
                        (item.getSessionCreateFailCount() - lastItem.getSessionCreateFailCount()) * 1e9d / delta);
                    item.setGatherTimeDelta(delta / 1e9d);
                }
                // Do update if more than 1s.
                if (delta >= 1e9) {
                    lastItem = item;
                    lastNanos = nanos;
                }
            }
        }

        // Gather TCP count.
        final int aliveSize = clients.size();
        final int agingSize = agingClients.size();
        item.setTcpTotalCount(aliveSize + agingSize);
        item.setTcpAgingCount(agingSize);

        // Gather session count.
        List<XClient> copy = new ArrayList<>();
        copy.addAll(clients);
        copy.addAll(agingClients);
        item.setSessionCount(copy.stream().mapToLong(XClient::getWorkingSessionCount).sum());

        item.setSessionIdleCount(idleCount.get());
        item.setRefCount(refCount.get());

        return item;
    }

    public void gatherTcpPerf(List<TcpPerfItem> list) {
        final XClient[] working = clients.toArray(new XClient[0]);
        for (XClient client : working) {
            final TcpPerfItem tcp = client.getPerfItem();
            tcp.setTcpState(TcpPerfItem.TcpState.Working);
            list.add(tcp);
        }
        final XClient[] aging = agingClients.toArray(new XClient[0]);
        for (XClient client : aging) {
            final TcpPerfItem tcp = client.getPerfItem();
            tcp.setTcpState(TcpPerfItem.TcpState.Aging);
            list.add(tcp);
        }
    }

    public void gatherSessionPerf(List<SessionPerfItem> list) {
        final XClient[] working = clients.toArray(new XClient[0]);
        for (XClient client : working) {
            client.gatherSessionPerf(list);
        }
        final XClient[] aging = agingClients.toArray(new XClient[0]);
        for (XClient client : aging) {
            client.gatherSessionPerf(list);
        }
    }

    // Put idle connection to pool if ok.
    public boolean reuseSession(XSession session) {
        if (idleCount.get() < manager.getMaxPooledSessionPerInstance() && session.reusable()) {
            idleCount.getAndIncrement();
            idleSessions.offer(session); // Note: ConcurrentLinkedQueue always true and non-block.
            return true;
        }
        return false;
    }

    public XConnection getConnection(BiFunction<XClient, XPacket, Boolean> consumer, long timeoutNanos)
        throws Exception {
        final long concurrent = perfCollection.getSessionActiveCount().get();
        final long wait = perfCollection.getGetConnectionCount().get();
        final long waitThresh =
            XConfig.GALAXY_X_PROTOCOL ? 512 : DynamicConfig.getInstance().getXprotoMaxDnWaitConnection();
        if (concurrent > DynamicConfig.getInstance().getXprotoMaxDnConcurrent() ||
            wait > waitThresh) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT,
                this + " max concurrent or wait exceed. now concurrent: " + concurrent + " wait: " + wait);
        }

        try {
            perfCollection.getGetConnectionCount().getAndIncrement();
            return getConnection(consumer, timeoutNanos, false);
        } finally {
            perfCollection.getGetConnectionCount().getAndDecrement();
        }
    }

    public XConnection getConnection(BiFunction<XClient, XPacket, Boolean> consumer, long timeoutNanos,
                                     boolean forceAlloc)
        throws Exception {
        final long startNanos = System.nanoTime();

        // Get idle or create new connection.
        scan:
        while (true) {
            if (!isInUse()) {
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT, this + " not in use.");
            }

            if (!forceAlloc) {
                // Get from idle first.
                for (XSession pooled; (pooled = idleSessions.poll()) != null; ) {
                    idleCount.getAndDecrement();
                    try {
                        if (pooled.reset()) {
                            if (XLog.XProtocolLogger.isInfoEnabled()) {
                                pooled.recordThreadContext();
                            }
                            try {
                                final XConnection connection = new XConnection(pooled);
                                connection.init();
                                connection.setConnectNano(0);
                                connection.setWaitNano(System.nanoTime() - startNanos);
                                return connection;
                            } catch (Throwable e) {
                                XLog.XLogLogger.error(e);
                            }
                        }
                    } catch (Throwable ignore) {
                    }
                    pooled.getClient().dropSession(pooled);
                }
            }

            // Get available in pool.
            final int clientSize = clients.size();

            // Try get no session client.
            for (int i = 0; i < clientSize; ++i) {
                final XClient client;
                try {
                    client = clients.get(i);
                } catch (IndexOutOfBoundsException ignore) {
                    continue scan;
                }
                if (!client.isActive()) {
                    continue;
                }

                if (0 == client.getWorkingSessionCount()) {
                    final XConnection connection = client.newXConnection(manager.getIdGenerator());
                    try {
                        connection.init();
                        connection.setConnectNano(0);
                        connection.setWaitNano(System.nanoTime() - startNanos);
                        return connection;
                    } catch (Throwable e) {
                        XLog.XLogLogger.error(e);
                        connection.close();
                        throw e;
                    }
                }
            }

            // No idle client, try reuse when not client first or client reach the limit.
            if (!XConfig.CLIENT_FIRST_STRATEGY || clientSize >= manager.getMaxClientPerInstance()) {
                for (int i = 0; i < clientSize; ++i) {
                    final XClient client;
                    try {
                        client = clients.get(clientRR.getAndIncrement() % clientSize);
                    } catch (IndexOutOfBoundsException ignore) {
                        continue scan;
                    }
                    if (!client.isActive()) {
                        continue;
                    }

                    // Note client.getWorkingSessionCount() may less than real session number.
                    if (client.getWorkingSessionCount() < manager.getMaxSessionPerClient()) {
                        final XConnection connection = client.newXConnection(manager.getIdGenerator());
                        try {
                            connection.init();
                            connection.setConnectNano(0);
                            connection.setWaitNano(System.nanoTime() - startNanos);
                            return connection;
                        } catch (Throwable e) {
                            XLog.XLogLogger.error(e);
                            connection.close();
                            throw e;
                        }
                    }
                }
            }

            // Available client not found, need wait or create new one.
            final XClient newClient;
            final XConnection newConnection;
            synchronized (clients) {
                final int nowPoolSize = clients.size();
                final long nowNanos = System.nanoTime();
                if (nowNanos - startNanos >= timeoutNanos) {
                    throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CONNECTION,
                        this + " get XConnection timeout. " + timeoutNanos + "ns");
                } else {
                    if (nowPoolSize < manager.getMaxClientPerInstance()) {
                        // Allocate new client.
                        newClient = new XClient(manager.getEventWorker(), this, consumer, timeoutNanos);
                        try {
                            newConnection = newClient.newXConnection(manager.getIdGenerator());
                        } catch (Throwable e) {
                            XLog.XLogLogger.error(e);
                            newClient.close();
                            throw e;
                        }
                        clients.add(newClient);
                    } else {
                        newClient = null;
                        newConnection = null;
                    }
                }
            }
            if (null == newClient) {
                Thread.sleep(10); // Client exceed the limit and no available session, so wait for 10ms.
                continue;
            }
            final long connectStartNanos = System.nanoTime();
            // Wait new client ready.
            try {
                if (!newClient.waitChannel()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT, this + " connect fail.");
                }
                // Init client.
                final long nowNanos = System.nanoTime();
                long initTimeoutNanos = startNanos + timeoutNanos - nowNanos; // Calculate the rest time.
                if (initTimeoutNanos <= XConfig.MIN_INIT_TIMEOUT_NANOS) {
                    initTimeoutNanos = XConfig.MIN_INIT_TIMEOUT_NANOS;
                }
                newClient.initClient(manager.getIdGenerator(), initTimeoutNanos);
                newClient.initWithConnection(newConnection, initTimeoutNanos);
            } catch (Exception e) {
                // Connection fail.
                try {
                    newConnection.close(); // Close connection.
                } catch (Throwable t) {
                    XLog.XLogLogger.error(t);
                }
                removeClient(newClient, e.getMessage());
                throw e;
            }

            // Recheck and release if pool released(To prevent release and pre-allocate concurrently).
            if (!isInUse()) {
                try {
                    newConnection.close(); // Close connection.
                } catch (Throwable t) {
                    XLog.XLogLogger.error(t);
                }
                final Exception e = new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT, this + " not in use.");
                removeClient(newClient, e.getMessage());
                throw e;
            }

            try {
                newConnection.init();
                // Record time.
                newConnection.setWaitNano(connectStartNanos - startNanos);
                newConnection.setConnectNano(System.nanoTime() - connectStartNanos);
                return newConnection;
            } catch (Throwable e) {
                XLog.XLogLogger.error(e);
                newConnection.close();
                throw e;
            }
        }
    }

    private void removeClient(XClient client, String msg) {
        synchronized (clients) {
            clients.remove(client);
        }
        synchronized (agingClients) {
            agingClients.remove(client);
        }
        client.setFatalError(new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT, msg));
        client.close();
    }

    public void deleteClientFromContainer(XClient client) {
        if (XConfig.GALAXY_X_PROTOCOL) {
            // Only galaxy protocol need this to clean container to make room for new client faster.
            synchronized (clients) {
                clients.remove(client);
            }
            synchronized (agingClients) {
                agingClients.remove(client);
            }
        }
    }

    public void cleanupClient(ThreadPoolExecutor executor) {
        // Caution: Do session clean and aging clean before alive check to prevent check on dropping TCP.

        // Check session.
        final long nowNanos = System.nanoTime();
        final int cnt = idleCount.get();
        int idx = 0;
        for (XSession pooled; (pooled = idleSessions.poll()) != null; ) {
            idleCount.getAndDecrement();
            final boolean exit =
                ++idx >= cnt || nowNanos - pooled.getLastPacketNanos() < XConfig.DEFAULT_PROBE_IDLE_NANOS;
            try {
                if (pooled.reset()) {
                    // Good session and put back.
                    if (reuseSession(pooled)) {
                        pooled = null;
                    }
                }
            } catch (Throwable ignore) {
            }
            if (pooled != null) {
                XLog.XLogLogger.debug("Drop session not reusable. " + pooled);
                try {
                    pooled.getClient().dropSession(pooled);
                } catch (Throwable t) {
                    XLog.XLogLogger.error(t);
                }
            }
            if (exit) {
                break;
            }
        }

        // Do TCP aging.
        List<XClient> copy = ImmutableList.copyOf(clients);
        for (XClient client : copy) {
            if (client.isOld() && agingClients.size() <= manager.getMaxClientPerInstance()) {
                // Never double the TCP connections.
                XLog.XLogLogger.info(client + " move to aging queue.");
                synchronized (clients) {
                    clients.remove(client);
                }
                agingClients.add(client);
            }
        }

        // Do aging close.
        final List<XClient> aliveAging = new ArrayList<>();
        copy = ImmutableList.copyOf(agingClients);
        for (XClient client : copy) {
            if (0 == client.getWorkingSessionCount()) {
                // All freed. Force close.
                XLog.XLogLogger.info(client + " aging close.");
                executor.execute(() -> removeClient(client, "Client removed safe."));
            } else {
                aliveAging.add(client);
            }
        }

        // Check alive on both container.
        copy = new ArrayList<>();
        copy.addAll(clients);
        copy.addAll(aliveAging);
        for (XClient client : copy) {
            executor.execute(() -> {
                if (client.isBad() ||
                    (client.needProb() &&
                        !client.probe(manager.getIdGenerator(), XConfig.DEFAULT_PROBE_TIMEOUT_NANOS))) {
                    XLog.XLogLogger.warn("Found bad " + client + " and remove it.");
                    removeClient(client, "Client removed.");
                } else if (client.isActive()) {
                    // Good client.
                    try {
                        client.shrinkBuffer(); // Auto shrink big read buffer after twice invoke(at most 20s).
                        client.refreshVariables(manager.getIdGenerator(), XConfig.DEFAULT_PROBE_TIMEOUT_NANOS);
                    } catch (Exception e) {
                        XLog.XLogLogger.error("Error when refresh variables on " + client + ".");
                        XLog.XLogLogger.error(e); // Just log and ignore.
                    }
                }
            });
        }

        // Pre-alloc connections.
        final int aliveCnt = getAliveSessionCount();
        if (aliveCnt < XConnectionManager.getInstance().getMinPooledSessionPerInstance()) {
            // Once and half.
            final int allocCnt =
                (XConnectionManager.getInstance().getMinPooledSessionPerInstance() - aliveCnt + 1) * 2 / 3;
            XLog.XLogLogger.debug("Pre-alloc connection cnt " + allocCnt + " on " + this);
            for (int i = 0; i < allocCnt; ++i) {
                executor.execute(() -> {
                    try {
                        try (final XConnection connection = getConnection(
                            XConnectionManager.getInstance().getPacketConsumer(),
                            XConfig.PRE_ALLOC_CONNECTION_TIMEOUT_NANOS, true)) {
                            final int originalNetworkTimeout = connection.getNetworkTimeout();
                            try {
                                connection.setNetworkTimeoutNanos(XConfig.PRE_ALLOC_CONNECTION_TIMEOUT_NANOS);
                                try (final XResult result = connection.execQuery("/*X pre alloc*/ select 1")) {
                                    while (result.next() != null) {
                                        ;
                                    }
                                }
                            } finally {
                                connection.setNetworkTimeout(null, originalNetworkTimeout);
                            }
                        }
                    } catch (Exception e) {
                        XLog.XLogLogger.error(e);
                    }
                });
            }
        }

        if (XLog.XProtocolLogger.isDebugEnabled()) {
            // Debug show.
            copy = new ArrayList<>();
            copy.addAll(clients);
            copy.addAll(agingClients);
            XLog.XLogLogger.info("Now cleanup on " + this + " ref:" + refCount.get() +
                " conc: " + perfCollection.getSessionActiveCount().get());
            for (XClient client : copy) {
                client.getWorkingSessionMap().values().stream().filter(s -> !idleSessions.contains(s)).forEach(s ->
                    XLog.XLogLogger.info(
                        "Running: " + (null == s.getLastRequest() ? "null" : s.getLastRequest().getSql())));
            }
            XLog.XLogLogger.info("Total sess cnt: " + XSession.GLOBAL_COUNTER.get());
        }
    }

    public void shutdown() {
        XLog.XLogLogger.info(this + " shutdown.");
        List<XClient> copy = new ArrayList<>();
        copy.addAll(clients);
        copy.addAll(agingClients);
        for (XClient client : copy) {
            removeClient(client, "Datasource shutdown.");
        }
    }

    @Override
    public String toString() {
        return "XClientPool to " + getDnTag();
    }
}
