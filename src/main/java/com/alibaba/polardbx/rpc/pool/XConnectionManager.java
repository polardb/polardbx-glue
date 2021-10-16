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

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.XLog;
import com.alibaba.polardbx.rpc.client.XClient;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.net.NIOProcessor;
import com.alibaba.polardbx.rpc.net.NIOWorker;
import com.alibaba.polardbx.rpc.packet.XPacket;
import com.alibaba.polardbx.rpc.perf.DnPerfItem;
import com.alibaba.polardbx.rpc.perf.ReactorPerfItem;
import com.alibaba.polardbx.rpc.perf.SessionPerfItem;
import com.alibaba.polardbx.rpc.perf.TcpPerfItem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

/**
 * @version 1.0
 */
public class XConnectionManager {

    private final NIOWorker eventWorker = new NIOWorker(ThreadCpuStatUtil.NUM_CORES * XConfig.NIO_THREAD_MULTIPLY);
    private final Map<String, XClientPool> instancePool = new ConcurrentHashMap<>();

    // < 0 to disable private protocol, = 0 dynamic port, > 0 static port.
    // Set by server.properties.
    private int metaDbPort = -1;
    private int storageDbPort = -1;

    private volatile int maxClientPerInstance;
    private volatile int maxSessionPerClient;
    private volatile int maxPooledSessionPerInstance;

    private volatile long sessionAgingNanos = XConfig.SESSION_AGING_NANOS;
    private volatile long slowThresholdNanos = XConfig.SLOW_THRESHOLD_NANOS;
    private volatile int minPooledSessionPerInstance = XConfig.MIN_POOLED_SESSION_PER_INSTANCE;

    private volatile boolean enableAuth = true;
    private volatile boolean enableAutoCommitOptimize = true;
    private volatile boolean enableXplan = true;
    private volatile boolean enableXplanExpendStar = true;
    private volatile boolean enableXplanTableScan = false;
    private volatile boolean enableTrxLeakCheck = false;
    private volatile boolean enableMessageTimestamp = true;
    private volatile boolean enablePlanCache = true;
    private volatile boolean enableChunkResult = true;
    private volatile boolean enablePureAsyncMpp = true;
    private volatile boolean enableDirectWrite = false;
    private volatile boolean enableFeedback = true;

    private volatile boolean enableChecker = true;
    private volatile long maxPacketSize = 64 * 1024 * 1024L; // 64MB

    private volatile int defaultQueryToken = XConfig.DEFAULT_QUERY_TOKEN;
    private volatile long defaultPipeBufferSize = XConfig.DEFAULT_PIPE_BUFFER_SIZE;

    private final ScheduledExecutorService service = ExecutorUtil.createScheduler(1,
        new NamedThreadFactory("XConnection-Check-Scheduler"),
        new ThreadPoolExecutor.CallerRunsPolicy());

    private final AtomicLong idGenerator = new AtomicLong(1); // id 0 is reserved for auth.

    private XConnectionManager(int maxClientPerInstance, int maxSessionPerClient, int maxPooledSessionPerInstance) {
        this.maxClientPerInstance = maxClientPerInstance;
        this.maxSessionPerClient = maxSessionPerClient;
        this.maxPooledSessionPerInstance = maxPooledSessionPerInstance;

        // Check connection pool and remove inactive connection.
        final long intervalNanos = XConfig.DEFAULT_PROBE_IDLE_NANOS;
        this.service.scheduleAtFixedRate(() -> {
            if (!enableChecker) {
                return; // Do not check if not allowed.
            }

            // Do perf record.
            try {
                if (XLog.XPerfLogger.isInfoEnabled()) {
                    final List<DnPerfItem> dnPerfItems = new ArrayList<>();
                    XConnectionManager.getInstance().gatherDnPerf(dnPerfItems);
                    XLog.XPerfLogger.info(JSONObject.toJSONString(dnPerfItems));
                }
            } catch (Throwable t) {
                XLog.XLogLogger.error(t);
            }

            ThreadPoolExecutor threadPoolExecutor = null;
            try {
                threadPoolExecutor = new ThreadPoolExecutor(XConfig.DEFAULT_CONNECTION_CHECKER_WORKER_NUMBER,
                    XConfig.DEFAULT_CONNECTION_CHECKER_WORKER_NUMBER,
                    0,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(),
                    new NamedThreadFactory("XConnection-Check-Workers", true));

                try {
                    for (XClientPool pool : instancePool.values()) {
                        if (pool.getRefCount().get() > 0) {
                            pool.cleanupClient(threadPoolExecutor);
                        } else {
                            XLog.XLogLogger.warn("Found 0 reference pool " + pool + " may leak or initializing.");
                        }
                    }
                } catch (Exception e) {
                    XLog.XLogLogger.error(e);
                }

                // Wait all worker done.
                threadPoolExecutor.shutdown();
                threadPoolExecutor
                    .awaitTermination(XConfig.DEFAULT_CONNECTION_CHECKER_TIMEOUT_NANOS, TimeUnit.NANOSECONDS);
            } catch (Exception e) {
                XLog.XLogLogger.error(e);
            } finally {
                if (threadPoolExecutor != null) {
                    threadPoolExecutor.shutdownNow();
                }
            }
        }, intervalNanos, intervalNanos, TimeUnit.NANOSECONDS);
    }

    private final static XConnectionManager INSTANCE =
        new XConnectionManager(XConfig.MAX_CLIENT_PER_INSTANCE, XConfig.MAX_SESSION_PER_CLIENT,
            XConfig.MAX_POOLED_SESSION_PER_INSTANCE);

    public static XConnectionManager getInstance() {
        return INSTANCE;
    }

    public NIOWorker getEventWorker() {
        return eventWorker;
    }

    public int getMetaDbPort() {
        return metaDbPort;
    }

    public void setMetaDbPort(int metaDbPort) {
        this.metaDbPort = metaDbPort;
    }

    public int getStorageDbPort() {
        return storageDbPort;
    }

    public void setStorageDbPort(int storageDbPort) {
        this.storageDbPort = storageDbPort;
    }

    public int getMaxClientPerInstance() {
        return maxClientPerInstance;
    }

    public void setMaxClientPerInstance(int maxClientPerInstance) {
        if (XConfig.GALAXY_X_PROTOCOL) {
            this.maxClientPerInstance = maxClientPerInstance * 8;
            // Galaxy protocol allow only one session per connection.
        } else {
            this.maxClientPerInstance = maxClientPerInstance;
        }
    }

    public int getMaxSessionPerClient() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return 1;
        }
        return maxSessionPerClient;
    }

    public void setMaxSessionPerClient(int maxSessionPerClient) {
        this.maxSessionPerClient = maxSessionPerClient;
    }

    public int getMaxPooledSessionPerInstance() {
        return maxPooledSessionPerInstance;
    }

    public void setMaxPooledSessionPerInstance(int maxPooledSessionPerInstance) {
        this.maxPooledSessionPerInstance = maxPooledSessionPerInstance;
    }

    public long getSessionAgingNanos() {
        return sessionAgingNanos;
    }

    public void setSessionAgingNanos(long sessionAgingNanos) {
        this.sessionAgingNanos = sessionAgingNanos;
    }

    public void setSessionAgingTimeMillis(long millis) {
        this.sessionAgingNanos = millis * 1000000L;
    }

    public long getSlowThresholdNanos() {
        return slowThresholdNanos;
    }

    public void setSlowThresholdNanos(long slowThresholdNanos) {
        this.slowThresholdNanos = slowThresholdNanos;
    }

    public void setSlowThresholdMillis(long millis) {
        this.slowThresholdNanos = millis * 1000000L;
    }

    public int getMinPooledSessionPerInstance() {
        return minPooledSessionPerInstance;
    }

    public void setMinPooledSessionPerInstance(int minPooledSessionPerInstance) {
        this.minPooledSessionPerInstance = minPooledSessionPerInstance;
    }

    public boolean isEnableAuth() {
        return enableAuth;
    }

    public void setEnableAuth(boolean enableAuth) {
        this.enableAuth = enableAuth;
    }

    public boolean isEnableAutoCommitOptimize() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }
        return enableAutoCommitOptimize;
    }

    public void setEnableAutoCommitOptimize(boolean enableAutoCommitOptimize) {
        this.enableAutoCommitOptimize = enableAutoCommitOptimize;
    }

    public boolean isEnableXplan() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }
        return enableXplan;
    }

    public void setEnableXplan(boolean enableXplan) {
        this.enableXplan = enableXplan;
    }

    public boolean isEnableXplanExpendStar() {
        return enableXplanExpendStar;
    }

    public void setEnableXplanExpendStar(boolean enableXplanExpendStar) {
        this.enableXplanExpendStar = enableXplanExpendStar;
    }

    public boolean isEnableXplanTableScan() {
        return enableXplanTableScan;
    }

    public void setEnableXplanTableScan(boolean enableXplanTableScan) {
        this.enableXplanTableScan = enableXplanTableScan;
    }

    public boolean isEnableTrxLeakCheck() {
        return enableTrxLeakCheck;
    }

    public void setEnableTrxLeakCheck(boolean enableTrxLeakCheck) {
        this.enableTrxLeakCheck = enableTrxLeakCheck;
    }

    public boolean isEnableMessageTimestamp() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }
        return enableMessageTimestamp;
    }

    public void setEnableMessageTimestamp(boolean enableMessageTimestamp) {
        this.enableMessageTimestamp = enableMessageTimestamp;
    }

    public boolean isEnablePlanCache() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }
        return enablePlanCache;
    }

    public void setEnablePlanCache(boolean enablePlanCache) {
        this.enablePlanCache = enablePlanCache;
    }

    public boolean isEnableChunkResult() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }
        return enableChunkResult;
    }

    public void setEnableChunkResult(boolean enableChunkResult) {
        this.enableChunkResult = enableChunkResult;
    }

    public boolean isEnablePureAsyncMpp() {
        return enablePureAsyncMpp;
    }

    public void setEnablePureAsyncMpp(boolean enablePureAsyncMpp) {
        this.enablePureAsyncMpp = enablePureAsyncMpp;
    }

    public boolean isEnableDirectWrite() {
        return enableDirectWrite;
    }

    public void setEnableDirectWrite(boolean enableDirectWrite) {
        this.enableDirectWrite = enableDirectWrite;
    }

    public boolean isEnableFeedback() {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }
        return enableFeedback;
    }

    public void setEnableFeedback(boolean enableFeedback) {
        this.enableFeedback = enableFeedback;
    }

    public boolean isEnableChecker() {
        return enableChecker;
    }

    public void setEnableChecker(boolean enableChecker) {
        this.enableChecker = enableChecker;
    }

    public long getMaxPacketSize() {
        return maxPacketSize;
    }

    public void setMaxPacketSize(long maxPacketSize) {
        this.maxPacketSize = maxPacketSize;
    }

    public int getDefaultQueryToken() {
        return defaultQueryToken;
    }

    public void setDefaultQueryToken(int defaultQueryToken) {
        this.defaultQueryToken = defaultQueryToken;
    }

    public long getDefaultPipeBufferSize() {
        return defaultPipeBufferSize;
    }

    public void setDefaultPipeBufferSize(long defaultPipeBufferSize) {
        this.defaultPipeBufferSize = defaultPipeBufferSize;
    }

    public AtomicLong getIdGenerator() {
        return idGenerator;
    }

    public void initializeDataSource(String host, int port, String username, String password) {
        synchronized (instancePool) {
            final XClientPool clientPool =
                instancePool
                    .computeIfAbsent(digest(host, port, username),
                        key -> new XClientPool(this, host, port, username, password));
            final int cnt = clientPool.getRefCount().getAndIncrement();
            XLog.XLogLogger.info("XConnectionManager new datasource to "
                + username + "@" + host + ":" + port + " id is " + cnt
                + " NOW_GLOBAL_SESSION: " + XSession.GLOBAL_COUNTER.get());
        }
    }

    public void deinitializeDataSource(String host, int port, String username) {
        final String digest = digest(host, port, username);
        XClientPool clientPool;
        synchronized (instancePool) {
            clientPool = instancePool.get(digest);
            if (clientPool != null) {
                final int cnt = clientPool.getRefCount().decrementAndGet();
                if (0 == cnt) {
                    instancePool.remove(digest);
                } else {
                    clientPool = null; // Don't remove it.
                }
                XLog.XLogLogger.info("XConnectionManager datasource deinit "
                    + username + "@" + host + ":" + port + " rest count is " + cnt
                    + " NOW_GLOBAL_SESSION: " + XSession.GLOBAL_COUNTER.get());
            }
        }
        if (clientPool != null) {
            clientPool.shutdown();
        }
    }

    public BiFunction<XClient, XPacket, Boolean> getPacketConsumer() {
        return (cli, pkt) -> {
            // TODO: Global dealing for any server notify.
//            try {
//                if (pkt.getType() == Polarx.ServerMessages.Type.NOTICE_VALUE) {
//                    // Notice.
//                    PolarxNotice.Frame frame = (PolarxNotice.Frame) pkt.getPacket();
//                    switch (frame.getType()) {
//                    case 1:
//                        final PolarxNotice.Warning warning = PolarxNotice.Warning.parseFrom(frame.getPayload());
//                        break;
//
//                    case 2:
//                        final PolarxNotice.SessionVariableChanged sessionVariableChanged =
//                            PolarxNotice.SessionVariableChanged.parseFrom(frame.getPayload());
//                        break;
//
//                    case 3:
//                        final PolarxNotice.SessionStateChanged sessionStateChanged =
//                            PolarxNotice.SessionStateChanged.parseFrom(frame.getPayload());
//                        break;
//                    }
//                }
//            } catch (Exception e) {
//                logger.error(e);
//            }
            return true;
        };
    }

    public XClientPool getClientPool(String host, int port, String username) {
        return instancePool.get(digest(host, port, username));
    }

    public XConnection getConnection(String host, int port, String username, String defaultDB, long timeoutNanos)
        throws Exception {
        final XClientPool clientPool = instancePool.get(digest(host, port, username));
        if (null == clientPool) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CLIENT, "Client pool not initialized.");
        }
        final XConnection connection = clientPool.getConnection(getPacketConsumer(), timeoutNanos);
        try {
            // Reset DB before use it.
            connection.setDefaultDB(defaultDB);
        } catch (Exception e) {
            connection.close();
            XLog.XLogLogger.error(e);
            throw e;
        }
        return connection;
    }

    public static String digest(String host, int port, String username) {
        return username + "@" + host + ":" + port;
    }

    public void gatherReactorPerf(List<ReactorPerfItem> list) {
        for (NIOProcessor processor : eventWorker.getProcessors()) {
            list.add(processor.getPerfItem());
        }
    }

    public void gatherDnPerf(List<DnPerfItem> list) {
        final XClientPool[] instances = instancePool.values().toArray(new XClientPool[0]);
        for (XClientPool pool : instances) {
            list.add(pool.getPerfItem());
        }
    }

    public void gatherTcpPerf(List<TcpPerfItem> list) {
        final XClientPool[] instances = instancePool.values().toArray(new XClientPool[0]);
        for (XClientPool pool : instances) {
            pool.gatherTcpPerf(list);
        }
    }

    public void gatherSessionPerf(List<SessionPerfItem> list) {
        final XClientPool[] instances = instancePool.values().toArray(new XClientPool[0]);
        for (XClientPool pool : instances) {
            pool.gatherSessionPerf(list);
        }
    }

}
