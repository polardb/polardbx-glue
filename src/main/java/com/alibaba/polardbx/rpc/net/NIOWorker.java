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

package com.alibaba.polardbx.rpc.net;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.rpc.XLog;

/**
 * @version 1.0
 */
public class NIOWorker {
    private static final Logger logger = LoggerFactory.getLogger(NIOWorker.class);

    private final NIOProcessor[] processors;
    private int index = 0;

    private static int MAX_THREADS = 32;
    private static long MAX_BUF_SIZE = 256 * 1024 * 1024; // 256MB

    static {
        if (System.getenv("cpu_cores") != null) {
            try {
                MAX_THREADS = Integer.parseInt(System.getenv("cpu_cores")) * 2; // Fix this when have env.
            } catch (Throwable e) {
                XLog.XLogLogger.warn(e);
            }
        }
        try {
            final long max = Runtime.getRuntime().maxMemory();
            long sz = 1024 * 1024;
            while (sz * 20 < max) {
                sz <<= 1;
            }
            MAX_BUF_SIZE = sz; // Use at most 10% heap size of direct buffer.
        } catch (Throwable e) {
            XLog.XLogLogger.warn(e);
        }
    }

    public NIOWorker(int threadNumber) {
        if (threadNumber > MAX_THREADS) {
            threadNumber = MAX_THREADS;
        }
        int bufPerThread = (int) (MAX_BUF_SIZE / threadNumber);
        if (bufPerThread > NIOProcessor.DEFAULT_BUFFER_SIZE) {
            bufPerThread = NIOProcessor.DEFAULT_BUFFER_SIZE;
        }
        XLog.XLogLogger.info("XProtocol NIOWorker start with " + threadNumber + " threads and "
            + bufPerThread + " bytes buf per thread.");
        processors = new NIOProcessor[threadNumber];
        try {
            for (int i = 0; i < threadNumber; ++i) {
                processors[i] =
                    new NIOProcessor("X-NIO-Worker-" + i, bufPerThread, NIOProcessor.DEFAULT_BUFFER_CHUNK_SIZE);
                processors[i].startup();
            }
        } catch (Throwable e) {
            XLog.XLogLogger.error(e);
            logger.error(e); // Log it in tddl log either.
            System.exit(-1);
        }
    }

    public NIOProcessor[] getProcessors() {
        return processors;
    }

    public NIOProcessor getProcessor() {
        return processors[++index % processors.length];
    }

}
