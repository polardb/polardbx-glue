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

package com.alibaba.polardbx.rpc.utils;

import com.alibaba.polardbx.rpc.XLog;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @version 1.0
 */
public class TimerThread {
    private static Thread thread;
    private static AtomicLong counter = new AtomicLong(0);

    static {
        thread = new Thread(() -> {
            while (true) {
                counter.set(System.nanoTime());
                try {
                    Thread.sleep(0, 100_000); // 0.1ms
                } catch (InterruptedException ignore) {
                } catch (Throwable t) {
                    XLog.XLogLogger.error(t);
                }
            }
        }, "XRPC timer thread.");
        thread.start();
    }

    public static long fastNanos() {
        return counter.get();
    }
}
