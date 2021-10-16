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

import java.util.concurrent.atomic.AtomicLong;

/**
 * @version 1.0
 */
public class TcpPerfCollection {

    // TCP level data.

    // Network perf.
    private final AtomicLong sendMsgCount = new AtomicLong(0);
    private final AtomicLong sendFlushCount = new AtomicLong(0);
    private final AtomicLong sendSize = new AtomicLong(0);
    private final AtomicLong recvMsgCount = new AtomicLong(0);
    private final AtomicLong recvNetCount = new AtomicLong(0);
    private final AtomicLong recvSize = new AtomicLong(0);

    // Session perf.
    private final AtomicLong sessionCreateCount = new AtomicLong(0);
    private final AtomicLong sessionDropCount = new AtomicLong(0);
    private final AtomicLong sessionCreateSuccessCount = new AtomicLong(0);
    private final AtomicLong sessionCreateFailCount = new AtomicLong(0);
    private final AtomicLong sessionActiveCount = new AtomicLong(0);

    public AtomicLong getSendMsgCount() {
        return sendMsgCount;
    }

    public AtomicLong getSendFlushCount() {
        return sendFlushCount;
    }

    public AtomicLong getSendSize() {
        return sendSize;
    }

    public AtomicLong getRecvMsgCount() {
        return recvMsgCount;
    }

    public AtomicLong getRecvNetCount() {
        return recvNetCount;
    }

    public AtomicLong getRecvSize() {
        return recvSize;
    }

    public AtomicLong getSessionCreateCount() {
        return sessionCreateCount;
    }

    public AtomicLong getSessionDropCount() {
        return sessionDropCount;
    }

    public AtomicLong getSessionCreateSuccessCount() {
        return sessionCreateSuccessCount;
    }

    public AtomicLong getSessionCreateFailCount() {
        return sessionCreateFailCount;
    }

    public AtomicLong getSessionActiveCount() {
        return sessionActiveCount;
    }
}
