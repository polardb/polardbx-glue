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

import com.alibaba.polardbx.rpc.XLog;
import com.alibaba.polardbx.rpc.perf.ReactorPerfCollection;
import com.alibaba.polardbx.rpc.utils.NotifyQueue;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 */
public class NIOReactor {

    private final String name;
    private final R reactorR;
    private final W reactorW;

    private final ReactorPerfCollection perfCollection = new ReactorPerfCollection();

    public NIOReactor(String name) throws IOException {
        this.name = name;
        this.reactorR = new R();
        this.reactorW = new W();
    }

    public ReactorPerfCollection getPerfCollection() {
        return perfCollection;
    }

    final void startup() {
        new Thread(reactorR, name + "-R").start();
        new Thread(reactorW, name + "-W").start();
    }

    final void postRegister(NIOConnection c) {
        reactorR.registerQueue.offer(c);
        reactorR.selector.wakeup();
    }

    final void postWrite(NIOConnection c) {
        reactorW.writeQueue.put(c);
    }

    private final class R implements Runnable {

        private final Selector selector;
        private final ConcurrentLinkedQueue<NIOConnection> registerQueue;

        private R() throws IOException {
            this.selector = Selector.open();
            this.registerQueue = new ConcurrentLinkedQueue<>();
        }

        @Override
        public void run() {
            final Selector selector = this.selector;
            for (; ; ) {
                try {
                    selector.select(1000L);
                    register(selector);
                    Set<SelectionKey> keys = selector.selectedKeys();
                    try {
                        perfCollection.getEventLoopCount().getAndIncrement();
                        for (SelectionKey key : keys) {
                            Object att = key.attachment();
                            if (att != null && key.isValid()) {
                                int readyOps = key.readyOps();
                                if ((readyOps & SelectionKey.OP_READ) != 0) {
                                    read((NIOConnection) att);
                                } else if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                                    write((NIOConnection) att);
                                } else {
                                    key.cancel();
                                }
                            } else {
                                key.cancel();
                            }
                        }
                    } finally {
                        keys.clear();
                    }
                } catch (Throwable e) {
                    XLog.XLogLogger.warn(name, e);
                }
            }
        }

        private void register(Selector selector) {
            NIOConnection c;
            while ((c = registerQueue.poll()) != null) {
                try {
                    perfCollection.getRegisterCount().getAndIncrement();
                    c.register(selector);
                } catch (Throwable e) {
                    c.handleError(ErrorCode.ERR_REGISTER, e);
                }
            }
        }

        private void read(NIOConnection c) {
            try {
                perfCollection.getReadCount().getAndIncrement();
                c.read();
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_READ, e);
            }
        }

        private void write(NIOConnection c) {
            try {
                perfCollection.getWriteCount().getAndIncrement();
                c.writeByEvent();
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_WRITE_BY_EVENT, e);
            }
        }
    }

    private final class W implements Runnable {

        private final NotifyQueue<NIOConnection> writeQueue;

        private W() {
            this.writeQueue = new NotifyQueue<>();
        }

        @Override
        public void run() {
            for (; ; ) {
                try {
                    final NIOConnection c;
                    if ((c = writeQueue.poll(1000_000_000L, TimeUnit.NANOSECONDS)) != null) {
                        write(c);
                    }
                } catch (Throwable e) {
                    XLog.XLogLogger.warn(name, e);
                }
            }
        }

        private void write(NIOConnection c) {
            try {
                c.writeByQueue();
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_WRITE_BY_QUEUE, e);
            }
        }
    }

}
