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

import com.alibaba.polardbx.rpc.perf.ReactorPerfItem;
import com.alibaba.polardbx.rpc.utils.BufferPool;

import java.io.IOException;

/**
 * @version 1.0
 */
public class NIOProcessor {

    public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 16; // 16MB
    public static final int DEFAULT_BUFFER_CHUNK_SIZE = 65536; // Max packet size. Max chunk (10*4k = 40k).

    private final String name;
    private final NIOReactor reactor;
    private final BufferPool bufferPool;

    public NIOProcessor(String name) throws IOException {
        this(name,
            DEFAULT_BUFFER_SIZE,
            DEFAULT_BUFFER_CHUNK_SIZE);
    }

    public NIOProcessor(String name, int buffer, int chunk) throws IOException {
        this.name = name;
        this.reactor = new NIOReactor(name);
        this.bufferPool = new BufferPool(buffer, chunk);
    }

    public String getName() {
        return name;
    }

    public NIOReactor getReactor() {
        return reactor;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public void startup() {
        reactor.startup();
    }

    public void postRegister(NIOConnection c) {
        reactor.postRegister(c);
    }

    public void postWrite(NIOConnection c) {
        reactor.postWrite(c);
    }

    public ReactorPerfItem getPerfItem() {
        final ReactorPerfItem item = new ReactorPerfItem();

        item.setName(name);
        item.setSocketCount(reactor.getPerfCollection().getSocketCount().get());
        item.setEventLoopCount(reactor.getPerfCollection().getEventLoopCount().get());
        item.setRegisterCount(reactor.getPerfCollection().getRegisterCount().get());
        item.setReadCount(reactor.getPerfCollection().getReadCount().get());
        item.setWriteCount(reactor.getPerfCollection().getWriteCount().get());

        item.setBufferSize((long) bufferPool.getChunkSize() * bufferPool.capacity());
        item.setBufferChunkSize(bufferPool.getChunkSize());
        item.setPooledBufferCount(bufferPool.size());

        return item;
    }

}
