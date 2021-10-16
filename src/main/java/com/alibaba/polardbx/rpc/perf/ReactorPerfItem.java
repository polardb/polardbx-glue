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
public class ReactorPerfItem {

    // NIO reactor level data.

    private String name;

    private long socketCount = 0;

    private long eventLoopCount = 0;
    private long registerCount = 0;
    private long readCount = 0;
    private long writeCount = 0;

    private long bufferSize = 0;
    private long bufferChunkSize = 0;
    private long pooledBufferCount = 0;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getSocketCount() {
        return socketCount;
    }

    public void setSocketCount(long socketCount) {
        this.socketCount = socketCount;
    }

    public long getEventLoopCount() {
        return eventLoopCount;
    }

    public void setEventLoopCount(long eventLoopCount) {
        this.eventLoopCount = eventLoopCount;
    }

    public long getRegisterCount() {
        return registerCount;
    }

    public void setRegisterCount(long registerCount) {
        this.registerCount = registerCount;
    }

    public long getReadCount() {
        return readCount;
    }

    public void setReadCount(long readCount) {
        this.readCount = readCount;
    }

    public long getWriteCount() {
        return writeCount;
    }

    public void setWriteCount(long writeCount) {
        this.writeCount = writeCount;
    }

    public long getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(long bufferSize) {
        this.bufferSize = bufferSize;
    }

    public long getBufferChunkSize() {
        return bufferChunkSize;
    }

    public void setBufferChunkSize(long bufferChunkSize) {
        this.bufferChunkSize = bufferChunkSize;
    }

    public long getPooledBufferCount() {
        return pooledBufferCount;
    }

    public void setPooledBufferCount(long pooledBufferCount) {
        this.pooledBufferCount = pooledBufferCount;
    }

}
