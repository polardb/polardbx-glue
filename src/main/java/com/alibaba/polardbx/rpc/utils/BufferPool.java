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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.ReentrantLock;

public final class BufferPool {

    private final int chunkSize;
    private final ByteBuffer[] items;
    private final ReentrantLock lock;
    private int putIndex;
    private int takeIndex;
    private volatile int count;

    public BufferPool(int bufferSize, int chunkSize) {
        this.chunkSize = chunkSize;
        int capacity = bufferSize / chunkSize;
        capacity = (bufferSize % chunkSize == 0) ? capacity : capacity + 1;
        this.items = new ByteBuffer[capacity];
        this.lock = new ReentrantLock();
        for (int i = 0; i < capacity; i++) {
            insert(ByteBuffer.allocateDirect(chunkSize).order(ByteOrder.LITTLE_ENDIAN)); // Only reuse direct buffer.
        }
    }

    public int capacity() {
        return items.length;
    }

    public int size() {
        return count;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public ByteBuffer allocate() {
        ByteBuffer node;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            node = (count == 0) ? null : extract();
        } finally {
            lock.unlock();
        }
        if (node == null) {
            node = ByteBuffer.allocate(chunkSize).order(ByteOrder.LITTLE_ENDIAN); // Allocate non-direct.
        }
        return node;
    }

    public void recycle(ByteBuffer buffer) {
        // Ignore big or non-direct.
        if (buffer == null || buffer.capacity() > chunkSize || !buffer.isDirect()) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (count != items.length) {
                buffer.clear();
                insert(buffer);
            }
        } finally {
            lock.unlock();
        }
    }

    private void insert(ByteBuffer buffer) {
        items[putIndex] = buffer;
        putIndex = inc(putIndex);
        ++count;
    }

    private ByteBuffer extract() {
        final ByteBuffer[] items = this.items;
        ByteBuffer item = items[takeIndex];
        items[takeIndex] = null;
        takeIndex = inc(takeIndex);
        --count;
        return item;
    }

    private int inc(int i) {
        return (++i == items.length) ? 0 : i;
    }

}
