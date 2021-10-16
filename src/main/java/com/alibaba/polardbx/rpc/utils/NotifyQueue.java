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

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @version 1.0
 */
public class NotifyQueue<E> {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Queue<E> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger count = new AtomicInteger(0);

    private void signal() {
        lock.lock();
        try {
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    public void put(E e) {
        queue.offer(e);
        if (0 == count.getAndIncrement()) {
            signal();
        }
    }

    public void put(Collection<? extends E> c) {
        for (E e : c) {
            queue.offer(e);
        }
        if (0 == count.getAndAdd(c.size())) {
            signal();
        }
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E e = queue.poll();
        if (e != null) {
            count.getAndDecrement();
        } else if (timeout > 0) {
            // Only wait if needed.
            long nanos = unit.toNanos(timeout);
            lock.lockInterruptibly();
            try {
                while (null == (e = queue.poll())) {
                    if (nanos <= 0) {
                        return null;
                    }
                    if (0 == count.get()) {
                        // Only wait when count is zero to prevent dead wait.
                        nanos = notEmpty.awaitNanos(nanos);
                    }
                }
                final long c = count.getAndDecrement();
                if (c > 1) {
                    notEmpty.signal();
                }
            } finally {
                lock.unlock();
            }
        }
        return e;
    }

    public int count() {
        return count.get();
    }

}
