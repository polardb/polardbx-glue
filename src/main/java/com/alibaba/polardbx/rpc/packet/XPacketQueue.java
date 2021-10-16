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

package com.alibaba.polardbx.rpc.packet;

import com.alibaba.polardbx.rpc.utils.NotifyQueue;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @version 1.0
 */
public class XPacketQueue extends NotifyQueue<XPacket> {

    private final AtomicLong bufferSize = new AtomicLong(0);

    @Override
    public void put(XPacket xPacket) {
        bufferSize.getAndAdd(xPacket.getPacketSize());
        super.put(xPacket);
    }

    @Override
    public void put(Collection<? extends XPacket> c) {
        for (XPacket xPacket : c) {
            bufferSize.getAndAdd(xPacket.getPacketSize());
        }
        super.put(c);
    }

    @Override
    public XPacket poll(long timeout, TimeUnit unit) throws InterruptedException {
        final XPacket xPacket = super.poll(timeout, unit);
        if (xPacket != null) {
            bufferSize.getAndAdd(-xPacket.getPacketSize());
        }
        return xPacket;
    }

    public long getBufferSize() {
        return bufferSize.get();
    }

}
