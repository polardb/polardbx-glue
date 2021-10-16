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

import com.alibaba.polardbx.rpc.XConfig;
import com.mysql.cj.x.protobuf.Polarx;

/**
 * @version 1.0
 */
public class XPacket {

    public final static int HEADER_SIZE = XConfig.GALAXY_X_PROTOCOL ? 14 : 13;
    public final static int VERSION_OFFSET = 8;
    public final static byte VERSION = 0;
    public final static int LENGTH_SIZE = 4;
    public final static int LENGTH_OFFSET = XConfig.GALAXY_X_PROTOCOL ? 9 : 8;
    public final static int TYPE_OFFSET = XConfig.GALAXY_X_PROTOCOL ? 13 : 12;
    public final static int TYPE_SIZE = 1;

    public final static XPacket EOF = new XPacket(-1, Polarx.ServerMessages.Type.ERROR_VALUE,
        Polarx.Error.newBuilder().setSeverity(Polarx.Error.Severity.FATAL).setCode(-1).setSqlState("Closed.")
            .setMsg("XSession closed.").build());

    public final static XPacket KILLED = new XPacket(-1, Polarx.ServerMessages.Type.ERROR_VALUE,
        Polarx.Error.newBuilder().setSeverity(Polarx.Error.Severity.FATAL).setCode(-1).setSqlState("Killed.")
            .setMsg("XSession killed.").build());

    public final static XPacket FATAL = new XPacket(-1, Polarx.ServerMessages.Type.ERROR_VALUE,
        Polarx.Error.newBuilder().setSeverity(Polarx.Error.Severity.FATAL).setCode(-1).setSqlState("Fatal.")
            .setMsg("XSession fatal.").build());

    final private long sid;
    final private int type;
    final private Object packet;
    final private int packetSize;

    public XPacket(long sid, int type, Object packet) {
        this.sid = sid;
        this.type = type;
        this.packet = packet;
        this.packetSize = 0;
    }

    public XPacket(long sid, int type, Object packet, int packetSize) {
        this.sid = sid;
        this.type = type;
        this.packet = packet;
        this.packetSize = packetSize;
    }

    public long getSid() {
        return sid;
    }

    public int getType() {
        return type;
    }

    public Object getPacket() {
        return packet;
    }

    public int getPacketSize() {
        return packetSize;
    }
}
