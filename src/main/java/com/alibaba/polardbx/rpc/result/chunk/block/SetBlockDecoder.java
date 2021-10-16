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

package com.alibaba.polardbx.rpc.result.chunk.block;

import com.alibaba.polardbx.rpc.result.chunk.AbstractBlockDecoder;
import com.alibaba.polardbx.rpc.result.chunk.Slice;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.util.HashSet;
import java.util.Set;

/**
 * @version 1.0
 */
public class SetBlockDecoder extends AbstractBlockDecoder {

    private Set<Slice> currentValue;

    public SetBlockDecoder(PolarxResultset.Chunk chunk, int columnIndex,
                           PolarxResultset.ColumnMetaData meta) {
        super(chunk, columnIndex, meta);
        this.currentValue = null;
    }

    @Override
    public boolean next() throws Exception {
        final boolean hasNext = super.next();
        if (hasNext) {
            if (currentNull) {
                currentValue = null;
            } else {
                // Decode via stream.
                final int totalLength = stream.readInt32();
                if (totalLength <= 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT, "Read chunk set more than 2G.");
                } else if (1 == totalLength) {
                    // Empty?
                    stream.skipRawBytes(totalLength);
                    currentValue = new HashSet<>();
                } else {
                    final int limit = stream.getTotalBytesRead() + totalLength;
                    if (limit <= stream.getTotalBytesRead()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT, "Set chunk overflow.");
                    }
                    currentValue = new HashSet<>();
                    while (stream.getTotalBytesRead() < limit) {
                        final int len = (int) stream.readInt64();
                        currentValue.add(new Slice(stream.readRawBytes(len), 0, len));
                    }
                }
            }
            return true;
        }

        currentNull = true;
        currentValue = null;
        return false;
    }

    @Override
    public Object getObject() throws Exception {
        return currentNull ? null : getSet();
    }

    @Override
    public Set<Slice> getSet() throws Exception {
        return currentValue;
    }

    @Override
    public Slice getString() throws Exception {
        if (currentNull) {
            return null;
        }
        final StringBuilder builder = new StringBuilder();
        for (Slice slice : currentValue) {
            if (builder.length() > 0) {
                builder.append(',');
            }
            builder.append(slice.toString());
        }
        final String str = builder.toString();
        final byte[] bytes = str.getBytes();
        return new Slice(bytes, 0, bytes.length);
    }
}
