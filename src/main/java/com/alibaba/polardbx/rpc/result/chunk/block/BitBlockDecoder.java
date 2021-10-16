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

import com.alibaba.polardbx.rpc.result.chunk.Slice;
import com.mysql.cj.polarx.protobuf.PolarxResultset;

/**
 * @version 1.0
 */
public class BitBlockDecoder extends LongBlockDecoder {

    public BitBlockDecoder(PolarxResultset.Chunk chunk, int columnIndex,
                           PolarxResultset.ColumnMetaData meta) {
        super(chunk, columnIndex, meta);
    }

    @Override
    public Object getObject() throws Exception {
        return currentNull ? null : getBit();
    }

    @Override
    public long getBit() throws Exception {
        assert !currentNull; // Just assert for performance.
        return getLong();
    }

    @Override
    public Slice getString() throws Exception {
        if (currentNull) {
            return null;
        }
        final String str = Long.toBinaryString(getLong());
        final byte[] bytes = str.getBytes();
        return new Slice(bytes, 0, bytes.length);
    }
}
