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
import com.alibaba.polardbx.rpc.result.chunk.Decimal;
import com.alibaba.polardbx.rpc.result.chunk.Slice;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.math.BigDecimal;

/**
 * @version 1.0
 */
public class StringBlockDecoder extends AbstractBlockDecoder {

    private Slice currentValue;

    public StringBlockDecoder(PolarxResultset.Chunk chunk, int columnIndex,
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
                final int length = stream.readInt32(); // With '\0' tail.
                if (length <= 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT, "Read chunk string more than 2G.");
                }
                currentValue = new Slice(stream.readRawBytes(length - 1), 0, length - 1);
                stream.skipRawBytes(1);
            }
            return true;
        }
        return false;
    }

    @Override
    public Object getObject() throws Exception {
        return currentNull ? null : getString();
    }

    @Override
    public Slice getString() throws Exception {
        return currentValue;
    }

    @Override
    public long getLong() throws Exception {
        // Optimize via byte operation.
        long val = 0;
        for (int i = 0; i < currentValue.getLength(); ++i) {
            final byte b = currentValue.getData()[currentValue.getOffset() + i];
            if (b >= '0' && b <= '9') {
                val = 10 * val + (b - '0');
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT, "Bad string to long.");
            }
        }
        return val;
    }

    @Override
    public float getFloat() throws Exception {
        return Float.parseFloat(currentValue.toString());
    }

    @Override
    public double getDouble() throws Exception {
        return Double.parseDouble(currentValue.toString());
    }

    @Override
    public long getDate() throws Exception {
        // TODO: Optimize this via new data type.
        return super.getDate();
    }

    @Override
    public long getBit() throws Exception {
        return Long.parseLong(currentValue.toString(), 2);
    }

    @Override
    public long getTime() throws Exception {
        // TODO: Optimize this via new data type.
        return super.getTime();
    }

    @Override
    public long getDatetime() throws Exception {
        // TODO: Optimize this via new data type.
        return super.getDatetime();
    }

    @Override
    public Decimal getDecimal() throws Exception {
        final BigDecimal bigDecimal = new BigDecimal(currentValue.toString());
        return new Decimal(bigDecimal.unscaledValue(), bigDecimal.scale());
    }
}
