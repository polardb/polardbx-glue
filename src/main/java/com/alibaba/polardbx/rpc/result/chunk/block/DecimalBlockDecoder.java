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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.rpc.result.chunk.AbstractBlockDecoder;
import com.alibaba.polardbx.rpc.result.chunk.Decimal;
import com.alibaba.polardbx.rpc.result.chunk.Slice;
import com.google.protobuf.CodedInputStream;
import com.mysql.cj.polarx.protobuf.PolarxResultset;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;

/**
 * @version 1.0
 */
public class DecimalBlockDecoder extends AbstractBlockDecoder {
    private static long LONG_OVERFLOW_M10D10 = (Long.MAX_VALUE - 0xFL) / 10L;

    private Decimal currentValue;

    public DecimalBlockDecoder(PolarxResultset.Chunk chunk, int columnIndex,
                               PolarxResultset.ColumnMetaData meta) {
        super(chunk, columnIndex, meta);
        this.currentValue = null;
    }

    public static Decimal decode(final CodedInputStream stream, int length) throws Exception {
        final byte scale = stream.readRawByte();
        final byte[] buf = stream.readRawBytes(length - 1);
        // For fast parse, we assume that unscaled long not overflow.
        long unscaled = 0;
        boolean overflow = false;
        byte sign = 0;
        int idx = 0;
        // read until we encounter the sign bit
        while (idx < buf.length) {
            int b = 0xFF & buf[idx];
            ++idx;
            if ((b >> 4) > 9) {
                sign = (byte) (b >> 4);
                break;
            }
            if (unscaled >= LONG_OVERFLOW_M10D10) {
                overflow = true;
                break;
            }
            unscaled = 10 * unscaled + (b >> 4);
            if ((b & 0x0f) > 9) {
                sign = (byte) (b & 0x0f);
                break;
            }
            if (unscaled >= LONG_OVERFLOW_M10D10) {
                overflow = true;
                break;
            }
            unscaled = 10 * unscaled + (b & 0x0f);
        }
        if (!overflow) {
            // Go with simple decimal.
            if (idx != buf.length) {
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                    "Did not read all bytes while decoding decimal. Bytes left: " + (buf.length - idx));
            }
            switch (sign) {
            case 0xb:
            case 0xd:
                unscaled = -unscaled; // This never overflow.
                break;
            }
            return new Decimal(unscaled, scale);
        }
        // Back to original BigInteger.
        // we allocate an extra char for the sign
        final CharBuffer unscaledString = CharBuffer.allocate(2 * buf.length);
        unscaledString.position(1);
        sign = 0;
        idx = 0;
        // read until we encounter the sign bit
        while (idx < buf.length) {
            int b = 0xFF & buf[idx];
            ++idx;
            if ((b >> 4) > 9) {
                sign = (byte) (b >> 4);
                break;
            }
            unscaledString.append((char) ((b >> 4) + '0'));
            if ((b & 0x0f) > 9) {
                sign = (byte) (b & 0x0f);
                break;
            }
            unscaledString.append((char) ((b & 0x0f) + '0'));
        }
        if (idx != buf.length) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                "Did not read all bytes while decoding decimal. Bytes left: " + (buf.length - idx));
        }
        switch (sign) {
        case 0xa:
        case 0xc:
        case 0xe:
        case 0xf:
            unscaledString.put(0, '+');
            break;
        case 0xb:
        case 0xd:
            unscaledString.put(0, '-');
            break;
        }
        // may have filled the CharBuffer or one remaining. need to remove it before toString()
        int characters = unscaledString.position();
        unscaledString.clear(); // reset position
        BigInteger bigUnscaled = new BigInteger(unscaledString.subSequence(0, characters).toString());
        return new Decimal(bigUnscaled, scale);
    }

    @Override
    public boolean next() throws Exception {
        final boolean hasNext = super.next();
        if (hasNext) {
            if (currentNull) {
                currentValue = null;
            } else {
                // Decode via stream.
                final int rowLen = stream.readInt32();
                assert rowLen > 1;
                currentValue = decode(stream, rowLen);
            }
            return true;
        }
        return false;
    }

    @Override
    public Object getObject() throws Exception {
        return currentNull ? null : getDecimal();
    }

    @Override
    public Decimal getDecimal() throws Exception {
        return currentValue;
    }

    @Override
    public long getLong() throws Exception {
        assert !currentNull;
        if (null == currentValue.getBigUnscaled()) {
            if (0 == currentValue.getScale()) {
                return currentValue.getUnscaled();
            } else {
                return currentValue.getUnscaled() / (long) Math.pow(10, currentValue.getUnscaled());
            }
        } else {
            if (0 == currentValue.getScale()) {
                return currentValue.getBigUnscaled().longValue();
            } else {
                final StringBuilder builder = new StringBuilder("1");
                for (int i = 0; i < currentValue.getScale(); ++i) {
                    builder.append('0');
                }
                return currentValue.getBigUnscaled().divide(new BigInteger(builder.toString())).longValue();
            }
        }
    }

    @Override
    public float getFloat() throws Exception {
        assert !currentNull;
        if (null == currentValue.getBigUnscaled()) {
            if (0 == currentValue.getScale()) {
                return currentValue.getUnscaled();
            } else {
                return currentValue.getUnscaled() / (float) Math.pow(10, currentValue.getUnscaled());
            }
        } else {
            if (0 == currentValue.getScale()) {
                return currentValue.getBigUnscaled().floatValue();
            } else {
                return new BigDecimal(currentValue.getBigUnscaled(), currentValue.getScale()).floatValue();
            }
        }
    }

    @Override
    public double getDouble() throws Exception {
        assert !currentNull;
        if (null == currentValue.getBigUnscaled()) {
            if (0 == currentValue.getScale()) {
                return currentValue.getUnscaled();
            } else {
                return currentValue.getUnscaled() / Math.pow(10, currentValue.getUnscaled());
            }
        } else {
            if (0 == currentValue.getScale()) {
                return currentValue.getBigUnscaled().doubleValue();
            } else {
                return new BigDecimal(currentValue.getBigUnscaled(), currentValue.getScale()).doubleValue();
            }
        }
    }

    @Override
    public Slice getString() throws Exception {
        if (currentNull) {
            return null;
        }
        // TODO: may optimize by bytes.
        final String str;
        if (null == currentValue.getBigUnscaled()) {
            if (0 == currentValue.getScale()) {
                str = Long.toString(currentValue.getUnscaled());
            } else {
                str =
                    new BigDecimal(BigInteger.valueOf(currentValue.getUnscaled()), currentValue.getScale()).toString();
            }
        } else {
            if (0 == currentValue.getScale()) {
                str = currentValue.getBigUnscaled().toString();
            } else {
                str = new BigDecimal(currentValue.getBigUnscaled(), currentValue.getScale()).toString();
            }
        }
        final byte[] bytes = str.getBytes();
        return new Slice(bytes, 0, bytes.length);
    }
}
