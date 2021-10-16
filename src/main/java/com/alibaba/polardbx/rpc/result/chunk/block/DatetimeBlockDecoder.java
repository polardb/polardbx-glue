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

import com.alibaba.polardbx.rpc.result.chunk.Decimal;
import com.alibaba.polardbx.rpc.result.chunk.Slice;
import com.mysql.cj.polarx.protobuf.PolarxResultset;

import java.sql.Timestamp;

/**
 * @version 1.0
 */
public class DatetimeBlockDecoder extends LongBlockDecoder {

    public DatetimeBlockDecoder(PolarxResultset.Chunk chunk, int columnIndex,
                                PolarxResultset.ColumnMetaData meta) {
        super(chunk, columnIndex, meta);
    }

    @Override
    public Object getObject() throws Exception {
        return currentNull ? null : longToTimestamp(getDatetime());
    }

    @Override
    public long getDatetime() throws Exception {
        assert !currentNull; // Just assert for performance.
        return super.getLong(); // Get raw long(packed datetime).
    }

    @Override
    public long getLong() throws Exception {
        assert !currentNull;
        return longToTimestamp(getDatetime()).getTime();
    }

    @Override
    public float getFloat() throws Exception {
        assert !currentNull;
        return getLong(); // Get converted long.
    }

    @Override
    public double getDouble() throws Exception {
        assert !currentNull;
        return getLong(); // Get converted long.
    }

    @Override
    public long getDate() throws Exception {
        assert !currentNull;
        return super.getLong(); // Get raw long(packed date).
    }

    @Override
    public long getTime() throws Exception {
        assert !currentNull;
        return super.getLong(); // Get raw long(packed date).
    }

    @Override
    public Decimal getDecimal() throws Exception {
        if (currentNull) {
            return null;
        }
        return new Decimal(getLong(), 0); // Get converted long.
    }

    @Override
    public Slice getString() throws Exception {
        if (currentNull) {
            return null;
        }
        final String str = longToString(getDate());
        final byte[] bytes = str.getBytes();
        return new Slice(bytes, 0, bytes.length);
    }

    public static String longToString(long datetime) {
        final boolean negative;
        if (negative = (datetime < 0)) {
            datetime = -datetime;
        }

        final long micros = datetime % (1L << 24);
        final long ymdhms = datetime >> 24;

        final long ymd = ymdhms >> 17;
        final long ym = ymd >> 5;
        final long hms = ymdhms % (1 << 17);

        final long day = ymd % (1 << 5);
        final long month = ym % 13;
        final long year = ym / 13;

        final long second = hms % (1 << 6);
        final long minute = (hms >> 6) % (1 << 6);
        final long hour = hms >> 12;

        return String.format("%s%04d-%02d-%02d %02d:%02d:%02d.%06d",
            negative ? "-" : "", year, month, day, hour, minute, second, micros);
    }

    public static Timestamp longToTimestamp(long datetime) {
        final boolean negative;
        if (negative = (datetime < 0)) {
            datetime = -datetime;
        }

        final long micros = datetime % (1L << 24);
        final long ymdhms = datetime >> 24;

        final long ymd = ymdhms >> 17;
        final long ym = ymd >> 5;
        final long hms = ymdhms % (1 << 17);

        final long day = ymd % (1 << 5);
        final long month = ym % 13;
        final long year = ym / 13;

        final long second = hms % (1 << 6);
        final long minute = (hms >> 6) % (1 << 6);
        final long hour = hms >> 12;

        return new Timestamp((int) ((negative ? -year : year) - 1900), (int) (month - 1), (int) day, (int) hour,
            (int) minute, (int) second, (int) (micros * 1000L));
    }
}
