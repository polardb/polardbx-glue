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

import java.sql.Time;
import java.util.TimeZone;

/**
 * @version 1.0
 */
public class TimeBlockDecoder extends LongBlockDecoder {

    private TimeZone timeZone = TimeZone.getDefault();

    public TimeBlockDecoder(PolarxResultset.Chunk chunk, int columnIndex,
                            PolarxResultset.ColumnMetaData meta) {
        super(chunk, columnIndex, meta);
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public Object getObject() throws Exception {
        return currentNull ? null : longToTime(getTime(), timeZone);
    }

    @Override
    public long getTime() throws Exception {
        assert !currentNull; // Just assert for performance.
        return super.getLong(); // Get raw long(packed date).
    }

    @Override
    public long getLong() throws Exception {
        assert !currentNull;
        return longToTime(getTime(), timeZone).getTime();
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
    public long getDatetime() throws Exception {
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

    public static String longToString(long time) {
        final boolean negative;
        if (negative = (time < 0)) {
            time = -time;
        }

        final long micros = time % (1L << 24);
        final long ymdhms = time >> 24;

        final long hms = ymdhms % (1 << 17);

        final long second = hms % (1 << 6);
        final long minute = (hms >> 6) % (1 << 6);
        final long hour = hms >> 12;

        return String.format("%s%02d:%02d:%02d.%06d",
            negative ? "-" : "", hour, minute, second, micros);
    }

    public static Time longToTime(long time, TimeZone tz) {
        final boolean negative;
        if (negative = (time < 0)) {
            time = -time;
        }

        final long micros = time % (1L << 24);
        final long ymdhms = time >> 24;

        final long hms = ymdhms % (1 << 17);

        final long second = hms % (1 << 6);
        final long minute = (hms >> 6) % (1 << 6);
        final long hour = hms >> 12;

        return new Time(-tz.getRawOffset() + (negative ? -1 : 1) * (hour * 3600_000L + minute * 60_000L + second * 1000L
            + micros / 1000L));
    }
}
