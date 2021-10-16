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

import java.sql.Date;

/**
 * @version 1.0
 */
public class DateBlockDecoder extends LongBlockDecoder {

    public DateBlockDecoder(PolarxResultset.Chunk chunk, int columnIndex,
                            PolarxResultset.ColumnMetaData meta) {
        super(chunk, columnIndex, meta);
    }

    @Override
    public Object getObject() throws Exception {
        return currentNull ? null : longToDate(getDate());
    }

    @Override
    public long getDate() throws Exception {
        assert !currentNull; // Just assert for performance.
        return super.getLong(); // Get raw long(packed date).
    }

    @Override
    public long getLong() throws Exception {
        assert !currentNull;

        long raw = super.getLong();

        final boolean negative;
        if (negative = (raw < 0)) {
            raw = -raw;
        }

        final long ymdhms = raw >> 24;

        final long ymd = ymdhms >> 17;
        final long ym = ymd >> 5;

        final long day = ymd % (1 << 5);
        final long month = ym % 13;
        final long year = ym / 13;

        return (negative ? -1 : 1) * (year * 10000 + month * 100 + day);
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
    public long getTime() throws Exception {
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

    public static String longToString(long date) {
        final boolean negative;
        if (negative = (date < 0)) {
            date = -date;
        }

        final long ymdhms = date >> 24;

        final long ymd = ymdhms >> 17;
        final long ym = ymd >> 5;

        final long day = ymd % (1 << 5);
        final long month = ym % 13;
        final long year = ym / 13;

        return String.format("%s%04d-%02d-%02d",
            negative ? "-" : "", year, month, day);
    }

    public static Date longToDate(long date) {
        final boolean negative;
        if (negative = (date < 0)) {
            date = -date;
        }

        final long ymdhms = date >> 24;

        final long ymd = ymdhms >> 17;
        final long ym = ymd >> 5;

        final long day = ymd % (1 << 5);
        final long month = ym % 13;
        final long year = ym / 13;

        return new Date((int) ((negative ? -year : year) - 1900), (int) (month - 1), (int) day);
    }
}
