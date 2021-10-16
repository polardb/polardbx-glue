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

package com.alibaba.polardbx.rpc.result.chunk;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.alibaba.polardbx.common.exception.NotSupportException;

import java.util.Set;

/**
 * @version 1.0
 */
public abstract class AbstractBlockDecoder implements BlockDecoder {

    protected final PolarxResultset.Chunk chunk;
    protected final int columnIndex;
    protected final CodedInputStream stream;
    protected final ByteString null_bitmap;

    protected final PolarxResultset.ColumnMetaData meta;

    protected int rowIndex;
    protected boolean currentNull;

    public AbstractBlockDecoder(PolarxResultset.Chunk chunk, int columnIndex,
                                PolarxResultset.ColumnMetaData meta) {
        this.chunk = chunk;
        this.columnIndex = columnIndex;
        final PolarxResultset.Column column = chunk.getColumns(columnIndex);
        this.stream = column.hasFixedSizeColumn() ? column.getFixedSizeColumn().getValue().newCodedInput() :
            column.getVariableSizeColumn().getValue().newCodedInput();
        this.null_bitmap =
            chunk.getColumns(columnIndex).hasNullBitmap() ? chunk.getColumns(columnIndex).getNullBitmap() : null;
        this.meta = meta;

        this.rowIndex = -1;
        this.currentNull = true;
    }

    @Override
    public int rowCount() {
        return chunk.getRowCount();
    }

    @Override
    public int restCount() {
        if (rowIndex < 0) {
            return rowCount();
        } else if (rowIndex >= rowCount() - 1) {
            return 0;
        } else {
            return (rowCount() - rowIndex) - 1;
        }
    }

    private boolean internalIsNull() {
        if (null == null_bitmap) {
            return false; // All block not null.
        }
        final int offset = rowIndex / Byte.SIZE;
        final int bit_offset = rowIndex % Byte.SIZE;
        return (null_bitmap.byteAt(offset) & (1 << (7 - bit_offset))) != 0;
    }

    @Override
    public boolean next() throws Exception {
        if (rowIndex < rowCount() - 1) {
            ++rowIndex;
            currentNull = internalIsNull();
            return true;
        }
        return false;
    }

    @Override
    public boolean isNull() throws Exception {
        return currentNull;
    }

    @Override
    public long getLong() throws Exception {
        throw new NotSupportException("XChunkDecoder getLong from " + this.getClass().getSimpleName());
    }

    @Override
    public float getFloat() throws Exception {
        throw new NotSupportException("XChunkDecoder getFloat from " + this.getClass().getSimpleName());
    }

    @Override
    public double getDouble() throws Exception {
        throw new NotSupportException("XChunkDecoder getDouble from " + this.getClass().getSimpleName());
    }

    @Override
    public long getDate() throws Exception {
        throw new NotSupportException("XChunkDecoder getDate from " + this.getClass().getSimpleName());
    }

    @Override
    public long getBit() throws Exception {
        throw new NotSupportException("XChunkDecoder getBit from " + this.getClass().getSimpleName());
    }

    @Override
    public long getTime() throws Exception {
        throw new NotSupportException("XChunkDecoder getTime from " + this.getClass().getSimpleName());
    }

    @Override
    public long getDatetime() throws Exception {
        throw new NotSupportException("XChunkDecoder getDatetime from " + this.getClass().getSimpleName());
    }

    @Override
    public Decimal getDecimal() throws Exception {
        throw new NotSupportException("XChunkDecoder getDecimal from " + this.getClass().getSimpleName());
    }

    @Override
    public Set<Slice> getSet() throws Exception {
        throw new NotSupportException("XChunkDecoder getSet from " + this.getClass().getSimpleName());
    }

    @Override
    public Slice getString() throws Exception {
        throw new NotSupportException("XChunkDecoder getString from " + this.getClass().getSimpleName());
    }
}
