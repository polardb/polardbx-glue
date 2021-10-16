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

package com.alibaba.polardbx.rpc.result;

import com.alibaba.polardbx.rpc.result.chunk.block.LongBlockDecoder;
import com.alibaba.polardbx.rpc.result.chunk.block.SignedLongBlockDecoder;
import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.alibaba.polardbx.rpc.result.chunk.BlockDecoder;
import com.alibaba.polardbx.rpc.result.chunk.block.BitBlockDecoder;
import com.alibaba.polardbx.rpc.result.chunk.block.DateBlockDecoder;
import com.alibaba.polardbx.rpc.result.chunk.block.DatetimeBlockDecoder;
import com.alibaba.polardbx.rpc.result.chunk.block.DecimalBlockDecoder;
import com.alibaba.polardbx.rpc.result.chunk.block.DoubleBlockDecoder;
import com.alibaba.polardbx.rpc.result.chunk.block.FloatBlockDecoder;
import com.alibaba.polardbx.rpc.result.chunk.block.SetBlockDecoder;
import com.alibaba.polardbx.rpc.result.chunk.block.StringBlockDecoder;
import com.alibaba.polardbx.rpc.result.chunk.block.TimeBlockDecoder;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 */
public class XResultObject {

    private final PolarxResultset.Chunk chunk;
    private List<PolarxResultset.Chunk> secondChunks = null; // If column more than thresh.
    private final List<ByteString> row;

    public XResultObject() {
        this.chunk = null;
        this.row = null;
    }

    public XResultObject(PolarxResultset.Chunk chunk) {
        this.chunk = chunk;
        this.row = null;
    }

    public XResultObject(List<ByteString> row) {
        this.chunk = null;
        this.row = row;
    }

    public void addSecondChunk(PolarxResultset.Chunk secondChunk) {
        if (null == secondChunks) {
            secondChunks = new ArrayList<>();
        }
        secondChunks.add(secondChunk);
    }

    public boolean isPending() {
        return null == chunk && null == row;
    }

    public PolarxResultset.Chunk getChunk() {
        return chunk;
    }

    public List<PolarxResultset.Chunk> getSecondChunks() {
        return secondChunks;
    }

    public List<ByteString> getRow() {
        return row;
    }

    public int getChunkColumnCount() {
        return (null == chunk ? 0 : chunk.getColumnsCount()) + (null == secondChunks ? 0 :
            secondChunks.stream().mapToInt(PolarxResultset.Chunk::getColumnsCount).sum());
    }

    /**
     * Decoder of chunk.
     */

    private List<PolarxResultset.ColumnMetaData> metas = null;
    private List<BlockDecoder> decoders = null;

    private BlockDecoder choseDecoder(PolarxResultset.Chunk nowChunk, int index, PolarxResultset.ColumnMetaData meta) {
        switch (meta.getType()) {
        case SINT:
            return new SignedLongBlockDecoder(nowChunk, index, meta);

        case UINT:
            return new LongBlockDecoder(nowChunk, index, meta);

        case DOUBLE:
            return new DoubleBlockDecoder(nowChunk, index, meta);

        case FLOAT:
            return new FloatBlockDecoder(nowChunk, index, meta);

        case BYTES:
        case ENUM:
            return new StringBlockDecoder(nowChunk, index, meta);

        case TIME:
            return new TimeBlockDecoder(nowChunk, index, meta);

        case DATETIME:
            switch (meta.getOriginalType()) {
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_NEWDATE:
                return new DateBlockDecoder(nowChunk, index, meta);

            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_DATETIME2:
            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_TIMESTAMP2:
                return new DatetimeBlockDecoder(nowChunk, index, meta);

            default:
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                    "Unknown DATETIME sub-type " + meta.getOriginalType().toString());
            }

        case SET:
            return new SetBlockDecoder(nowChunk, index, meta);

        case BIT:
            return new BitBlockDecoder(nowChunk, index, meta);

        case DECIMAL:
            return new DecimalBlockDecoder(nowChunk, index, meta);

        default:
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                "Unknown meta type " + meta.getType().toString());
        }
    }

    public boolean intiForChunkDecode(List<PolarxResultset.ColumnMetaData> metas) {
        if (null == chunk) {
            return false;
        }
        this.metas = metas;
        decoders = new ArrayList<>(metas.size());
        // Check column count.
        final int totalColumn = getChunkColumnCount();
        if (totalColumn != metas.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT, "Chunk column number mismatch.");
        }
        int metaIdx = 0;
        for (int i = 0; i < chunk.getColumnsCount(); ++i) {
            decoders.add(choseDecoder(chunk, i, metas.get(metaIdx++)));
        }
        if (secondChunks != null) {
            for (PolarxResultset.Chunk c : secondChunks) {
                for (int i = 0; i < c.getColumnsCount(); ++i) {
                    decoders.add(choseDecoder(c, i, metas.get(metaIdx++)));
                }
            }
        }
        return true;
    }

    public boolean next() {
        try {
            for (BlockDecoder decoder : decoders) {
                if (!decoder.next()) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT, e);
        }
    }

    public List<BlockDecoder> getDecoders() {
        return decoders;
    }
}
