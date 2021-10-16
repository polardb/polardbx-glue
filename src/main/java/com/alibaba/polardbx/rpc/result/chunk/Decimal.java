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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @version 1.0
 */
public class Decimal {

    private final BigInteger bigUnscaled;
    private final long unscaled;
    private final int scale;

    public Decimal(long unscaled, int scale) {
        this.bigUnscaled = null;
        this.unscaled = unscaled;
        this.scale = scale;
    }

    public Decimal(BigInteger bigUnscaled, int scale) {
        this.bigUnscaled = bigUnscaled;
        this.unscaled = 0;
        this.scale = scale;
    }

    public BigInteger getBigUnscaled() {
        return bigUnscaled;
    }

    public long getUnscaled() {
        return unscaled;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public String toString() {
        return (new BigDecimal(null == bigUnscaled ? new BigInteger(Long.toString(unscaled)) : bigUnscaled, scale))
            .toString();
    }
}
