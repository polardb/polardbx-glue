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

package com.alibaba.polardbx.rpc.perf;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @version 1.0
 */
public class SessionPerfCollection {

    // Session level data.

    private final AtomicLong planCount = new AtomicLong(0);
    private final AtomicLong queryCount = new AtomicLong(0);
    private final AtomicLong updateCount = new AtomicLong(0);
    private final AtomicLong tsoCount = new AtomicLong(0);

    public AtomicLong getPlanCount() {
        return planCount;
    }

    public AtomicLong getQueryCount() {
        return queryCount;
    }

    public AtomicLong getUpdateCount() {
        return updateCount;
    }

    public AtomicLong getTsoCount() {
        return tsoCount;
    }

}
