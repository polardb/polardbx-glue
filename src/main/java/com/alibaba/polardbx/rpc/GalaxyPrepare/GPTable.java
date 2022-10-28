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

package com.alibaba.polardbx.rpc.GalaxyPrepare;

/**
 * @version 1.0
 */
public class GPTable {
    private final int tableParamIndex; // index of parameterized sql(start from 1)
    private final String tableName;

    public GPTable(int tableParamIndex, String tableName) {
        this.tableParamIndex = tableParamIndex;
        this.tableName = tableName;
    }

    public int getTableParamIndex() {
        return tableParamIndex;
    }

    public String getTableName() {
        return tableName;
    }
}
