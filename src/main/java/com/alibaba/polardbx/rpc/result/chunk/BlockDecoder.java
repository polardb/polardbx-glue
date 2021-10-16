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

import java.util.Set;

public interface BlockDecoder {

    int rowCount();

    int restCount();

    boolean next() throws Exception;

    Object getObject() throws Exception;

    boolean isNull() throws Exception;

    long getLong() throws Exception; // ll & ull

    float getFloat() throws Exception;

    double getDouble() throws Exception;

    long getDate() throws Exception; // Compact data of year,month,day

    long getBit() throws Exception; // ull for 64bit

    long getTime() throws Exception; // Compact data of hour,minute,second,microsecond

    long getDatetime() throws Exception; // Compact data of year,month,day,hour,minute,second,microsecond

    Decimal getDecimal() throws Exception;

    Set<Slice> getSet() throws Exception;

    Slice getString() throws Exception;
}
