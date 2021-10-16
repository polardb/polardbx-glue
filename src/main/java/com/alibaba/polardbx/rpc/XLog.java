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

package com.alibaba.polardbx.rpc;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.text.MessageFormat;

/**
 * @version 1.0
 */
public class XLog {

    // Protocol log.
    public final static Logger XProtocolLogger = LoggerFactory.getLogger("XProtocol");

    // Physical slow log.
    public final static Logger XRequestLogger = LoggerFactory.getLogger("XRequest");
    // sql # dsName # cache # retrans # chunk? # startTime # responseTime # finishTime # fetchCount # tokenDoneCount # activeOfferTokenCount # traceid # sql/plan # sessionId # extra
    public final static MessageFormat XRequestFormat =
        new MessageFormat("{0}#{1}#{2}#{3}#{4}#{5}#{6}#{7}#{8}#{9}#{10}#{11}#{12}#{13}#{14}");

    // Put protocol runtime log to single log file.
    public static final Logger XLogLogger = LoggerFactory.getLogger("XLog");

    // Perf log to single log file.
    public static final Logger XPerfLogger = LoggerFactory.getLogger("XPerf");

}
