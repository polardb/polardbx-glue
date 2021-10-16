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

/**
 * @version 1.0
 */
public class XConfig {

    // TODO: Set true if local debug XDB with this feature.
    public final static boolean X_LOCAL_ENABLE_MESSAGE_TIMESTAMP = true;
    // TODO: Set true if local debug XDB with this feature.
    public final static boolean X_LOCAL_ENABLE_PLAN_CACHE = true;
    // TODO: Set true if local debug XDB with this feature.
    public final static boolean X_LOCAL_ENABLE_CHUNK_RESULT = true;
    // TODO: Set true if local debug XDB with this feature.
    public final static boolean X_LOCAL_ENABLE_FEEDBACK = true;

    public final static String X_USER = "mysqlxsys";
    public final static String X_TYPE = "xdb";

    // Most parameters here can be reset via connection properties.

    public final static long MAX_QUEUED_BATCH_REQUEST = 64;

    public final static long DEFAULT_PROBE_TIMEOUT_NANOS = 5000 * 1000000L; // 5s
    public final static long DEFAULT_GET_CONN_TIMEOUT_NANOS = 5000 * 1000000L; // 5s
    public final static long DEFAULT_TIMEOUT_NANOS = 900 * 1000 * 1000000L; // 900s
    public final static long DEFAULT_TOTAL_TIMEOUT_NANOS = 3600 * 1000 * 1000000L; // 3600s = 1h

    // Prob all tcp connections which is inactive over 10s.
    public final static long DEFAULT_PROBE_IDLE_NANOS = 10 * 1000 * 1000000L;

    public final static int DEFAULT_CONNECTION_CHECKER_WORKER_NUMBER = 32;
    public final static long DEFAULT_CONNECTION_CHECKER_TIMEOUT_NANOS = 5 * 60 * 1000 * 1000000L; // 5 minutes

    public final static int NIO_THREAD_MULTIPLY = 2;

    // Try use idle client or new a client, then try reuse client.
    public final static boolean CLIENT_FIRST_STRATEGY = true;
    public final static int MAX_CLIENT_PER_INSTANCE = 32;
    public final static int MAX_SESSION_PER_CLIENT = 1024;
    // Pool will balance the session when equal to client number.
    public final static int MAX_POOLED_SESSION_PER_INSTANCE = 512;
    // For session aging(Session dropped after this time).
    public final static long SESSION_AGING_NANOS = 10 * 60 * 1000 * 1000000L; // 10 minutes
    public final static long SLOW_THRESHOLD_NANOS = 500 * 1000000L; // 500ms
    public final static int MIN_POOLED_SESSION_PER_INSTANCE = 128;
    public final static long PRE_ALLOC_CONNECTION_TIMEOUT_NANOS = 2000 * 1000000L; // 2s
    public final static long MIN_INIT_TIMEOUT_NANOS = 1000 * 1000000L; // 1s

    public final static int DEFAULT_QUERY_TOKEN = 10000;
    public final static long DEFAULT_PIPE_BUFFER_SIZE = 256 * 1024 * 1024; // 256MB

    public final static boolean VIP_WITH_X_PROTOCOL = false;

    public static boolean GALAXY_X_PROTOCOL = false;
}
