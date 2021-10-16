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

package com.alibaba.polardbx.rpc.packet;

import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxSql;
import com.mysql.cj.x.protobuf.Polarx;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.io.UnsupportedEncodingException;

/**
 * For rebuild XPacket and lazy build.
 *
 * @version 1.0
 */
public class XPacketBuilder {

    final long sessionId;

    final PolarxExecPlan.ExecPlan.Builder planBuilder;
    final PolarxExecPlan.AnyPlan plan;

    final PolarxSql.StmtExecute.Builder sqlBuilder;
    final String sql;
    final String encoding;

    public XPacketBuilder(long sessionId, PolarxExecPlan.ExecPlan.Builder planBuilder, PolarxExecPlan.AnyPlan plan) {
        this.sessionId = sessionId;
        this.planBuilder = planBuilder;
        this.plan = plan;
        this.sqlBuilder = null;
        this.sql = null;
        this.encoding = null;
    }

    public XPacketBuilder(long sessionId, PolarxSql.StmtExecute.Builder sqlBuilder, String sql, String encoding) {
        this.sessionId = sessionId;
        this.planBuilder = null;
        this.plan = null;
        this.sqlBuilder = sqlBuilder;
        this.sql = sql;
        this.encoding = encoding;
    }

    public boolean isPlan() {
        return planBuilder != null;
    }

    public XPacket build() {
        if (planBuilder != null) {
            assert plan != null;
            planBuilder.setPlan(plan);
            return new XPacket(sessionId, Polarx.ClientMessages.Type.EXEC_PLAN_READ_VALUE, planBuilder.build());
        } else {
            assert sqlBuilder != null;
            assert sql != null;
            assert encoding != null;
            try {
                sqlBuilder.setStmt(ByteString.copyFrom(sql, encoding));
            } catch (UnsupportedEncodingException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CONNECTION, e.getMessage());
            }
            return new XPacket(sessionId, Polarx.ClientMessages.Type.EXEC_SQL_VALUE, sqlBuilder.build());
        }
    }

}
