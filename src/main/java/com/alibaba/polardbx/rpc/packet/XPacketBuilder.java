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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.google.protobuf.ByteString;
import com.googlecode.protobuf.format.JsonFormat;
import com.mysql.cj.polarx.protobuf.PolarxSql;
import com.mysql.cj.x.protobuf.Polarx;
import com.mysql.cj.x.protobuf.PolarxExecPlan;

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
    final BytesSql sql;
    final String encoding;

    final PolarxSql.GalaxyPrepareExecute.Builder galaxyPrepareBuilder;
    final ByteString extraPrefix;

    public XPacketBuilder(long sessionId, PolarxExecPlan.ExecPlan.Builder planBuilder, PolarxExecPlan.AnyPlan plan) {
        this.sessionId = sessionId;
        this.planBuilder = planBuilder;
        this.plan = plan;
        this.sqlBuilder = null;
        this.sql = null;
        this.encoding = null;
        this.galaxyPrepareBuilder = null;
        this.extraPrefix = null;
    }

    public XPacketBuilder(long sessionId, PolarxSql.StmtExecute.Builder sqlBuilder, BytesSql sql, String encoding) {
        this.sessionId = sessionId;
        this.planBuilder = null;
        this.plan = null;
        this.sqlBuilder = sqlBuilder;
        this.sql = sql;
        this.encoding = encoding;
        this.galaxyPrepareBuilder = null;
        this.extraPrefix = null;
    }

    public XPacketBuilder(long sessionId, PolarxSql.GalaxyPrepareExecute.Builder galaxyPrepareBuilder, BytesSql sql,
                          String encoding, ByteString extraPrefix) {
        this.sessionId = sessionId;
        this.planBuilder = null;
        this.plan = null;
        this.sqlBuilder = null;
        this.sql = sql;
        this.encoding = encoding;
        this.galaxyPrepareBuilder = galaxyPrepareBuilder;
        this.extraPrefix = extraPrefix;
    }

    public boolean isPlan() {
        return planBuilder != null;
    }

    public boolean isSql() {
        return sqlBuilder != null;
    }

    public boolean isGalaxyPrepare() {
        return galaxyPrepareBuilder != null;
    }

    public XPacket build() {
        if (planBuilder != null) {
            assert plan != null;
            planBuilder.setPlan(plan);
            // and audit str
            final JsonFormat format = new JsonFormat();
            planBuilder.setAuditStr(ByteString.copyFromUtf8(format.printToString(planBuilder.getPlan())));
            return new XPacket(sessionId, Polarx.ClientMessages.Type.EXEC_PLAN_READ_VALUE, planBuilder.build());
        } else if (sqlBuilder != null) {
            assert sql != null;
            assert encoding != null;
            try {
                sqlBuilder.setStmt(sql.byteString(encoding));
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CONNECTION, e.getMessage());
            }
            return new XPacket(sessionId, Polarx.ClientMessages.Type.EXEC_SQL_VALUE, sqlBuilder.build());
        } else {
            assert galaxyPrepareBuilder != null;
            assert sql != null;
            assert encoding != null;
            try {
                galaxyPrepareBuilder.setStmt(
                    null == extraPrefix ? sql.byteString(encoding) : extraPrefix.concat(sql.byteString(encoding)));
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CONNECTION, e.getMessage());
            }
            return new XPacket(sessionId, Polarx.ClientMessages.Type.GALAXY_PREPARE_EXECUTE_VALUE,
                galaxyPrepareBuilder.build());
        }
    }

}
