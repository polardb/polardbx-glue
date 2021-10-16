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

import com.alibaba.polardbx.rpc.jdbc.CharsetMapping;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.sql.Types;

/**
 * @version 1.0
 */
public class XMetaUtil {

    private int jdbcType;
    private String jdbcTypeString;
    private long fieldLength;

    public XMetaUtil(PolarxResultset.ColumnMetaData meta) {
        switch (meta.getOriginalType()) {
        case MYSQL_TYPE_BIT:
            jdbcType = Types.BIT;
            jdbcTypeString = "BIT";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_TINY:
            jdbcType = 1 == meta.getLength() ? Types.BIT : Types.TINYINT;
            jdbcTypeString =
                PolarxResultset.ColumnMetaData.FieldType.UINT == meta.getType() ? "TINYINT UNSIGNED" : "TINYINT";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_SHORT:
            jdbcType = Types.SMALLINT;
            jdbcTypeString =
                PolarxResultset.ColumnMetaData.FieldType.UINT == meta.getType() ? "SMALLINT UNSIGNED" : "SMALLINT";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_INT24:
            jdbcType = Types.INTEGER;
            jdbcTypeString =
                PolarxResultset.ColumnMetaData.FieldType.UINT == meta.getType() ? "MEDIUMINT UNSIGNED" : "MEDIUMINT";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_LONG:
            jdbcType = Types.INTEGER;
            jdbcTypeString = PolarxResultset.ColumnMetaData.FieldType.UINT == meta.getType() ? "INT UNSIGNED" : "INT";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_LONGLONG:
            jdbcType = Types.BIGINT;
            jdbcTypeString =
                PolarxResultset.ColumnMetaData.FieldType.UINT == meta.getType() ? "BIGINT UNSIGNED" : "BIGINT";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
            jdbcType = Types.DECIMAL;
            jdbcTypeString = "DECIMAL";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_FLOAT:
            jdbcType = Types.REAL;
            jdbcTypeString =
                (meta.getFlags() & XResultUtil.COLUMN_FLAGS_FLOAT_UNSIGNED) != 0 ? "FLOAT UNSIGNED" : "FLOAT";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_DOUBLE:
            jdbcType = Types.DOUBLE;
            jdbcTypeString =
                (meta.getFlags() & XResultUtil.COLUMN_FLAGS_DOUBLE_UNSIGNED) != 0 ? "DOUBLE UNSIGNED" : "DOUBLE";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
            jdbcType = Types.DATE;
            jdbcTypeString = "DATE";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
            jdbcType = Types.TIMESTAMP;
            jdbcTypeString = "DATETIME";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
            jdbcType = Types.TIMESTAMP;
            jdbcTypeString = "TIMESTAMP";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2:
            jdbcType = Types.TIME;
            jdbcTypeString = "TIME";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_YEAR:
            jdbcType = Types.DATE;
            jdbcTypeString = "YEAR";
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VAR_STRING:
            if (CharsetMapping.MYSQL_COLLATION_INDEX_binary == meta.getCollation()) {
                jdbcType = Types.BINARY;
                jdbcTypeString = "BINARY";
            } else {
                jdbcType = Types.CHAR;
                jdbcTypeString = "CHAR";
            }
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_VARCHAR:
            if (CharsetMapping.MYSQL_COLLATION_INDEX_binary == meta.getCollation()) {
                jdbcType = Types.VARBINARY;
                jdbcTypeString = "VARBINARY";
            } else {
                jdbcType = Types.VARCHAR;
                jdbcTypeString = "VARCHAR";
            }
            fieldLength = meta.getLength();
            break;

        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
            switch (meta.getLength()) {
            case 255:
                if (CharsetMapping.MYSQL_COLLATION_INDEX_binary == meta.getCollation()) {
                    jdbcType = Types.VARBINARY;
                    jdbcTypeString = "TINYBLOB";
                } else {
                    jdbcType = Types.LONGVARCHAR;
                    jdbcTypeString = "VARCHAR";
                }
                break;

            case 65535:
                if (CharsetMapping.MYSQL_COLLATION_INDEX_binary == meta.getCollation()) {
                    jdbcType = Types.LONGVARBINARY;
                    jdbcTypeString = "BLOB";
                } else {
                    jdbcType = Types.LONGVARCHAR;
                    jdbcTypeString = "VARCHAR";
                }
                break;

            case 16777215:
                if (CharsetMapping.MYSQL_COLLATION_INDEX_binary == meta.getCollation()) {
                    jdbcType = Types.LONGVARBINARY;
                    jdbcTypeString = "MEDIUMBLOB";
                } else {
                    jdbcType = Types.LONGVARCHAR;
                    jdbcTypeString = "VARCHAR";
                }
                break;

            case -1:
            default:
                if (CharsetMapping.MYSQL_COLLATION_INDEX_binary == meta.getCollation()) {
                    jdbcType = Types.LONGVARBINARY;
                    jdbcTypeString = "LONGBLOB";
                } else {
                    jdbcType = Types.LONGVARCHAR;
                    jdbcTypeString = "VARCHAR";
                }
                break;
            }
            fieldLength = meta.getLength();
            if (fieldLength < 0) {
                fieldLength = 4294967295L;
            }
            break;

        case MYSQL_TYPE_ENUM:
            jdbcType = Types.CHAR;
            jdbcTypeString = "CHAR";
            fieldLength = 4; // TODO: Always this?
            break;

        case MYSQL_TYPE_SET:
            jdbcType = Types.CHAR;
            jdbcTypeString = "CHAR";
            fieldLength = 20; // TODO: Always this?
            break;

        case MYSQL_TYPE_JSON:
            jdbcType = Types.CHAR;
            jdbcTypeString = "JSON";
            fieldLength = 4294967295L;
            break;

        case MYSQL_TYPE_GEOMETRY:
            jdbcType = Types.BINARY;
            jdbcTypeString = "GEOMETRY";
            fieldLength = 4294967295L;
            break;

        case MYSQL_TYPE_NULL:
            jdbcType = Types.NULL;
            jdbcTypeString = "NULL";
            fieldLength = 0;
            break;

        default:
            throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT,
                "Unknown meta type: " + meta.getOriginalType().name());
        }
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public String getJdbcTypeString() {
        return jdbcTypeString;
    }

    public long getFieldLength() {
        return fieldLength;
    }
}
