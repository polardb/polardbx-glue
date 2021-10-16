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

package com.alibaba.polardbx.rpc.compatible;

import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.common.exception.NotSupportException;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @version 1.0
 */
public class XResultSetMetaData implements ResultSetMetaData {

    private final XResult result;

    public XResultSetMetaData(XResult result) {
        this.result = result;
    }

    public XResult getResult() {
        return result;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return result.getMetaData().size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return (result.getMetaData().get(column - 1).getFlags() & 0x0100) != 0;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int isNullable(int column) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        try {
            return result.getMetaData().get(column - 1).getName()
                .toString(XSession.toJavaEncoding(result.getSession().getResultMetaEncodingMySQL()));
        } catch (Exception e) {
            throw new SQLException("Failed to get column label from XResult.", e);
        }
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        try {
            return result.getMetaData().get(column - 1).getName()
                .toString(XSession.toJavaEncoding(result.getSession().getResultMetaEncodingMySQL()));
        } catch (Exception e) {
            throw new SQLException("Failed to get column name from XResult.", e);
        }
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        try {
            return result.getMetaData().get(column - 1).getSchema()
                .toString(XSession.toJavaEncoding(result.getSession().getResultMetaEncodingMySQL()));
        } catch (Exception e) {
            throw new SQLException("Failed to get schema name from XResult.", e);
        }
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return result.getMetaData().get(column - 1).getLength();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return result.getMetaData().get(column - 1).getFractionalDigits();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        try {
            return result.getMetaData().get(column - 1).getTable()
                .toString(XSession.toJavaEncoding(result.getSession().getResultMetaEncodingMySQL()));
        } catch (Exception e) {
            throw new SQLException("Failed to get table name from XResult.", e);
        }
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        try {
            return result.getMetaData().get(column - 1).getCatalog()
                .toString(XSession.toJavaEncoding(result.getSession().getResultMetaEncodingMySQL()));
        } catch (Exception e) {
            throw new SQLException("Failed to get catalog name from XResult.", e);
        }
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        } else {
            throw new SQLException("not a wrapper for " + iface);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return XResultSetMetaData.class.isAssignableFrom(iface);
    }
}
