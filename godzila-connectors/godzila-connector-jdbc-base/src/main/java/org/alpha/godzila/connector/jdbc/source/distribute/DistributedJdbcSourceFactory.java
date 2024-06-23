/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.alpha.godzila.connector.jdbc.source.distribute;

import org.alpha.godzila.config.FieldConfig;
import org.alpha.godzila.config.SyncConfig;
import org.alpha.godzila.config.TypeConfig;
import org.alpha.godzila.connector.jdbc.config.ConnectionConfig;
import org.alpha.godzila.connector.jdbc.config.DataSourceConfig;
import org.alpha.godzila.connector.jdbc.dialect.JdbcDialect;
import org.alpha.godzila.connector.jdbc.source.JdbcInputFormatBuilder;
import org.alpha.godzila.connector.jdbc.source.JdbcSourceFactory;
import org.alpha.godzila.util.ColumnBuildUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public abstract class DistributedJdbcSourceFactory extends JdbcSourceFactory {

    protected DistributedJdbcSourceFactory(
            SyncConfig syncConfig, StreamExecutionEnvironment env, JdbcDialect jdbcDialect) {
        super(syncConfig, env, jdbcDialect);
    }

    protected JdbcInputFormatBuilder getBuilder() {
        DistributedJdbcInputFormatBuilder builder =
                new DistributedJdbcInputFormatBuilder(new DistributedJdbcInputFormat());
        List<ConnectionConfig> connectionConfigList = jdbcConfig.getConnection();
        List<DataSourceConfig> dataSourceConfigList = new ArrayList<>(connectionConfigList.size());
        for (ConnectionConfig connectionConfig : connectionConfigList) {
            String currentUsername =
                    (StringUtils.isNotBlank(connectionConfig.getUsername()))
                            ? connectionConfig.getUsername()
                            : jdbcConfig.getUsername();
            String currentPassword =
                    (StringUtils.isNotBlank(connectionConfig.getPassword()))
                            ? connectionConfig.getPassword()
                            : jdbcConfig.getPassword();

            String schema = connectionConfig.getSchema();
            for (String table : connectionConfig.getTable()) {
                DataSourceConfig dataSourceConfig = new DataSourceConfig();
                dataSourceConfig.setUserName(currentUsername);
                dataSourceConfig.setPassword(currentPassword);
                dataSourceConfig.setJdbcUrl(connectionConfig.obtainJdbcUrl());
                dataSourceConfig.setTable(table);
                dataSourceConfig.setSchema(schema);

                dataSourceConfigList.add(dataSourceConfig);
            }
        }
        builder.setSourceList(dataSourceConfigList);
        return builder;
    }

    @Override
    protected void initColumnInfo() {
        columnNameList = new ArrayList<>();
        columnTypeList = new ArrayList<>();
        for (FieldConfig fieldConfig : jdbcConfig.getColumn()) {
            this.columnNameList.add(fieldConfig.getName());
            this.columnTypeList.add(fieldConfig.getType());
        }
        Pair<List<String>, List<TypeConfig>> columnPair =
                ColumnBuildUtil.handleColumnList(
                        jdbcConfig.getColumn(), this.columnNameList, this.columnTypeList);
        this.columnNameList = columnPair.getLeft();
        this.columnTypeList = columnPair.getRight();
    }
}
