/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alpha.godzila.connector.jdbc.table;

import org.alpha.godzila.connector.jdbc.config.JdbcConfig;
import org.alpha.godzila.connector.jdbc.config.JdbcLookupConfig;
import org.alpha.godzila.connector.jdbc.config.SinkConnectionConfig;
import org.alpha.godzila.connector.jdbc.config.SourceConnectionConfig;
import org.alpha.godzila.connector.jdbc.dialect.JdbcDialect;
import org.alpha.godzila.connector.jdbc.sink.JdbcDynamicTableSink;
import org.alpha.godzila.connector.jdbc.sink.JdbcOutputFormat;
import org.alpha.godzila.connector.jdbc.sink.JdbcOutputFormatBuilder;
import org.alpha.godzila.connector.jdbc.source.JdbcDynamicTableSource;
import org.alpha.godzila.connector.jdbc.source.JdbcInputFormat;
import org.alpha.godzila.connector.jdbc.source.JdbcInputFormatBuilder;
import org.alpha.godzila.connector.jdbc.util.JdbcUtil;
import org.alpha.godzila.enums.Semantic;
import org.alpha.godzila.lookup.config.LookupConfig;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import java.util.*;

import static org.alpha.godzila.connector.jdbc.options.JdbcCommonOptions.*;
import static org.alpha.godzila.connector.jdbc.options.JdbcLookupOptions.*;
import static org.alpha.godzila.connector.jdbc.options.JdbcSinkOptions.*;
import static org.alpha.godzila.connector.jdbc.options.JdbcSinkOptions.SINK_PARALLELISM;
import static org.alpha.godzila.connector.jdbc.options.JdbcSourceOptions.SCAN_CUSTOM_SQL;
import static org.alpha.godzila.connector.jdbc.options.JdbcSourceOptions.SCAN_WHERE;
import static org.alpha.godzila.lookup.options.LookupOptions.*;
import static org.alpha.godzila.source.options.SourceOptions.*;
import static org.alpha.godzila.table.options.SinkOptions.*;
import static org.apache.flink.util.Preconditions.checkState;

public abstract class JdbcDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        helper.validateExcept(VERTX_PREFIX, DRUID_PREFIX);
        validateConfigOptions(config, resolvedSchema);
        // 3.封装参数
        JdbcDialect jdbcDialect = getDialect();

        final Map<String, Object> druidConf =
                getLibConfMap(context.getCatalogTable().getOptions(), DRUID_PREFIX);

        return new JdbcDynamicTableSource(
                getSourceConnectionConfig(helper.getOptions()),
                getJdbcLookupConfig(
                        helper.getOptions(),
                        context.getObjectIdentifier().getObjectName(),
                        druidConf),
                resolvedSchema,
                jdbcDialect,
                getInputFormatBuilder());
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        validateConfigOptions(config, resolvedSchema);
        JdbcDialect jdbcDialect = getDialect();

        // 3.封装参数
        return new JdbcDynamicTableSink(
                getSinkConnectionConfig(helper.getOptions(), resolvedSchema),
                jdbcDialect,
                resolvedSchema,
                getOutputFormatBuilder());
    }

    protected JdbcConfig getSinkConnectionConfig(
            ReadableConfig readableConfig, ResolvedSchema schema) {
        JdbcConfig jdbcConfig = new JdbcConfig();
        SinkConnectionConfig conf = new SinkConnectionConfig();
        jdbcConfig.setConnection(Collections.singletonList(conf));
        conf.setJdbcUrl(readableConfig.get(URL));
        conf.setTable(Arrays.asList(readableConfig.get(TABLE_NAME)));
        conf.setSchema(readableConfig.get(SCHEMA));
        conf.setAllReplace(readableConfig.get(SINK_ALL_REPLACE));

        jdbcConfig.setUsername(readableConfig.get(USERNAME));
        jdbcConfig.setPassword(readableConfig.get(PASSWORD));

        jdbcConfig.setAllReplace(conf.isAllReplace());
        jdbcConfig.setBatchSize(readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        jdbcConfig.setFlushIntervalMills(readableConfig.get(SINK_BUFFER_FLUSH_INTERVAL));
        jdbcConfig.setParallelism(readableConfig.get(SINK_PARALLELISM));
        jdbcConfig.setSemantic(readableConfig.get(SINK_SEMANTIC));

        if (StringUtils.isNotEmpty(readableConfig.get(SINK_PRE_SQL))) {
            jdbcConfig.setPreSql(Arrays.asList(readableConfig.get(SINK_PRE_SQL).split(";")));
        }
        if (StringUtils.isNotEmpty(readableConfig.get(SINK_POST_SQL))) {
            jdbcConfig.setPostSql(Arrays.asList(readableConfig.get(SINK_POST_SQL).split(";")));
        }

        List<String> keyFields = new ArrayList<>();
        if (schema.getPrimaryKey().isPresent()) {
            keyFields = schema.getPrimaryKey().get().getColumns();
        }

        jdbcConfig.setUniqueKey(keyFields);
        resetTableInfo(jdbcConfig);
        return jdbcConfig;
    }

    protected LookupConfig getJdbcLookupConfig(
            ReadableConfig readableConfig, String tableName, Map<String, Object> druidConf) {
        return JdbcLookupConfig.build()
                .setDruidConfig(druidConf)
                .setAsyncPoolSize(readableConfig.get(VERTX_WORKER_POOL_SIZE))
                .setTableName(tableName)
                .setPeriod(readableConfig.get(LOOKUP_CACHE_PERIOD))
                .setCacheSize(readableConfig.get(LOOKUP_CACHE_MAX_ROWS))
                .setCacheTtl(readableConfig.get(LOOKUP_CACHE_TTL))
                .setCache(readableConfig.get(LOOKUP_CACHE_TYPE))
                .setMaxRetryTimes(readableConfig.get(LOOKUP_MAX_RETRIES))
                .setErrorLimit(readableConfig.get(LOOKUP_ERROR_LIMIT))
                .setFetchSize(readableConfig.get(LOOKUP_FETCH_SIZE))
                .setAsyncTimeout(readableConfig.get(LOOKUP_ASYNC_TIMEOUT))
                .setParallelism(readableConfig.get(LOOKUP_PARALLELISM));
    }

    protected JdbcConfig getSourceConnectionConfig(ReadableConfig readableConfig) {
        JdbcConfig jdbcConfig = new JdbcConfig();
        SourceConnectionConfig conf = new SourceConnectionConfig();
        jdbcConfig.setConnection(Lists.newArrayList(conf));

        conf.setJdbcUrl(Lists.newArrayList(readableConfig.get(URL)));
        conf.setTable(Lists.newArrayList(readableConfig.get(TABLE_NAME)));
        conf.setSchema(readableConfig.get(SCHEMA));

        jdbcConfig.setJdbcUrl(readableConfig.get(URL));
        jdbcConfig.setUsername(readableConfig.get(USERNAME));
        jdbcConfig.setPassword(readableConfig.get(PASSWORD));

        jdbcConfig.setParallelism(readableConfig.get(SCAN_PARALLELISM));
        jdbcConfig.setFetchSize(
                readableConfig.get(SCAN_FETCH_SIZE) == 0
                        ? getDefaultFetchSize()
                        : readableConfig.get(SCAN_FETCH_SIZE));
        jdbcConfig.setQueryTimeOut(readableConfig.get(SCAN_QUERY_TIMEOUT));

        jdbcConfig.setSplitPk(readableConfig.get(SCAN_PARTITION_COLUMN));
        jdbcConfig.setSplitStrategy(readableConfig.get(SCAN_PARTITION_STRATEGY));

        String increColumn = readableConfig.get(SCAN_INCREMENT_COLUMN);
        if (StringUtils.isNotBlank(increColumn)) {
            jdbcConfig.setIncrement(true);
            jdbcConfig.setIncreColumn(increColumn);
            jdbcConfig.setIncreColumnType(readableConfig.get(SCAN_INCREMENT_COLUMN_TYPE));
        }

        jdbcConfig.setOrderByColumn(readableConfig.get(SCAN_ORDER_BY_COLUMN));

        jdbcConfig.setStartLocation(readableConfig.get(SCAN_START_LOCATION));

        jdbcConfig.setRestoreColumn(readableConfig.get(SCAN_RESTORE_COLUMNNAME));
        jdbcConfig.setRestoreColumnType(readableConfig.get(SCAN_RESTORE_COLUMNTYPE));

        Optional<Integer> pollingInterval = readableConfig.getOptional(SCAN_POLLING_INTERVAL);
        if (pollingInterval.isPresent() && pollingInterval.get() > 0) {
            jdbcConfig.setPolling(true);
            jdbcConfig.setPollingInterval(pollingInterval.get());
            jdbcConfig.setFetchSize(
                    readableConfig.get(SCAN_FETCH_SIZE) == 0
                            ? SCAN_DEFAULT_FETCH_SIZE.defaultValue()
                            : readableConfig.get(SCAN_FETCH_SIZE));
        }

        jdbcConfig.setWhere(readableConfig.get(SCAN_WHERE));
        jdbcConfig.setCustomSql(readableConfig.get(SCAN_CUSTOM_SQL));
        if (StringUtils.isBlank(jdbcConfig.getCustomSql())) {
            resetTableInfo(jdbcConfig);
        }
        return jdbcConfig;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(SCHEMA);

        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_PARTITION_STRATEGY);
        optionalOptions.add(SCAN_INCREMENT_COLUMN);
        optionalOptions.add(SCAN_INCREMENT_COLUMN_TYPE);
        optionalOptions.add(SCAN_POLLING_INTERVAL);
        optionalOptions.add(SCAN_START_LOCATION);
        optionalOptions.add(SCAN_PARALLELISM);
        optionalOptions.add(SCAN_QUERY_TIMEOUT);
        optionalOptions.add(SCAN_FETCH_SIZE);
        optionalOptions.add(SCAN_RESTORE_COLUMNNAME);
        optionalOptions.add(SCAN_RESTORE_COLUMNTYPE);
        optionalOptions.add(SCAN_ORDER_BY_COLUMN);
        optionalOptions.add(SCAN_WHERE);
        optionalOptions.add(SCAN_CUSTOM_SQL);

        optionalOptions.add(LOOKUP_CACHE_PERIOD);
        optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(LOOKUP_CACHE_TTL);
        optionalOptions.add(LOOKUP_CACHE_TYPE);
        optionalOptions.add(LOOKUP_MAX_RETRIES);
        optionalOptions.add(LOOKUP_ERROR_LIMIT);
        optionalOptions.add(LOOKUP_FETCH_SIZE);
        optionalOptions.add(LOOKUP_ASYNC_TIMEOUT);
        optionalOptions.add(LOOKUP_PARALLELISM);

        optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_ALL_REPLACE);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(SINK_SEMANTIC);
        optionalOptions.add(SINK_PRE_SQL);
        optionalOptions.add(SINK_POST_SQL);
        return optionalOptions;
    }

    protected void validateConfigOptions(ReadableConfig config, ResolvedSchema tableSchema) {
        String jdbcUrl = config.get(URL);
        final Optional<JdbcDialect> dialect = Optional.of(getDialect());
        checkState(dialect.get().canHandle(jdbcUrl), "Cannot handle such jdbc url: " + jdbcUrl);

        checkAllOrNone(config, new ConfigOption[] {USERNAME});

        if (config.getOptional(SCAN_POLLING_INTERVAL).isPresent()
                && config.getOptional(SCAN_POLLING_INTERVAL).get() > 0) {
            checkState(
                    StringUtils.isNotBlank(config.get(SCAN_INCREMENT_COLUMN)),
                    "scan.increment.column can not null or empty in polling-interval mode.");
        }

        checkAllOrNone(config, new ConfigOption[] {LOOKUP_CACHE_MAX_ROWS, LOOKUP_CACHE_TTL});

        if (config.get(LOOKUP_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            LOOKUP_MAX_RETRIES.key(), config.get(LOOKUP_MAX_RETRIES)));
        }

        if (config.get(SINK_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            SINK_MAX_RETRIES.key(), config.get(SINK_MAX_RETRIES)));
        }
        try {
            Semantic.getByName(config.get(SINK_SEMANTIC));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option should only be %s or %s, but is %s.",
                            SINK_SEMANTIC.key(),
                            Semantic.EXACTLY_ONCE.getAlisName(),
                            Semantic.AT_LEAST_ONCE.getAlisName(),
                            config.get(SINK_SEMANTIC)));
        }
        String orderByColumn = config.get(SCAN_ORDER_BY_COLUMN);
        if (orderByColumn != null) {
            boolean isExist =
                    tableSchema.getColumns().stream()
                            .anyMatch(tableColumn -> tableColumn.getName().equals(orderByColumn));
            if (!isExist) {
                throw new IllegalArgumentException(
                        String.format(
                                "The value of '%s' option must be one of the column names you defined",
                                SCAN_ORDER_BY_COLUMN.key()));
            }
        }
    }

    protected void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = 0;
        for (ConfigOption<?> configOption : configOptions) {
            if (config.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames =
                Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(
                configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n"
                        + String.join("\n", propertyNames));
    }

    /**
     * 子类根据不同数据库定义不同标记
     *
     * @return
     */
    @Override
    public abstract String factoryIdentifier();

    /**
     * 不同数据库不同方言
     *
     * @return
     */
    protected abstract JdbcDialect getDialect();

    /**
     * 从数据库中每次读取的条数，离线读取的时候每个插件需要测试，防止大数据量下生成大量临时文件
     *
     * @return
     */
    protected int getDefaultFetchSize() {
        return SCAN_DEFAULT_FETCH_SIZE.defaultValue();
    }

    /**
     * 获取JDBC插件的具体inputFormatBuilder
     *
     * @return JdbcInputFormatBuilder
     */
    protected JdbcInputFormatBuilder getInputFormatBuilder() {
        return new JdbcInputFormatBuilder(new JdbcInputFormat());
    }

    /**
     * 获取JDBC插件的具体outputFormatBuilder
     *
     * @return JdbcOutputFormatBuilder
     */
    protected JdbcOutputFormatBuilder getOutputFormatBuilder() {
        return new JdbcOutputFormatBuilder(new JdbcOutputFormat());
    }

    /** table字段有可能是schema.table格式 需要转换为对应的schema 和 table 字段* */
    protected void resetTableInfo(JdbcConfig jdbcConfig) {
        if (StringUtils.isBlank(jdbcConfig.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConfig, "\\\"", "\\\"");
        }
    }
}
