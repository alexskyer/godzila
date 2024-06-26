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

package org.alpha.godzila.connector.stream.sink;

import org.alpha.godzila.config.SyncConfig;
import org.alpha.godzila.connector.stream.config.StreamConfig;
import org.alpha.godzila.connector.stream.converter.StreamRawTypeConverter;
import org.alpha.godzila.connector.stream.converter.StreamSqlConverter;
import org.alpha.godzila.connector.stream.converter.StreamSyncConverter;
import org.alpha.godzila.converter.AbstractRowConverter;
import org.alpha.godzila.converter.RawTypeMapper;
import org.alpha.godzila.sink.SinkFactory;
import org.alpha.godzila.util.GsonUtil;
import org.alpha.godzila.util.TableUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class StreamSinkFactory extends SinkFactory {

    private final StreamConfig streamConfig;

    public StreamSinkFactory(SyncConfig config) {
        super(config);
        streamConfig =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()),
                        StreamConfig.class);
        streamConfig.setColumn(config.getWriter().getFieldList());
        super.initCommonConf(streamConfig);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        StreamOutputFormatBuilder builder = new StreamOutputFormatBuilder();
        builder.setStreamConfig(streamConfig);
        AbstractRowConverter converter;
        if (useAbstractBaseColumn) {
            converter = new StreamSyncConverter(streamConfig);
        } else {
            final RowType rowType =
                    TableUtil.createRowType(streamConfig.getColumn(), getRawTypeMapper());
            converter = new StreamSqlConverter(rowType);
        }

        builder.setRowConverter(converter, useAbstractBaseColumn);
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return StreamRawTypeConverter::apply;
    }
}
