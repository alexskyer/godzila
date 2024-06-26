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

package org.alpha.godzila.connector.stream.util;

import org.alpha.godzila.config.FieldConfig;
import org.alpha.godzila.config.TypeConfig;
import org.alpha.godzila.connector.stream.config.StreamConfig;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.util.List;
import java.util.stream.Collectors;

public class StreamConfigUtil {

    private StreamConfigUtil() throws IllegalAccessException {
        throw new IllegalAccessException(getClass() + " can not be instantiated.");
    }

    public static void extractFieldConfig(ResolvedSchema schema, StreamConfig streamConfig) {
        List<FieldConfig> fieldConfigList =
                schema.getColumns().stream()
                        .map(
                                column -> {
                                    FieldConfig fieldConfig = new FieldConfig();
                                    fieldConfig.setName(column.getName());
                                    fieldConfig.setType(
                                            TypeConfig.fromString(column.getDataType().toString()));
                                    return fieldConfig;
                                })
                        .collect(Collectors.toList());

        streamConfig.setColumn(fieldConfigList);
    }
}
