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

package org.alpha.godzila.typeutil.serializer;

import org.alpha.godzila.element.AbstractBaseColumn;
import org.alpha.godzila.typeutil.serializer.base.*;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.*;
import org.apache.flink.table.types.logical.*;

public class AbstractColumnSerializerUtil {

    /**
     * Creates a TypeSerializer for internal data structures of the given LogicalType and
     * descriptor.
     */
    public static TypeSerializer<AbstractBaseColumn> getTypeSerializer(
            LogicalType logicalType, String descriptor) {
        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case STRUCTURED_TYPE:
                return new StringColumnSerializer(descriptor);
            case BOOLEAN:
                return BooleanColumnSerializer.INSTANCE;
            case BINARY:
            case VARBINARY:
                return BytesColumnSerializer.INSTANCE;
            case DECIMAL:
                return DecimalColumnSerializer.INSTANCE;
            case TINYINT:
                return ByteColumnSerializer.INSTANCE;
            case SMALLINT:
                return ShortColumnSerializer.INSTANCE;
            case INTERVAL_YEAR_MONTH:
                return new YearMonthColumnSerializer((YearMonthIntervalType) logicalType);
            case INTERVAL_DAY_TIME:
                return new DayTimeColumnSerializer((DayTimeIntervalType) logicalType);
            case INTEGER:
                return IntColumnSerializer.INSTANCE;
            case DATE:
                return SqlDateColumnSerializer.INSTANCE;
            case TIME_WITHOUT_TIME_ZONE:
                return TimeColumnSerializer.INSTANCE;
            case TIMESTAMP_WITH_TIME_ZONE:
                return ZonedTimestampColumnSerializer.INSTANCE;
            case BIGINT:
                return LongColumnSerializer.INSTANCE;
            case FLOAT:
                return FloatColumnSerializer.INSTANCE;
            case DOUBLE:
                return DoubleColumnSerializer.INSTANCE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampColumnSerializer.INSTANCE;
            case ARRAY:
                LogicalType elementType = ((ArrayType) logicalType).getElementType();
                return new ArrayColumnSerializer(elementType);
            case MAP:
                LogicalType keyType = ((MapType) logicalType).getKeyType();
                LogicalType valueType = ((MapType) logicalType).getValueType();
                return new BaseMapColumnSerializer(keyType, valueType);
            case NULL:
                return NullColumnSerializer.INSTANCE;
            case MULTISET:
            case ROW:
            case DISTINCT_TYPE:
            case RAW:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type '" + logicalType + "' to get internal serializer");
        }
    }

    public static TypeSerializerSingleton getBaseSerializer(LogicalType elementType) {
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                return BooleanSerializer.INSTANCE;
            case TINYINT:
                return ByteSerializer.INSTANCE;
            case SMALLINT:
                return ShortSerializer.INSTANCE;
            case INTEGER:
                return IntSerializer.INSTANCE;
            case BIGINT:
                return LongSerializer.INSTANCE;
            case FLOAT:
                return FloatSerializer.INSTANCE;
            case DOUBLE:
                return DoubleSerializer.INSTANCE;
            case DECIMAL:
                return BigDecSerializer.INSTANCE;
            case CHAR:
            case VARCHAR:
                return StringSerializer.INSTANCE;
            case TIME_WITHOUT_TIME_ZONE:
                return SqlTimeSerializer.INSTANCE;
            case DATE:
                return SqlDateSerializer.INSTANCE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return SqlTimestampSerializer.INSTANCE;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + elementType);
        }
    }
}
