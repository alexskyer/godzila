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

package org.alpha.godzila.typeutil.serializer.base;

import org.alpha.godzila.element.AbstractBaseColumn;
import org.alpha.godzila.element.column.MapColumn;
import org.alpha.godzila.element.column.NullColumn;
import org.alpha.godzila.throwable.GodzilaRuntimeException;
import org.alpha.godzila.util.JsonUtil;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Map;

public class MapColumnSerializer extends TypeSerializerSingleton<AbstractBaseColumn> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the MapColumnSerializer. */
    public static final MapColumnSerializer INSTANCE = new MapColumnSerializer();

    private static final NullColumn REUSE_NULL = new NullColumn();
    private final StringSerializer stringSerializer = StringSerializer.INSTANCE;

    private static final MapColumn EMPTY = new MapColumn(null);

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public AbstractBaseColumn createInstance() {
        return EMPTY;
    }

    @Override
    public AbstractBaseColumn copy(AbstractBaseColumn from) {
        return from;
    }

    @Override
    public AbstractBaseColumn copy(AbstractBaseColumn from, AbstractBaseColumn reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(AbstractBaseColumn record, DataOutputView target) throws IOException {
        if (record == null || record instanceof NullColumn) {
            target.write(0);
        } else {
            target.write(1);
            stringSerializer.serialize(JsonUtil.toJson(record.getData()), target);
        }
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        byte type = source.readByte();
        switch (type) {
            case 0:
                return REUSE_NULL;
            case 1:
                return MapColumn.from(
                        JsonUtil.toObject(stringSerializer.deserialize(source), Map.class));
            default:
                throw new GodzilaRuntimeException("you should not be here");
        }
    }

    @Override
    public AbstractBaseColumn deserialize(AbstractBaseColumn reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        byte type = source.readByte();
        target.write(type);
        if (type == 1) {
            stringSerializer.copy(source, target);
        } else if (type != 0) {
            throw new GodzilaRuntimeException("you should not be here");
        }
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new MapColumnSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class MapColumnSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<AbstractBaseColumn> {

        public MapColumnSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
