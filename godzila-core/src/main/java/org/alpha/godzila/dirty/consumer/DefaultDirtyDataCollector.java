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
package org.alpha.godzila.dirty.consumer;

import org.alpha.godzila.dirty.DirtyConfig;
import org.alpha.godzila.dirty.impl.DirtyDataEntry;
import org.alpha.godzila.dirty.utils.TablePrintUtil;

public class DefaultDirtyDataCollector extends DirtyDataCollector {

    private static final long serialVersionUID = -8471928215911902043L;

    @Override
    protected void init(DirtyConfig conf) {}

    @Override
    protected void consume(DirtyDataEntry dirty) {
        TablePrintUtil.printTable(dirty);
    }

    @Override
    public void close() {}
}
