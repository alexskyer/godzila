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

package org.alpha.godzila.connector.ftp.source;

import org.alpha.godzila.connector.ftp.config.FtpConfig;
import org.alpha.godzila.source.format.BaseRichInputFormatBuilder;
import org.alpha.godzila.throwable.GodzilaRuntimeException;
import org.apache.commons.lang3.StringUtils;

public class FtpInputFormatBuilder extends BaseRichInputFormatBuilder<FtpInputFormat> {

    public FtpInputFormatBuilder() {
        super(new FtpInputFormat());
    }

    public void setFtpConfig(FtpConfig ftpConfig) {
        super.setConfig(ftpConfig);
        format.setFtpConfig(ftpConfig);
    }

    @Override
    protected void checkFormat() {
        FtpConfig ftpConfig = format.getFtpConfig();
        if (StringUtils.isBlank(ftpConfig.getProtocol())) {
            throw new GodzilaRuntimeException("Please Set protocol");
        }
        if (StringUtils.isBlank(ftpConfig.getHost())) {
            throw new GodzilaRuntimeException("Please Set host");
        }
        if (StringUtils.isBlank(ftpConfig.getPath())) {
            throw new GodzilaRuntimeException("Please Set path");
        }
    }
}
