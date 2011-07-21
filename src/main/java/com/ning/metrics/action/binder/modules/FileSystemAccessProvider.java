/*
 * Copyright 2010-2011 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.metrics.action.binder.modules;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.ning.metrics.action.binder.config.ActionCoreConfig;
import com.ning.metrics.serialization.hadoop.FileSystemAccess;
import com.ning.metrics.serialization.hadoop.HadoopSmileOutputStreamSerialization;
import com.ning.metrics.serialization.hadoop.HadoopThriftEnvelopeSerialization;
import com.ning.metrics.serialization.hadoop.HadoopThriftWritableSerialization;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

class FileSystemAccessProvider implements Provider<FileSystemAccess>
{
    private final FileSystemAccess fileSystemAccess;

    @Inject
    public FileSystemAccessProvider(final ActionCoreConfig actionCoreConfig) throws IOException
    {
        final Configuration hadoopConfig = new Configuration();

        final String hfsHost = actionCoreConfig.getNamenodeUrl();
        if (hfsHost.isEmpty()) {
            // Local filesystem, for testing
            hadoopConfig.set("fs.default.name", "file:///");
        }
        else {
            hadoopConfig.set("fs.default.name", hfsHost);
        }

        hadoopConfig.setBoolean("fs.automatic.close", false);
        hadoopConfig.setLong("dfs.block.size", actionCoreConfig.getHadoopBlockSize());
        hadoopConfig.set("hadoop.job.ugi", actionCoreConfig.getHadoopUgi());
        hadoopConfig.setStrings("io.serializations", HadoopThriftWritableSerialization.class.getName(),
            HadoopThriftEnvelopeSerialization.class.getName(),
            HadoopSmileOutputStreamSerialization.class.getName(),
            "org.apache.hadoop.io.serializer.WritableSerialization",
            actionCoreConfig.getSerializations());

        fileSystemAccess = new FileSystemAccess(hadoopConfig);
    }

    public FileSystemAccess get()
    {
        return fileSystemAccess;
    }
}