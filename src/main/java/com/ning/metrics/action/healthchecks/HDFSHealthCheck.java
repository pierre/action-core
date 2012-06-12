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

package com.ning.metrics.action.healthchecks;

import com.ning.metrics.serialization.hadoop.FileSystemAccess;

import com.yammer.metrics.core.HealthCheck;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weakref.jmx.Managed;

import javax.inject.Inject;
import java.io.IOException;

public class HDFSHealthCheck extends HealthCheck
{
    private final FileSystemAccess fileSystemAccess;
    private static final long MAX_WAIT_TIME = 10000; // 10 seconds

    @Inject
    public HDFSHealthCheck(final FileSystemAccess fileSystemAccess)
    {
        super("HDFSHealthCheck");
        this.fileSystemAccess = fileSystemAccess;
    }

    @Override
    public Result check()
    {
        try {
            final FileSystem fs = fileSystemAccess.get(MAX_WAIT_TIME);
            if (fs == null) {
                return Result.unhealthy("Unable to talk to HDFS");
            }
            else {
                return Result.healthy();
            }
        }
        catch (IOException e) {
            return Result.unhealthy(e);
        }
    }

    @Managed
    public boolean isHealthy()
    {
        return check().isHealthy();
    }
}
