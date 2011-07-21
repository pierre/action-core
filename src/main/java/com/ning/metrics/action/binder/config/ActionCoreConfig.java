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

package com.ning.metrics.action.binder.config;

import org.skife.config.Config;
import org.skife.config.Default;

public interface ActionCoreConfig
{
    @Config("action.hadoop.namenode.url")
    @Default("hdfs://127.0.0.1:9000")
    String getNamenodeUrl();

    @Config("action.hadoop.ugi")
    @Default("hadoop,hadoop")
    String getHadoopUgi();

    @Config("sction.hadoop.block.size")
    @Default("134217728")
    long getHadoopBlockSize();

    @Config("action.hadoop.io.row.serializations")
    @Default("")
    String getRowSerializations();

    @Config("action.hadoop.io.serializations")
    @Default("org.apache.hadoop.io.serializer.WritableSerialization")
    String getSerializations();

    @Config("action.registrar.host")
    @Default("127.0.0.1")
    String getRegistrarHost();

    @Config("action.registrar.port")
    @Default("8081")
    int getRegistrarPort();

    @Config("action.registrar.file")
    @Default(".registrar.cache")
    String getRegistrarStateFile();
}
