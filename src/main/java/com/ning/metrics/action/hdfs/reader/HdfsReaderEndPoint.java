/*
 * Copyright 2010 Ning, Inc.
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

package com.ning.metrics.action.hdfs.reader;

import com.google.inject.Inject;
import com.ning.metrics.action.binder.config.ActionCoreConfig;
import com.ning.metrics.action.hdfs.data.RowFileContentsIteratorFactory;
import com.ning.metrics.serialization.hadoop.HadoopThriftEnvelopeSerialization;
import com.ning.metrics.serialization.hadoop.HadoopThriftWritableSerialization;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HdfsReaderEndPoint
{
    private static final Logger log = Logger.getLogger(HdfsReaderEndPoint.class);

    private final FileSystem fileSystem;
    private final RowFileContentsIteratorFactory rowFileContentsIteratorFactory;

    @Inject
    public HdfsReaderEndPoint(
        RowFileContentsIteratorFactory rowFileContentsIteratorFactory,
        ActionCoreConfig config
    ) throws IOException
    {
        this.rowFileContentsIteratorFactory = rowFileContentsIteratorFactory;
        Configuration conf = new Configuration();

        conf.set("fs.default.name", config.getNamenodeUrl());
        conf.set("hadoop.job.ugi", config.getHadoopUgi());
        conf.setStrings("io.serializations", HadoopThriftWritableSerialization.class.getName(), HadoopThriftEnvelopeSerialization.class.getName(), "org.apache.hadoop.io.serializer.WritableSerialization", config.getSerializations());

        this.fileSystem = FileSystem.get(conf);

        log.info("Connected successfully to HDFS!");
    }

    public boolean isDir(String path) throws IOException
    {
        return !fileSystem.isFile(new Path(path));
    }

    /**
     * Return all entries in a directory.
     *
     * @param dir directory entries to find
     * @return HdfsListing containing entries in the directory
     * @throws IOException HDFS crawling error
     */
    public HdfsListing getListing(String dir) throws IOException
    {
        return getListing(dir, false, false);
    }

    /**
     * Return content in a directory/file, possibly recursively.
     *
     * @param path
     * @param raw
     * @param recursive
     * @return HdfsListing containing entries in the directory
     * @throws java.io.IOException
     */
    public HdfsListing getListing(String path, boolean raw, boolean recursive) throws IOException
    {
        return new HdfsListing(fileSystem, new Path(path), raw, rowFileContentsIteratorFactory, recursive);
    }
}
