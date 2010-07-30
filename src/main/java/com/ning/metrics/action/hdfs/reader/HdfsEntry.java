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

import com.ning.metrics.action.hdfs.data.RowFileContentsIterator;
import com.ning.metrics.action.hdfs.data.RowFileContentsIteratorFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

import java.io.IOException;

/**
 * Summarizes information about HDFS files and directories in a Hadoop-agnostic way.
 */
public class HdfsEntry
{
    private final Path path;
    private final long size;
    private final long replicatedSize;
    private final DateTime modificationDate;
    private final boolean directory;
    private final FileSystem fs;
    private final boolean raw;
    private final RowFileContentsIteratorFactory rowFileContentsIteratorFactory;

    public HdfsEntry(FileSystem fs, FileStatus status, boolean raw, RowFileContentsIteratorFactory rowFileContentsIteratorFactory) throws IOException
    {
        this.fs = fs;
        this.path = status.getPath();
        this.modificationDate = new DateTime(status.getModificationTime());

        if (status.isDir()) {
            this.size = 0;
            this.replicatedSize = 0;
            this.directory = true;
        }
        else {
            this.size = status.getLen();
            this.replicatedSize = status.getReplication() * status.getLen();
            this.directory = false;
        }

        this.raw = raw;
        this.rowFileContentsIteratorFactory = rowFileContentsIteratorFactory;
    }

    public String getPath()
    {
        return path.toUri().getPath();
    }

    public long getSize()
    {
        return size;
    }

    public long getReplicatedSize()
    {
        return replicatedSize;
    }

    public DateTime getModificationDate()
    {
        return modificationDate;
    }

    public boolean isDirectory()
    {
        return directory;
    }

    public RowFileContentsIterator getContent() throws IOException
    {
        return rowFileContentsIteratorFactory.build(fs, path, raw);
    }

    @Override
    public String toString()
    {
        return "HdfsEntry{" +
            "path='" + path + '\'' +
            ", size=" + size +
            ", replicatedSize=" + replicatedSize +
            ", modificationDate=" + modificationDate +
            ", directory=" + directory +
            '}';
    }
}
