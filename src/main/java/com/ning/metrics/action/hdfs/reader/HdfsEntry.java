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

import com.google.common.collect.ImmutableMap;
import com.ning.metrics.action.hdfs.data.RowFileContentsIterator;
import com.ning.metrics.action.hdfs.data.RowFileContentsIteratorFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonValue;
import org.joda.time.DateTime;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Summarizes information about HDFS files and directories in a Hadoop-agnostic way.
 */
public class HdfsEntry
{
    private final Path path;
    private final long blockSize;
    private final long size;
    private final short replication;
    private final long replicatedSize;
    private final DateTime modificationDate;
    private final boolean directory;
    private final FileSystem fs;
    private final boolean raw;
    private final RowFileContentsIteratorFactory rowFileContentsIteratorFactory;

    private static final String[] SIZES = {"B", "KB", "MB", "GB", "TB", "PB"};

    public static final String JSON_ENTRY_PATH = "path";
    public static final String JSON_ENTRY_MTIME = "mtime";
    public static final String JSON_ENTRY_SIZE = "size";
    public static final String JSON_ENTRY_REPLICATION = "replication";
    public static final String JSON_ENTRY_IS_DIR = "isDir";
    public static final String JSON_ENTRY_CONTENT = "content";

    @JsonCreator
    @SuppressWarnings("unused")
    public HdfsEntry(
        @JsonProperty(JSON_ENTRY_PATH) String path,
        @JsonProperty(JSON_ENTRY_MTIME) long mtime,
        @JsonProperty(JSON_ENTRY_SIZE) long sizeInBytes,
        @JsonProperty(JSON_ENTRY_REPLICATION) short replication,
        @JsonProperty(JSON_ENTRY_IS_DIR) boolean isDirectory
    )
    {
        this.fs = null;
        this.path = new Path(path);
        this.modificationDate = new DateTime(mtime);
        this.blockSize = -1;
        this.size = sizeInBytes;
        this.replication = replication;
        this.replicatedSize = sizeInBytes * replication;
        this.directory = isDirectory;

        this.raw = true;
        this.rowFileContentsIteratorFactory = null;
    }

    public HdfsEntry(FileSystem fs, FileStatus status, boolean raw, RowFileContentsIteratorFactory rowFileContentsIteratorFactory) throws IOException
    {
        this.fs = fs;
        this.path = status.getPath();
        this.modificationDate = new DateTime(status.getModificationTime());

        this.blockSize = status.getBlockSize();
        this.size = status.getLen();
        this.replication = status.getReplication();
        this.replicatedSize = status.getReplication() * status.getLen();

        this.directory = status.isDir();

        this.raw = raw;
        this.rowFileContentsIteratorFactory = rowFileContentsIteratorFactory;
    }

    public String getPath()
    {
        return path.toUri().getPath();
    }

    /**
     * Get the length of the file, in bytes
     *
     * @return the length of this file, in bytes
     */
    public long getSize()
    {
        return size;
    }

    /**
     * Pretty print the size of the file
     *
     * @return a string representing the size of the file
     */
    public String getPrettySize()
    {
        DecimalFormat format = new DecimalFormat();
        long sizeInBytes = size;
        int i = 0;

        while (sizeInBytes > 1023 && i < SIZES.length - 1) {
            sizeInBytes /= 1024;
            i += 1;
        }

        if (sizeInBytes < 10) {
            format.setMaximumFractionDigits(1);
        }

        return format.format(size) + " " + SIZES[i];
    }

    public short getReplication()
    {
        return replication;
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

    @JsonValue
    @SuppressWarnings({"unchecked", "unused"})
    public ImmutableMap toMap()
    {
        RowFileContentsIterator content = null;
        try {
            content = getContent();
        }
        catch (IOException ignored) {
        }

        return new ImmutableMap.Builder()
            .put(JSON_ENTRY_PATH, getPath())
            .put(JSON_ENTRY_MTIME, getModificationDate().getMillis())
            .put(JSON_ENTRY_SIZE, getSize())
            .put(JSON_ENTRY_REPLICATION, getReplication())
            .put(JSON_ENTRY_IS_DIR, isDirectory())
            .put(JSON_ENTRY_CONTENT, content)
            .build();
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
