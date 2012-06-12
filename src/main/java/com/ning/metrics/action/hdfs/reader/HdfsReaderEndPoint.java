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

package com.ning.metrics.action.hdfs.reader;

import com.google.common.io.ByteStreams;
import com.ning.metrics.action.hdfs.data.RowFileContentsIteratorFactory;
import com.ning.metrics.serialization.hadoop.FileSystemAccess;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;

public class HdfsReaderEndPoint
{
    private final FileSystemAccess fileSystemAccess;
    private final RowFileContentsIteratorFactory rowFileContentsIteratorFactory;

    @Inject
    public HdfsReaderEndPoint(final RowFileContentsIteratorFactory rowFileContentsIteratorFactory, final FileSystemAccess fileSystemAccess) throws IOException
    {
        this.rowFileContentsIteratorFactory = rowFileContentsIteratorFactory;
        this.fileSystemAccess = fileSystemAccess;
    }

    public boolean isDir(final String path) throws IOException
    {
        return !fileSystemAccess.get().isFile(new Path(path));
    }

    /**
     * Return all entries in a directory.
     *
     * @param dir directory entries to find
     * @return HdfsListing containing entries in the directory
     * @throws IOException HDFS crawling error
     */
    public HdfsListing getListing(final String dir) throws IOException
    {
        return getListing(dir, false, false);
    }

    /**
     * Return content in a directory/file, possibly recursively.
     *
     * @param path      path to either a file or directory
     * @param raw       whether to lookup in Goodwill the schema
     * @param recursive for directories, whether to download recursively the data
     * @return HdfsListing containing entries in the directory
     * @throws java.io.IOException generic I/O Exception
     */
    public HdfsListing getListing(final String path, final boolean raw, final boolean recursive) throws IOException
    {
        return new HdfsListing(fileSystemAccess.get(), new Path(path), raw, rowFileContentsIteratorFactory, recursive);
    }

    /**
     * Stream a file from HDFS without doing any deserialization.
     *
     * @param path path to a file
     * @return stream of data
     * @throws IOException generic I/O Exception
     */
    public StreamingOutput getFile(final String path) throws IOException
    {
        final FSDataInputStream inputStream = fileSystemAccess.get().open(new Path(path));

        return new StreamingOutput()
        {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException
            {
                try {
                    ByteStreams.copy(inputStream, output);
                }
                finally {
                    inputStream.close();
                }
            }
        };
    }
}
