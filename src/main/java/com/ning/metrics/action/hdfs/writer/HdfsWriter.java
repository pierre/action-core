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

package com.ning.metrics.action.hdfs.writer;

import com.google.inject.Inject;
import com.ning.metrics.serialization.hadoop.FileSystemAccess;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class HdfsWriter
{
    private static final Logger log = Logger.getLogger(HdfsWriter.class);
    // Make sure to bump your young generation
    private static final int ONE_MEG = 1024;

    private final FileSystemAccess fileSystemAccess;

    @Inject
    public HdfsWriter(final FileSystemAccess fileSystemAccess)
    {
        this.fileSystemAccess = fileSystemAccess;
    }

    public URI write(
        final InputStream inputStream,
        final String outputPath,
        final boolean overwrite,
        final short replication,
        final long blockSize,
        final String permission
    ) throws IOException
    {
        final long start = System.nanoTime();
        log.info(String.format("Writing to HDFS: %s", outputPath));

        final Path hdfsPath = new Path(outputPath);
        FSDataOutputStream outputStream = null;
        int bytesWritten = 0;
        try {
            new FsPermission(permission);
            outputStream = fileSystemAccess.get().create(hdfsPath, new FsPermission(permission), overwrite,
                ONE_MEG, replication, blockSize, null);

            byte[] buffer = new byte[ONE_MEG];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) > 0) {
                outputStream.write(buffer, 0, bytesRead);
                bytesWritten += bytesRead;
            }

            // GC-ready
            //noinspection UnusedAssignment
            buffer = null;
        }
        finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }

        final long end = System.nanoTime();
        log.info(String.format("Written %.3f Mb in %d sec. to %s", (double) bytesWritten / (1024 * 1024), (end - start) / 1000000000, outputPath));

        return hdfsPath.toUri();
    }

    public void delete(final String outputPath, final boolean recursive) throws IOException
    {
        log.info(String.format("Deleting: %s (%srecursive)", outputPath, recursive ? "" : "non "));
        fileSystemAccess.get().delete(new Path(outputPath), recursive);
    }
}
