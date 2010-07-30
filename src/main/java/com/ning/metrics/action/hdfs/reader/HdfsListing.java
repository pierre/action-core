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

import com.google.common.collect.ImmutableList;
import com.ning.metrics.action.hdfs.data.RowFileContentsIteratorFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Container for list of {@link HdfsEntry}.
 *
 * @see HdfsEntry
 */
public class HdfsListing
{
    private final Path path;
    private final String parentPath;
    private final ImmutableList<HdfsEntry> entries;
    private final boolean recursive;
    private final boolean raw;
    private final FileSystem fs;
    private final RowFileContentsIteratorFactory rowFileContentsIteratorFactory;

    public HdfsListing(FileSystem fileSystem, Path path, boolean raw, RowFileContentsIteratorFactory rowFileContentsIteratorFactory, String type, boolean recursive) throws IOException
    {
        this.fs = fileSystem;
        this.path = path;
        this.parentPath = "/".equals(this.path) ? null : path.getParent().toUri().toString();
        this.raw = raw;
        this.recursive = recursive;
        this.rowFileContentsIteratorFactory = rowFileContentsIteratorFactory;

        final ImmutableList.Builder<HdfsEntry> entriesBuilder = ImmutableList.builder();
        findEntries(fileSystem, path, entriesBuilder);
        this.entries = entriesBuilder.build();
    }

    private void findEntries(FileSystem fs, Path p, ImmutableList.Builder<HdfsEntry> entriesBuilder) throws IOException
    {
        for (final FileStatus s : fs.listStatus(p)) {
            if (s.isDir() && recursive) {
                findEntries(fs, s.getPath(), entriesBuilder);
            }

            entriesBuilder.add(new HdfsEntry(fs, s, raw, rowFileContentsIteratorFactory));
        }
    }

    /**
     * Returns the path to this listing.
     *
     * @return path of this listing
     */
    public String getPath()
    {
        return path.toUri().getPath();
    }

    /**
     * Returns the directory containing this listing.
     *
     * @return parent path, or null if this is the root
     */
    public String getParentPath()
    {
        return parentPath;
    }

    /**
     * Returns a list of the child files and folders of this listing.
     *
     * @return list of child entries
     */
    public ImmutableList<HdfsEntry> getEntries()
    {
        return entries;
    }

    @Override
    public String toString()
    {
        return "HdfsListing{" +
            "path='" + path + '\'' +
            ", parentPath='" + parentPath + '\'' +
            ", entries=" + entries +
            '}';
    }
}