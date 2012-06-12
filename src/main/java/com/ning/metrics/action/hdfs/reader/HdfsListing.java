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

import com.ning.metrics.action.hdfs.data.RowFileContentsIteratorFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Container for list of {@link HdfsEntry}.
 * In practice, this is a collection of files and directories.
 *
 * @see HdfsEntry
 */
public class HdfsListing
{
    private static final byte DELIMITER = (byte) ',';

    private final Path path;
    private final String parentPath;
    private final ImmutableList<HdfsEntry> entries;
    private final boolean recursive;
    private final boolean raw;
    private final RowFileContentsIteratorFactory rowFileContentsIteratorFactory;

    public static final String JSON_LISTING_PATH = "path";
    public static final String JSON_LISTING_PARENT_PATH = "parentPath";
    public static final String JSON_LISTING_ENTRIES = "entries";

    @JsonCreator
    @SuppressWarnings("unused")
    public HdfsListing(@JsonProperty(JSON_LISTING_PATH) String path,
                       @JsonProperty(JSON_LISTING_PARENT_PATH) String parentPath,
                       @JsonProperty(JSON_LISTING_ENTRIES) List<HdfsEntry> entries)
    {
        this.path = new Path(path);
        this.parentPath = parentPath;
        this.entries = ImmutableList.copyOf(entries);

        raw = true;
        recursive = false;
        rowFileContentsIteratorFactory = null;
    }

    public HdfsListing(FileSystem fileSystem, Path path, boolean raw, RowFileContentsIteratorFactory rowFileContentsIteratorFactory, boolean recursive) throws IOException
    {
        this.path = path;
        this.parentPath = "/".equals(path.toUri().toString()) ? null : path.getParent().toUri().toString();
        this.raw = raw;
        this.recursive = recursive;
        this.rowFileContentsIteratorFactory = rowFileContentsIteratorFactory;

        final ImmutableList.Builder<HdfsEntry> entriesBuilder = ImmutableList.builder();
        findEntries(fileSystem, path, entriesBuilder);
        this.entries = entriesBuilder.build();
    }

    private void findEntries(FileSystem fs, Path p, ImmutableList.Builder<HdfsEntry> entriesBuilder) throws IOException
    {
        final FileStatus[] fileStatuses = fs.listStatus(p);
        if (fileStatuses == null) {
            return;
        }

        for (final FileStatus s : fileStatuses) {
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

    @SuppressWarnings({"unchecked", "unused"})
    public void toJson(final OutputStream out, final boolean pretty) throws IOException
    {
        final String parentPath = getParentPath() == null ? "" : getParentPath();

        final JsonGenerator generator = new JsonFactory().createJsonGenerator(out);
        generator.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        if (pretty) {
            generator.setPrettyPrinter(new DefaultPrettyPrinter());
        }

        generator.writeStartObject();
        generator.writeObjectField(JSON_LISTING_PATH, getPath());
        generator.writeObjectField(JSON_LISTING_PARENT_PATH, parentPath);
        generator.writeArrayFieldStart(JSON_LISTING_ENTRIES);
        // Important: need to flush before appending pre-serialized events
        generator.flush();

        for (HdfsEntry entry : getEntries()) {
            entry.toJson(generator);
        }
        generator.writeEndArray();

        generator.writeEndObject();
        generator.close();
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
