package com.ning.metrics.action.endpoint;

import com.google.common.collect.ImmutableList;
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
    private final String path;
    private final String parentPath;
    private final ImmutableList<HdfsEntry> entries;
    private final boolean recursive;

    public HdfsListing(final FileSystem fileSystem, Path path, boolean recursive) throws IOException
    {
        this.path = path.toUri().getPath();
        this.parentPath = "/".equals(this.path) ? null : path.getParent().toUri().toString();
        this.recursive = recursive;

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

            entriesBuilder.add(new HdfsEntry(s));
        }
    }

    /**
     * Returns the path to this listing.
     *
     * @return path of this listing
     */
    public String getPath()
    {
        return path;
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