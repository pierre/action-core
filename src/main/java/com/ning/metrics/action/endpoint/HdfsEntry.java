package com.ning.metrics.action.endpoint;

import org.apache.hadoop.fs.FileStatus;
import org.joda.time.DateTime;

import java.io.IOException;

/**
 * Summarizes information about HDFS files and directories in a Hadoop-agnostic way.
 */
public class HdfsEntry
{
    private final String path;
    private final long size;
    private final long replicatedSize;
    private final DateTime modificationDate;
    private final boolean directory;

    public HdfsEntry(FileStatus status) throws IOException
    {
        this.path = status.getPath().toUri().getPath();
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
    }

    private HdfsEntry(String path, long size, long replicatedSize, DateTime modificationDate, boolean directory)
    {
        this.path = path;
        this.size = size;
        this.replicatedSize = replicatedSize;
        this.modificationDate = modificationDate;
        this.directory = directory;
    }

    public String getPath()
    {
        return path;
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

    public String getContent()
    {
        return "TODO";
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
