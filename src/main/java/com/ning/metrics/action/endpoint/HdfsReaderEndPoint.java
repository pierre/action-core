package com.ning.metrics.action.endpoint;

import com.google.inject.Inject;
import com.ning.metrics.action.binder.config.ActionCoreConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;

public class HdfsReaderEndPoint
{
    private static final Logger log = Logger.getLogger(HdfsReaderEndPoint.class);

    private final FileSystem fileSystem;

    @Inject
    public HdfsReaderEndPoint(ActionCoreConfig config) throws IOException
    {
        Configuration conf = new Configuration();

        conf.set("fs.default.name", config.getNamenodeUrl());
        conf.set("hadoop.job.ugi", config.getHadoopUgi());

        this.fileSystem = FileSystem.get(conf);

        log.info("Connected successfully to HDFS!");
    }

    public HdfsListing get(String dir, boolean recursive)
    {
        if (dir == null) {
            throw new IllegalArgumentException("Request missing dir parameter");
        }
        else {
            Path dirPath = new Path(dir);

            try {
                log.debug(String.format("Crawling path [%s], recursive=[%s]", dirPath, recursive));
                return new HdfsListing(fileSystem, dirPath, recursive);
            }
            catch (IOException e) {
                throw new WebApplicationException(e);
            }
        }
    }
}
