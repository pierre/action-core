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

package com.ning.metrics.action.endpoint;

import com.google.inject.Inject;
import com.ning.metrics.action.hdfs.reader.HdfsReaderEndPoint;
import com.sun.jersey.api.view.Viewable;
import org.apache.log4j.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import java.io.IOException;

@Path("action")
public class Action
{
    private HdfsReaderEndPoint hdfsReader;
    private org.apache.log4j.Logger log = Logger.getLogger(Action.class);

    @Inject
    public Action(
        HdfsReaderEndPoint store
    )
    {
        this.hdfsReader = store;
    }

    /**
     * Build a Viewable to browse HDFS.
     *
     * @param path      path in HDFS to render (directory listing or file), defaults to /
     * @param type      optional, serialization type (avrojson, avrodata, thrift, text)
     * @param range     optional, bucket of lines to read (e.g. 200-250) (used in the content.jsp only)
     * @param raw       optional, whether to try to deserialize
     * @param recursive optional, whether to crawl all files under a directory
     * @return Viewable to render the jsp
     * @throws IOException HDFS crawling error
     */
    @GET
    @Produces({"text/html", "text/plain"})
    public Viewable getContent(
        @QueryParam("path") String path,
        @QueryParam("type") String type,
        @QueryParam("range") String range,
        @QueryParam("raw") boolean raw,
        @QueryParam("recursive") boolean recursive
    ) throws IOException
    {
        log.debug(String.format("Got request for path=[%s], type=[%s], raw=[%s] and recursive=[%s]", path, type, raw, recursive));

        if (path == null) {
            path = "/";
        }

        if (hdfsReader.isDir(path) && !recursive) {
            return new Viewable("/hdfs/listing.jsp", hdfsReader.getListing(path));
        }
        else {
            if (raw) {
                return new Viewable("/hdfs/contentRaw.jsp", hdfsReader.getListing(path, type, raw, recursive));
            }
            else {
                return new Viewable("/hdfs/content.jsp", hdfsReader.getListing(path, type, raw, recursive));
            }
        }
    }
}

