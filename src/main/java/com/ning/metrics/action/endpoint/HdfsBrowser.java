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
import com.ning.metrics.action.hdfs.reader.HdfsListing;
import com.ning.metrics.action.hdfs.reader.HdfsReaderEndPoint;
import com.sun.jersey.api.view.Viewable;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.LinkedHashMap;

@Path("/rest/1.0")
public class HdfsBrowser
{
    private HdfsReaderEndPoint hdfsReader;
    private Logger log = Logger.getLogger(HdfsBrowser.class);

    @Inject
    public HdfsBrowser(
        HdfsReaderEndPoint store
    )
    {
        this.hdfsReader = store;
    }

    /**
     * Build a Viewable to browse HDFS.
     *
     * @param path      path in HDFS to render (directory listing or file), defaults to /
     * @param range     optional, bucket of lines to read (e.g. 200-250) (used in the content.jsp only)
     * @param raw       optional, whether to try to deserialize
     * @param recursive optional, whether to crawl all files under a directory
     * @return Viewable to render the jsp
     * @throws IOException HDFS crawling error
     */
    @GET
    @Path("/hdfs")
    @Produces({"text/html", "text/plain"})
    public Viewable getListing(
        @QueryParam("path") String path,
        @QueryParam("range") String range,
        @QueryParam("raw") boolean raw,
        @QueryParam("recursive") boolean recursive
    ) throws IOException
    {
        log.debug(String.format("Got request for path=[%s], raw=[%s] and recursive=[%s]", path, raw, recursive));

        if (path == null) {
            path = "/";
        }

        if (hdfsReader.isDir(path) && !recursive) {
            return new Viewable("/rest/listing.jsp", hdfsReader.getListing(path));
        }
        else {
            if (raw) {
                return new Viewable("/rest/contentRaw.jsp", hdfsReader.getListing(path, raw, recursive));
            }
            else {
                return new Viewable("/rest/content.jsp", hdfsReader.getListing(path, raw, recursive));
            }
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/json")
    public Response listingToJson(
        @QueryParam("path") String path,
        @QueryParam("recursive") boolean recursive,
        @QueryParam("pretty") boolean pretty
    ) throws IOException
    {
        final HdfsListing hdfsListing = hdfsReader.getListing(path, true, recursive);

        if (pretty) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            mapper.writeValue(out, hdfsListing.toMap());

            return Response.ok().entity(new String(out.toByteArray())).build();
        }

        return Response.ok().entity(hdfsListing).build();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/text")
    public Viewable dirToJson(
        @QueryParam("path") String path,
        @QueryParam("recursive") boolean recursive
    ) throws IOException
    {
        return new Viewable("/rest/contentRaw.jsp", hdfsReader.getListing(path, true, recursive));
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/viewer")
    public Response prettyPrintOneLine(
        @QueryParam("object") String object) throws IOException
    {
        // TODO: retrieve the goodwill schema to output properly the json payload (respect integers, ...)

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectMapper mapper = new ObjectMapper();

        String objectURIDecoded = URLDecoder.decode(object, "UTF-8");
        byte[] objectBase64Decoded = Base64.decodeBase64(objectURIDecoded.getBytes());

        mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        LinkedHashMap map = mapper.readValue(new String(objectBase64Decoded), LinkedHashMap.class);

        // We need to re-serialize the json (pretty print works only on serialization)
        mapper.writeValue(out, map);

        return Response.ok().entity(new String(out.toByteArray())).build();
    }
}
