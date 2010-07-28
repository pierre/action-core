package com.ning.metrics.action.endpoint;

import com.google.inject.Inject;
import com.sun.jersey.api.view.Viewable;
import org.apache.log4j.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
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

    /*
     * UI
     */

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Viewable getAll(
        @QueryParam("dir") String dir,
        @QueryParam("type") String type,
        @QueryParam("raw") boolean raw,
        @QueryParam("recursive") boolean recursive
    ) throws IOException
    {
        log.debug(String.format("Got request for dir=[%s], type=[%s], raw=[%s] and recursive=[%s]", dir, type, raw, recursive));

        if (dir == null) {
            dir = "/";
        }

        return new Viewable("/registrar/type.jsp", hdfsReader.get(dir, recursive));
    }
}

