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

import org.apache.log4j.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Guice 'servlets' are not really servlets but degenerated filters. They get executed as part of the filter chain. However, jetty
 * only executes filters if an actual servlet is mapped at a given location (in web.xml). So this servlet gets mapped all over the
 * server and logs if a request actually hits the servlet (which should never happen because they are grabbed by the guice servlet
 * filterchain).
 */
public class LogInvalidResourcesServlet extends HttpServlet
{
    private static final long serialVersionUID = 1L;

    private static final Logger log = Logger.getLogger(LogInvalidResourcesServlet.class);

    @Override
    public void service(final HttpServletRequest req, final HttpServletResponse res) throws IOException
    {
        log.warn(String.format("Requested '%s' from '%s', which is an invalid resource.", req.getPathInfo(), req.getServletPath()));
        res.sendError(HttpServletResponse.SC_NOT_FOUND);
    }
}