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

package com.ning.metrics.action.binder.modules;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.servlet.ServletModule;
import com.ning.metrics.action.binder.ActionCoreContainer;
import com.ning.metrics.action.binder.config.ActionCoreConfig;
import com.ning.metrics.action.hdfs.data.RowFileContentsIteratorFactory;
import com.ning.metrics.action.schema.GoodwillRegistrar;
import com.ning.metrics.action.schema.Registrar;
import com.sun.jersey.api.core.PackagesResourceConfig;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.skife.config.ConfigurationObjectFactory;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;

public class ActionCoreServerModule extends ServletModule
{
    @Override
    protected void configureServlets()
    {
        install(new Module()
        {
            @Override
            public void configure(final Binder binder)
            {
                final ActionCoreConfig config = new ConfigurationObjectFactory(System.getProperties()).build(ActionCoreConfig.class);
                binder.bind(ActionCoreConfig.class).toInstance(config);

                binder.bind(MBeanServer.class).toInstance(ManagementFactory.getPlatformMBeanServer());

                binder.bind(RowFileContentsIteratorFactory.class).asEagerSingleton();
                binder.bind(Registrar.class).to(GoodwillRegistrar.class).asEagerSingleton();

                // Plug-in Jersey
                bind(JacksonJsonProvider.class).asEagerSingleton();
            }
        });

        install(new HdfsModule());

        // TODO: add these filters
        // ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS, GZIPContentEncodingFilter.class.getName(),
        // ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS, GZIPContentEncodingFilter.class.getName()
        // For some reason, they trigger:
        // com.sun.jersey.api.container.ContainerException: java.lang.IllegalStateException: Committed
        //	at com.sun.jersey.server.impl.container.servlet.JSPTemplateProcessor.writeTo(JSPTemplateProcessor.java:124)
        //	at com.sun.jersey.server.impl.container.servlet.JSPTemplateProcessor.writeTo(JSPTemplateProcessor.java:60)
        filter("/*").through(ActionCoreContainer.class, ImmutableMap.of(
            PackagesResourceConfig.PROPERTY_PACKAGES, "com.ning.metrics.action.endpoint"
        ));
    }
}
