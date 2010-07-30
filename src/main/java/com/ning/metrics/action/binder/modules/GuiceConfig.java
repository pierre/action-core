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

package com.ning.metrics.action.binder.modules;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.ning.metrics.action.binder.config.ActionCoreConfig;
import com.ning.metrics.action.hdfs.data.RowFileContentsIteratorFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.skife.config.ConfigurationObjectFactory;

import java.util.HashMap;
import java.util.Map;

public class GuiceConfig extends GuiceServletContextListener
{
    @Override
    protected Injector getInjector()
    {
        /* Scan for Jersey endpoints */
        final Map<String, String> params = new HashMap<String, String>();
        params.put(PackagesResourceConfig.PROPERTY_PACKAGES, "com.ning.metrics.action.endpoint");

        return Guice.createInjector(
            Stage.PRODUCTION,
            new Module()
            {
                @Override
                public void configure(Binder binder)
                {
                    ActionCoreConfig config = new ConfigurationObjectFactory(System.getProperties()).build(ActionCoreConfig.class);
                    binder.bind(ActionCoreConfig.class).toInstance(config);
                    binder.bind(RowFileContentsIteratorFactory.class).asEagerSingleton();
                }
            },
            new ServletModule()
            {
                @Override
                protected void configureServlets()
                {
                    //serve("/*").with(GuiceContainer.class, params);
                    filter("/*").through(GuiceContainer.class, params);

                }
            }
        );

    }
}
