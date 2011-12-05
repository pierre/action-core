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

package com.ning.metrics.action.schema;

import com.google.inject.Inject;
import com.ning.metrics.action.binder.config.ActionCoreConfig;
import com.ning.metrics.goodwill.access.CachingGoodwillAccessor;
import com.ning.metrics.goodwill.access.GoodwillSchema;
import com.ning.metrics.goodwill.access.GoodwillSchemaField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class GoodwillRegistrar implements Registrar
{
    private final CachingGoodwillAccessor goodwillAccessor;

    @Inject
    public GoodwillRegistrar(ActionCoreConfig config) throws IOException, ExecutionException, InterruptedException
    {
        String host = config.getRegistrarHost();
        int port = config.getRegistrarPort();

        goodwillAccessor = new CachingGoodwillAccessor(host, port);
    }

    @Override
    public String getCanonicalName(String type)
    {
        GoodwillSchema goodwillSchema = goodwillAccessor.getSchema(type);

        if (goodwillSchema != null) {
            return goodwillSchema.getName();
        }
        else {
            return null;
        }
    }

    @Override
    public Collection<String> getAllTypes()
    {
        Collection<String> result = new ArrayList<String>();

        List<GoodwillSchema> goodwillSchemata = goodwillAccessor.getSchemata();
        for (GoodwillSchema goodwillSchema : goodwillSchemata) {
            result.add(goodwillSchema.getName());
        }

        return result;
    }

    @Override
    public Map<Short, GoodwillSchemaField> getSchema(String type)
    {
        // Make sure to use a LinkedHashMap to preserve ordering
        Map<Short, GoodwillSchemaField> result = new LinkedHashMap<Short, GoodwillSchemaField>();

        GoodwillSchema goodwillSchema = goodwillAccessor.getSchema(type);
        // Schema not found
        if (goodwillSchema == null) {
            return null;
        }

        for (GoodwillSchemaField goodwillField : goodwillSchema.getSchema()) {
            result.put(goodwillField.getId(), goodwillField);
        }

        return result;
    }
}
