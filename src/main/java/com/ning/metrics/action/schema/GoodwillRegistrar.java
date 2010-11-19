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

package com.ning.metrics.action.schema;

import com.google.inject.Inject;
import com.ning.metrics.action.binder.config.ActionCoreConfig;
import com.ning.metrics.goodwill.access.GoodwillAccessor;
import com.ning.metrics.goodwill.access.GoodwillSchema;
import com.ning.metrics.goodwill.access.GoodwillSchemaField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class GoodwillRegistrar implements Registrar
{
    private final GoodwillAccessor goodwillAccessor;

    @Inject
    public GoodwillRegistrar(
        ActionCoreConfig config
    ) throws IOException, ExecutionException, InterruptedException
    {
        String host = config.getRegistrarHost();
        int port = config.getRegistrarPort();

        goodwillAccessor = new GoodwillAccessor(host, port);
    }

    @Override
    public String getCanonicalName(String type)
    {
        try {
            Future<GoodwillSchema> schemaFuture = goodwillAccessor.getSchema(type);
            // IOException
            if (schemaFuture == null) {
                return null;
            }

            GoodwillSchema goodwillSchema = schemaFuture.get();
            if (goodwillSchema != null) {
                return goodwillSchema.getName();
            }
            else {
                return null;
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException(String.format("Was interrupted while getting schema: %s", type), e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(String.format("Problem getting schema: %s", type), e);
        }
    }

    @Override
    public Collection<String> getAllTypes()
    {
        Collection<String> result = new ArrayList<String>();

        try {
            Future<List<GoodwillSchema>> schemataFuture = goodwillAccessor.getSchemata();
            // IOException
            if (schemataFuture == null) {
                return null;
            }

            List<GoodwillSchema> goodwillSchemata = schemataFuture.get();
            for (GoodwillSchema goodwillSchema : goodwillSchemata) {
                result.add(goodwillSchema.getName());
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Was interrupted while getting the list of schemata", e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Problem getting the list of schemata", e);
        }

        return result;
    }

    @Override
    public Map<Short, GoodwillSchemaField> getSchema(String type)
    {
        Map<Short, GoodwillSchemaField> result = new HashMap<Short, GoodwillSchemaField>();

        try {
            Future<GoodwillSchema> schemaFuture = goodwillAccessor.getSchema(type);
            // IOException
            if (schemaFuture == null) {
                return null;
            }

            GoodwillSchema goodwillSchema = schemaFuture.get();
            for (GoodwillSchemaField goodwillField : goodwillSchema.getSchema()) {
                result.put(goodwillField.getId(), goodwillField);
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException(String.format("Was interrupted while getting schema: %s", type), e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(String.format("Problem getting schema: %s", type), e);
        }

        return result;
    }
}