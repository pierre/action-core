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
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.ning.metrics.action.binder.config.ActionCoreConfig;
import com.ning.serialization.SchemaField;
import com.ning.serialization.SchemaFieldType;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

public class GoodwillRegistrar implements Registrar
{
    private static final Logger log = Logger.getLogger(GoodwillRegistrar.class);

    private volatile Map<String, String> canonicalMap = new HashMap<String, String>();
    private final ConcurrentMap<String, Map<Short, SchemaField>> schemaCache = new ConcurrentHashMap<String, Map<Short, SchemaField>>();
    private final String host;
    private final int port;
    private final AsyncHttpClient client;
    private final SchemaSerializer schemaSerializer;

    @Inject
    public GoodwillRegistrar(
        ActionCoreConfig config
    ) throws IOException, ExecutionException, InterruptedException
    {
        this.host = config.getRegistrarHost();
        this.port = config.getRegistrarPort();
        File stateFile = new File(config.getRegistrarStateFile());
        this.client = new AsyncHttpClient();
        this.schemaSerializer = new SchemaSerializer(stateFile);

        try {
            this.schemaCache.putAll(this.schemaSerializer.loadState());
        }
        catch (SchemaSerializerException e) {
            log.warn(String.format("Failed to load state from %s", stateFile), e);
        }

        for (String type : schemaCache.keySet()) {
            canonicalMap.put(type.toLowerCase(), type);
        }
    }

    public String getCanonicalName(String type)
    {
        if (!updateCanonicalMap()) {
            log.info(String.format("Unable to update canonical map. Schema for type %s may be stale", type));
        }

        return canonicalMap.get(type.toLowerCase());
    }

    public Collection<String> getAllTypes()
    {
        updateCanonicalMap();

        return Collections.unmodifiableCollection(canonicalMap.values());
    }

    private boolean updateCanonicalMap()
    {
        boolean updatedSuccessfully = false;

        try {
            String url = String.format("http://%s:%d/goodwill/registrar", host, port);

            final Map<String, String> typeMap = new HashMap<String, String>();

            client.prepareGet(url).addHeader("Accept", "application/json").execute(new AsyncCompletionHandler<Response>()
            {

                @Override
                public Response onCompleted(Response response) throws Exception
                {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(response.getResponseBodyAsStream()));
                    ObjectMapper mapper = new ObjectMapper();
                    for (Object o : mapper.readValue(reader, ArrayList.class)) {
                        try {
                            HashMap type = (HashMap) o;
                            String typeName = (String) type.get("name");
                            typeMap.put(typeName.toLowerCase(), typeName);
                        }
                        catch (RuntimeException e) {
                            log.warn("Unexpected exception while communicating with the Goodwill server", e);
                        }
                    }
                    reader.close();

                    return response;
                }

                @Override
                public void onThrowable(Throwable t)
                {
                    log.warn(t);
                }
            });

            if (!typeMap.isEmpty()) {
                canonicalMap = typeMap;
                updatedSuccessfully = true;
            }
        }
        catch (IOException e) {
            log.warn(String.format("Error updating canonical map from %s:%d", host, port));
        }
        finally {
            if (!updatedSuccessfully) {
                log.info("unable to update canonical map");
            }
        }

        return updatedSuccessfully;
    }

    public Map<Short, SchemaField> getSchema(String type)
    {
        String url = String.format("http://%s:%d/goodwill/registrar/%s", host, port, type);
        String canonicalType = canonicalMap.get(type.toLowerCase());

        if (canonicalType == null) {
            updateCanonicalMap();
            canonicalType = canonicalMap.get(type.toLowerCase());

            if (canonicalType == null) {
                log.info(String.format("unable to get canonical type for %s", type));
                canonicalType = type;
            }
        }

        try {
            final Map<Short, SchemaField> schemaFieldMap = new LinkedHashMap<Short, SchemaField>();

            client.prepareGet(url).addHeader("Accept", "application/json").execute(new AsyncCompletionHandler<Response>()
            {
                @Override
                public Response onCompleted(Response response) throws Exception
                {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(response.getResponseBodyAsStream()));
                    ObjectMapper mapper = new ObjectMapper();
                    HashMap map = mapper.readValue(reader, HashMap.class);
                    try {
                        for (Object o : (ArrayList) map.get("schema")) {
                            HashMap field = (HashMap) o;
                            SchemaField schemaField = SchemaFieldType.createSchemaField((String) field.get("name"), (String) field.get("type"), ((Integer) field.get("position")).shortValue());
                            schemaFieldMap.put((short) schemaField.getId(), schemaField);

                        }
                    }
                    catch (RuntimeException e) {
                        log.warn("Unexpected exception while communicating with the Goodwill server", e);
                    }

                    reader.close();

                    return response;
                }

                @Override
                public void onThrowable(Throwable t)
                {
                    log.warn(t);
                }
            });

            if (!schemaFieldMap.isEmpty()) {
                schemaCache.put(canonicalType, Collections.unmodifiableMap(schemaFieldMap));

                if (schemaSerializer != null) {
                    try {
                        schemaSerializer.saveState(schemaCache);
                    }
                    catch (SchemaSerializerException e) {
                        log.warn(String.format("Failed to save state to %s", schemaSerializer), e);
                    }
                }
            }
        }
        catch (IOException e) {
            log.warn(String.format("Unable to contact type registrar at host %s, port %d.  Using cached schema for type %s", host, port, type), e);
        }

        Map<Short, SchemaField> result = schemaCache.get(canonicalType);

        return result == null ? null : Collections.unmodifiableMap(result);
    }
}