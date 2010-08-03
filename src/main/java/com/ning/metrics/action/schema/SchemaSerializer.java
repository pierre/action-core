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

import com.ning.serialization.SchemaField;
import com.ning.serialization.SchemaFieldType;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONWriter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class SchemaSerializer
{
    private static final Logger log = Logger.getLogger(SchemaSerializer.class);

    private static ConcurrentHashMap<String, Lock> fileLocks = new ConcurrentHashMap<String, Lock>();

    private final String stateFilePath;

    SchemaSerializer(File stateFile) throws IOException
    {
        this.stateFilePath = stateFile.getCanonicalPath();

        File stateFileParent = stateFile.getParentFile();

        if (!stateFileParent.exists()) {
            if (!stateFileParent.mkdirs()) {
                throw new IOException(String.format("Unable to make directory %s", stateFileParent));
            }
        }
    }

    private Lock acquireLock()
    {
        Lock lock = new ReentrantLock();
        Lock result = fileLocks.putIfAbsent(stateFilePath, lock);

        if (result == null) {
            result = lock;
        }

        result.lock();

        return result;
    }

    private void releaseLock(Lock lock)
    {
        lock.unlock();
        fileLocks.remove(stateFilePath, lock);
    }

    void saveState(Map<String, Map<Short, SchemaField>> schemaCache) throws SchemaSerializerException
    {
        Lock lock = acquireLock();

        try {
            Writer writer = null;

            try {
                writer = new BufferedWriter(new FileWriter(stateFilePath));

                JSONWriter json = new JSONWriter(writer);

                json.object();
                json.key("version").value(1);
                json.key("schemas");
                json.object();

                for (Map.Entry<String, Map<Short, SchemaField>> entry : schemaCache.entrySet()) {
                    json.key(entry.getKey());
                    json.array();

                    for (SchemaField field : entry.getValue().values()) {
                        json.object();
                        json.key("id").value(field.getId());
                        json.key("name").value(field.getName());
                        json.key("type").value(field.getType().toString());
                        json.endObject();
                    }

                    json.endArray();
                }

                json.endObject();
                json.endObject();
            }
            catch (IOException e) {
                throw new SchemaSerializerException(e, "Failed to save state to %s", stateFilePath);
            }
            catch (JSONException e) {
                throw new SchemaSerializerException(e, "Failed to save state to %s", stateFilePath);
            }
            catch (RuntimeException e) {
                throw new SchemaSerializerException(e, "Failed to save state to %s", stateFilePath);
            }
            finally {
                try {
                    writer.close();
                }
                catch (IOException e) {
                    log.warn(String.format("Failed to close %s", stateFilePath), e);
                }
            }
        }
        finally {
            releaseLock(lock);
        }
    }

    Map<String, Map<Short, SchemaField>> loadState() throws SchemaSerializerException
    {
        Lock lock = acquireLock();

        try {
            Map<String, Map<Short, SchemaField>> result = new HashMap<String, Map<Short, SchemaField>>();

            BufferedReader reader = null;

            try {
                File file = new File(stateFilePath);

                if (!file.exists() || file.length() == 0) {
                    return Collections.emptyMap();
                }

                reader = new BufferedReader(new FileReader(file));

                JSONTokener tokener = new JSONTokener(reader);
                Object obj = tokener.nextValue();

                if (obj instanceof JSONObject) {
                    JSONObject state = (JSONObject) obj;
                    String version = state.getString("version");

                    if (!"1".equals(version)) {
                        throw new SchemaSerializerException("Incompatible version %s", version);
                    }

                    JSONObject schemas = state.getJSONObject("schemas");
                    Iterator<String> typeIterator = schemas.keys();

                    while (typeIterator.hasNext()) {
                        String type = typeIterator.next();
                        JSONArray fields = schemas.getJSONArray(type);
                        Map<Short, SchemaField> fieldMap = new HashMap<Short, SchemaField>();

                        for (int i = 0; i < fields.length(); ++i) {
                            JSONObject field = fields.getJSONObject(i);
                            short fieldId = (short) field.getInt("id");
                            String fieldName = field.getString("name");
                            String fieldType = field.getString("type");

                            fieldMap.put(fieldId, SchemaFieldType.createSchemaField(fieldName, fieldType, fieldId));
                        }

                        result.put(type, Collections.unmodifiableMap(fieldMap));
                    }
                }
                else {
                    throw new SchemaSerializerException("Expected serialized JSON object, but got %s", obj);
                }
            }
            catch (JSONException e) {
                throw new SchemaSerializerException(e, "Failed to load state from %s: %s", stateFilePath, e.getMessage());
            }
            catch (IOException e) {
                throw new SchemaSerializerException(e, "Failed to load state from %s: %s", stateFilePath, e.getMessage());
            }
            catch (RuntimeException e) {
                throw new SchemaSerializerException(e, "Failed to save state to %s: %s", stateFilePath, e.getMessage());
            }
            finally {
                if (reader != null) {
                    try {
                        reader.close();
                    }
                    catch (IOException e) {
                        log.warn(String.format("Failed to close %s", stateFilePath), e);
                    }
                }
            }

            return Collections.unmodifiableMap(result);
        }
        finally {
            releaseLock(lock);
        }
    }
}
