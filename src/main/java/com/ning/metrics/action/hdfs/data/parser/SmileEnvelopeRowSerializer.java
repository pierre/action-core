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

package com.ning.metrics.action.hdfs.data.parser;

import com.ning.metrics.action.hdfs.data.JsonNodeComparable;
import com.ning.metrics.action.hdfs.data.RowAccessException;
import com.ning.metrics.action.hdfs.data.RowFactory;
import com.ning.metrics.action.hdfs.data.Rows;
import com.ning.metrics.action.hdfs.data.schema.ColumnKey;
import com.ning.metrics.action.hdfs.data.schema.DynamicColumnKey;
import com.ning.metrics.action.hdfs.data.schema.RowSchema;
import com.ning.metrics.action.schema.Registrar;
import com.ning.metrics.goodwill.access.GoodwillSchemaField;
import com.ning.metrics.serialization.smile.SmileBucket;
import com.ning.metrics.serialization.smile.SmileBucketDeserializer;
import com.ning.metrics.serialization.smile.SmileOutputStream;
import org.codehaus.jackson.JsonNode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SmileEnvelopeRowSerializer implements RowSerializer
{
    @Override
    public boolean accept(Object o)
    {
        // Objects are not SmileEnvelope!
        // The client send SmileEnvelope via the eventtracker library, however we buffer
        // Smile events under the cover to leverage Smile back-references.
        return (o instanceof SmileOutputStream);
    }

    @Override
    public Rows toRows(Registrar r, Object value) throws RowAccessException
    {
        SmileOutputStream stream = (SmileOutputStream) value;

        SmileBucket bucket;
        try {
            bucket = SmileBucketDeserializer.deserialize(new ByteArrayInputStream(stream.toByteArray()));
        }
        catch (IOException e) {
            throw new RowAccessException(e);
        }

        Rows rows = new Rows();

        List<JsonNodeComparable> data;
        List<ColumnKey> columnKeyList;
        // Theoretically, all events in the stream are of the same type
        for (JsonNode node : bucket) {
            data = new ArrayList<JsonNodeComparable>(node.size());
            columnKeyList = new ArrayList<ColumnKey>(node.size());

            Map<Short, GoodwillSchemaField> schema = r.getSchema(stream.getTypeName());

            // TODO Fragile - need field IDs
            int i = 0;
            Iterator it = node.getElements();
            while (it.hasNext()) {
                i++;
                JsonNode nodeField = (JsonNode) it.next();

                GoodwillSchemaField schemaField = null;
                if (schema != null) {
                    schemaField = schema.get(i);
                }

                if (schemaField == null) {
                    columnKeyList.add(new DynamicColumnKey(String.format("%d", i)));
                }
                else {
                    columnKeyList.add(new DynamicColumnKey(schemaField.getName()));

                }

                data.add(new JsonNodeComparable(nodeField));
            }

            rows.add(RowFactory.getRow(new RowSchema(stream.getTypeName(), columnKeyList), data));
        }

        return rows;
    }
}
