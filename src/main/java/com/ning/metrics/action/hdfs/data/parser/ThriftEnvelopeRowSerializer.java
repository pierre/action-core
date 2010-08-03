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

package com.ning.metrics.action.hdfs.data.parser;

import com.ning.metrics.action.hdfs.TextSchema;
import com.ning.metrics.action.hdfs.data.Row;
import com.ning.metrics.action.hdfs.data.RowAccessException;
import com.ning.metrics.action.hdfs.data.key.ColumnKey;
import com.ning.metrics.action.hdfs.data.key.DynamicColumnKey;
import com.ning.metrics.action.schema.Registrar;
import com.ning.serialization.SchemaField;
import com.ning.serialization.ThriftEnvelope;
import com.ning.serialization.ThriftField;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ThriftEnvelopeRowSerializer implements RowSerializer
{
    @Override
    public boolean accept(Object o)
    {
        return (o instanceof ThriftEnvelope);
    }

    @Override
    public Row toRow(Registrar r, Object value) throws RowAccessException
    {
        ThriftEnvelope envelope = (ThriftEnvelope) value;

        List<ThriftField> payload = envelope.getPayload();
        List<Writable> data = new ArrayList<Writable>(payload.size());
        List<ColumnKey> columnKeyList = new ArrayList<ColumnKey>(payload.size());


        Map<Short, SchemaField> schema = r.getSchema(envelope.getTypeName());

        for (ThriftField field : payload) {
            SchemaField schemaField = null;
            if (schema != null) {
                schemaField = schema.get(field.getId());
            }

            if (schemaField == null) {
                columnKeyList.add(new DynamicColumnKey(String.format("%d", field.getId())));
            }
            else {
                columnKeyList.add(new DynamicColumnKey(schemaField.getName()));

            }
            data.add(field.getDataItem());
        }

        return new Row(new TextSchema(envelope.getTypeName(), columnKeyList), data);
    }
}
