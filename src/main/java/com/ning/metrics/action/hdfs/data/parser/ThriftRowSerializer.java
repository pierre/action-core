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

import com.ning.metrics.action.hdfs.data.RowAccessException;
import com.ning.metrics.action.hdfs.data.RowFactory;
import com.ning.metrics.action.hdfs.data.Rows;
import com.ning.metrics.action.hdfs.data.schema.ColumnKey;
import com.ning.metrics.action.hdfs.data.schema.DynamicColumnKey;
import com.ning.metrics.action.hdfs.data.schema.RowSchema;
import com.ning.metrics.action.schema.Registrar;
import com.ning.metrics.goodwill.access.GoodwillSchemaField;
import com.ning.metrics.serialization.event.ThriftEnvelopeEvent;
import com.ning.metrics.serialization.thrift.ThriftEnvelope;
import com.ning.metrics.serialization.thrift.ThriftEnvelopeEventDeserializer;
import com.ning.metrics.serialization.thrift.ThriftField;
import com.ning.metrics.serialization.thrift.item.DataItem;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Given a file in raw Thrift format, extract ThriftEnvelopeEvents
 */
public class ThriftRowSerializer implements RowSerializer
{
    @Override
    public boolean accept(final Object o)
    {
        return (o instanceof InputStream);
    }

    @Override
    public Rows toRows(final Registrar r, final Object value) throws RowAccessException
    {
        final ThriftEnvelopeEventDeserializer deserializer;
        try {
            deserializer = new ThriftEnvelopeEventDeserializer((InputStream) value);
        }
        catch (IOException e) {
            throw new RowAccessException(e);
        }

        final Rows rows = new Rows();
        while (deserializer.hasNextEvent()) {
            eventToRow(r, deserializer, rows);
        }

        return rows;
    }

    public static void eventToRow(Registrar r, ThriftEnvelopeEventDeserializer deserializer, Rows rows)
    {
        final ThriftEnvelopeEvent event;
        try {
            event = deserializer.getNextEvent();
        }
        catch (IOException e) {
            throw new RowAccessException(e);
        }
        final ThriftEnvelope envelope = (ThriftEnvelope) event.getData();
        final List<ThriftField> fields = envelope.getPayload();

        final Map<Short, GoodwillSchemaField> schema = r.getSchema(event.getName());
        final List<ColumnKey> columnKeyList = new ArrayList<ColumnKey>(envelope.getPayload().size());
        final List<DataItem> data = new ArrayList<DataItem>(envelope.getPayload().size());

        // Without Goodwill integration, simply pass the values
        if (schema == null) {
            int i = 1;
            for (final ThriftField field : fields) {
                columnKeyList.add(new DynamicColumnKey(String.valueOf(i)));
                data.add(field.getDataItem());
                i++;
            }
        }
        else {
            // With Goodwill, select only the fields present in the Goodwill schema, and preserve ordering
            final Iterator<ThriftField> iterator = fields.iterator();
            for (final GoodwillSchemaField schemaField : schema.values()) {
                final String schemaFieldName = schemaField.getName();
                columnKeyList.add(new DynamicColumnKey(schemaFieldName));
                if (iterator.hasNext()) {
                    data.add(iterator.next().getDataItem());
                }
                else {
                    data.add(null);
                }
            }
        }

        rows.add(RowFactory.getRow(new RowSchema(event.getName(), columnKeyList), data));
    }
}
