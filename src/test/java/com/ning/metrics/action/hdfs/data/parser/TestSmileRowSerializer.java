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

import com.ning.metrics.action.hdfs.data.RowSmile;
import com.ning.metrics.action.hdfs.data.Rows;
import com.ning.metrics.action.schema.Registrar;
import com.ning.metrics.goodwill.access.GoodwillSchemaField;
import com.ning.metrics.serialization.event.SmileEnvelopeEvent;
import com.ning.metrics.serialization.smile.SmileEnvelopeEventSerializer;

import com.fasterxml.jackson.databind.node.ValueNode;
import com.google.common.collect.ImmutableMap;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestSmileRowSerializer
{
    private class SomeRegistrar implements Registrar
    {
        @Override
        public String getCanonicalName(String type)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<String> getAllTypes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Short, GoodwillSchemaField> getSchema(String type)
        {
            final Map<Short, GoodwillSchemaField> map = new LinkedHashMap<Short, GoodwillSchemaField>();

            map.put((short) 1, new GoodwillSchemaField("eventDate", "date", (short) 1, "Something", null, null, null, null));
            map.put((short) 2, new GoodwillSchemaField("field1", "string", (short) 2, "Something", null, null, null, null));
            map.put((short) 3, new GoodwillSchemaField("field2", "string", (short) 3, "Something too", null, null, null, null));

            return map;
        }
    }

    private class NullRegistrar implements Registrar
    {
        @Override
        public String getCanonicalName(String type)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<String> getAllTypes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Short, GoodwillSchemaField> getSchema(String type)
        {
            return null;
        }
    }

    @Test(groups = "fast")
    public void testToRowsOrderingWithoutRegistrar() throws Exception
    {
        final Map<String, Object> eventMap = createEventMap();
        final ByteArrayOutputStream out = createSmileEnvelopePayload(eventMap);
        final InputStream stream = new ByteArrayInputStream(out.toByteArray());

        final SmileRowSerializer serializer = new SmileRowSerializer();
        final Rows rows = serializer.toRows(new NullRegistrar(), stream);
        final RowSmile firstRow = (RowSmile) rows.iterator().next();

        final ImmutableMap<String, ValueNode> actual = firstRow.toMap();
        // Without the registrar, verify we output all fields, including the metadata ones (eventDate, eventGranularity)
        Assert.assertEquals(actual.keySet().size(), eventMap.keySet().size() + 2);
        Assert.assertNotNull(actual.get("eventDate"));
        Assert.assertEquals(actual.get("eventGranularity"), "HOURLY");
        Assert.assertEquals(actual.get("field1"), eventMap.get("field1"));
        Assert.assertEquals(actual.get("field2"), eventMap.get("field2"));
    }

    @Test(groups = "fast")
    public void testToRowsOrderingWithRegistrar() throws Exception
    {
        final Map<String, Object> eventMap = createEventMap();
        final ByteArrayOutputStream out = createSmileEnvelopePayload(eventMap);
        final InputStream stream = new ByteArrayInputStream(out.toByteArray());

        final SmileRowSerializer serializer = new SmileRowSerializer();
        final Rows rows = serializer.toRows(new SomeRegistrar(), stream);
        final RowSmile firstRow = (RowSmile) rows.iterator().next();

        final ImmutableMap<String, ValueNode> actual = firstRow.toMap();
        // With the registrar, only the fields in the schema are outputted
        Assert.assertEquals(actual.keySet().size(), eventMap.keySet().size() + 1);
        Assert.assertNotNull(actual.get("eventDate"));
        Assert.assertEquals(actual.get("field1"), eventMap.get("field1"));
        Assert.assertEquals(actual.get("field2"), eventMap.get("field2"));
    }

    private ByteArrayOutputStream createSmileEnvelopePayload(final Map<String, Object> map) throws IOException
    {
        final SmileEnvelopeEventSerializer smileSerializer = new SmileEnvelopeEventSerializer(false);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        smileSerializer.open(out);
        final SmileEnvelopeEvent event = new SmileEnvelopeEvent("myEvent", new DateTime(), map);
        smileSerializer.serialize(event);
        smileSerializer.close();

        return out;
    }

    private Map<String, Object> createEventMap()
    {
        final Map<String, Object> map = new LinkedHashMap<String, Object>();

        map.put("field2", "hello");
        map.put("field1", "world");

        return map;
    }
}
