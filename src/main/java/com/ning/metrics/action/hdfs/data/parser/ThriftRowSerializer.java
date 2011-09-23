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
import com.ning.metrics.action.hdfs.data.Rows;
import com.ning.metrics.action.schema.Registrar;
import com.ning.metrics.serialization.event.ThriftEnvelopeEvent;
import com.ning.metrics.serialization.thrift.ThriftEnvelope;
import com.ning.metrics.serialization.thrift.ThriftEnvelopeEventDeserializer;

import java.io.IOException;
import java.io.InputStream;

/**
 * Given a file in raw Thrift format, extract ThriftEnvelopeEvents
 */
public class ThriftRowSerializer implements RowSerializer
{
    private final ThriftEnvelopeRowSerializer envelopeRowSerializer = new ThriftEnvelopeRowSerializer();

    @Override
    public boolean accept(final Object o)
    {
        return (o instanceof InputStream);
    }

    @Override
    public Rows toRows(final Registrar r, final Object value) throws RowAccessException
    {
        final ThriftEnvelopeEventDeserializer deserializer = new ThriftEnvelopeEventDeserializer((InputStream) value);

        final Rows rows = new Rows();
        while (deserializer.hasNextEvent()) {
            eventToRow(r, deserializer, rows);
        }

        return rows;
    }

    public void eventToRow(Registrar r, ThriftEnvelopeEventDeserializer deserializer, Rows rows)
    {
        final ThriftEnvelopeEvent event;
        try {
            event = deserializer.getNextEvent();
        }
        catch (IOException e) {
            throw new RowAccessException(e);
        }

        final ThriftEnvelope envelope = (ThriftEnvelope) event.getData();
        rows.addAll(envelopeRowSerializer.toRows(r, envelope));
    }
}
