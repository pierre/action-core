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

import com.ning.metrics.action.hdfs.data.Rows;
import com.ning.metrics.action.schema.Registrar;
import com.ning.metrics.serialization.smile.SmileEnvelopeEventDeserializer;

import java.io.IOException;
import java.io.InputStream;

public class BufferedSmileReader implements BufferedRowsReader
{
    private static final int DEFAULT_BUFFER_SIZE = 1000;

    private final Registrar registrar;
    private final SmileEnvelopeEventDeserializer deserializer;

    public BufferedSmileReader(final Registrar registrar, final InputStream stream) throws IOException
    {
        this.registrar = registrar;
        deserializer = new SmileEnvelopeEventDeserializer(stream, false);
    }

    @Override
    public Rows readNext()
    {
        final Rows rows = new Rows();

        int i = 0;
        while (deserializer.hasNextEvent() && i < DEFAULT_BUFFER_SIZE) {
            SmileRowSerializer.eventToRow(registrar, deserializer, rows);
            i++;
        }

        // Signal EOF
        if (rows.size() == 0) {
            return null;
        }
        else {
            return rows;
        }
    }
}
