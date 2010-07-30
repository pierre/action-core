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
import com.ning.serialization.DataItem;
import com.ning.serialization.ThriftEnvelope;
import com.ning.serialization.ThriftField;

import java.util.ArrayList;
import java.util.List;

public class ThriftEnvelopeRowSerializer implements RowSerializer
{
    @Override
    public boolean accept(Object o)
    {
        return (o instanceof ThriftEnvelope);
    }

    @Override
    public Row toRow(Object value) throws RowAccessException
    {
        ThriftEnvelope envelope = (ThriftEnvelope) value;

        List<ThriftField> payload = envelope.getPayload();
        List<DataItem> data = new ArrayList<DataItem>(payload.size());
        List<ColumnKey> columnKeyList = new ArrayList<ColumnKey>(payload.size());

        for (ThriftField aPayload : payload) {
            columnKeyList.add(new DynamicColumnKey(String.format("%d", aPayload.getId())));
            data.add(aPayload.getDataItem());
        }

        return new Row(new TextSchema(envelope.getTypeName(), columnKeyList), data);
    }
}
