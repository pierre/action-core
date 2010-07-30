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
import com.ning.serialization.StringDataItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;

public class WritableRowSerializer implements RowSerializer
{
    @Override
    public boolean accept(Object o)
    {
        return (o instanceof Writable);
    }

    @Override
    public Row toRow(Object value) throws RowAccessException
    {
        Row row;

        if (value instanceof Text) {
            String[] data = value.toString().split("\t");
            List<ColumnKey> columnKeyList = new ArrayList<ColumnKey>();
            DataItem[] items = new DataItem[data.length];

            for (int i = 0; i < data.length; i++) {
                columnKeyList.add(new DynamicColumnKey(String.valueOf("col-" + i)));
                items[i] = new StringDataItem(data[i]);
            }

            row = new Row(new TextSchema("Text", columnKeyList), items);
        }
        else if (value instanceof Row) {
            row = (Row) value;
        }
        else {
            throw new RowAccessException(String.format("Writable [%s] is not a known row type", value == null ? null : value.getClass()));
        }

        return row;
    }
}
