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

import com.ning.metrics.action.hdfs.data.Row;
import com.ning.metrics.action.hdfs.data.RowAccessException;
import com.ning.metrics.action.hdfs.data.RowFactory;
import com.ning.metrics.action.hdfs.data.RowThrift;
import com.ning.metrics.action.hdfs.data.Rows;
import com.ning.metrics.action.hdfs.data.schema.ColumnKey;
import com.ning.metrics.action.hdfs.data.schema.DynamicColumnKey;
import com.ning.metrics.action.hdfs.data.schema.RowSchema;
import com.ning.metrics.action.schema.Registrar;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WritableRowSerializer implements RowSerializer
{
    @Override
    public boolean accept(Object o)
    {
        return (o instanceof Writable);
    }

    @Override
    public Rows toRows(Registrar r, Object value) throws RowAccessException
    {
        Row row;

        if (value instanceof Text) {
            String[] data = value.toString().split("\t");
            List<ColumnKey> columnKeyList = new ArrayList<ColumnKey>();

            for (int i = 0; i < data.length; i++) {
                columnKeyList.add(new DynamicColumnKey(String.valueOf("col-" + i)));
            }

            row = RowFactory.getRow(new RowSchema("Text", columnKeyList), Arrays.asList(data));
        }
        else if (value instanceof BytesWritable) {
            byte[] data = ((BytesWritable) value).getBytes();

            ArrayList<String> listData = new ArrayList<String>();
            listData.add(new String(data, Charset.forName("UTF-8")));

            row = RowFactory.getRow(new RowSchema("BytesWritable", new DynamicColumnKey(String.valueOf("col-1"))), listData);
        }
        else if (value instanceof RowThrift) {
            row = (RowThrift) value;
        }
        else {
            throw new RowAccessException(String.format("Writable [%s] is not a known row type", value == null ? null : value.getClass()));
        }

        Rows rows = new Rows();
        rows.add(row);

        return rows;
    }
}
