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

import com.ning.metrics.action.hdfs.data.Row;
import com.ning.metrics.action.hdfs.data.RowAccessException;
import com.ning.metrics.action.hdfs.data.RowFactory;
import com.ning.metrics.action.hdfs.data.Rows;
import com.ning.metrics.action.hdfs.data.schema.ColumnKey;
import com.ning.metrics.action.hdfs.data.schema.DynamicColumnKey;
import com.ning.metrics.action.hdfs.data.schema.RowSchema;
import com.ning.metrics.action.schema.Registrar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StringRowSerializer implements RowSerializer
{
    @Override
    public boolean accept(final Object o)
    {
        return (o instanceof String);
    }

    @Override
    public Rows toRows(final Registrar r, final Object value) throws RowAccessException
    {
        final String[] data = value.toString().split("\t");
        final List<ColumnKey> columnKeyList = new ArrayList<ColumnKey>();

        for (int i = 0; i < data.length; i++) {
            columnKeyList.add(new DynamicColumnKey(String.valueOf("col-" + i)));
        }

        final Row row = RowFactory.getRow(new RowSchema("Text", columnKeyList), Arrays.asList(data));

        final Rows rows = new Rows();
        rows.add(row);

        return rows;
    }
}
