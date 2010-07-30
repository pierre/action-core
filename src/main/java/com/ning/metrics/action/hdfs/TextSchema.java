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

package com.ning.metrics.action.hdfs;

import com.ning.metrics.action.hdfs.data.RowAccessException;
import com.ning.metrics.action.hdfs.data.key.ColumnKey;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextSchema implements WritableComparable
{
    private final Map<String, Integer> columnMap = new HashMap<String, Integer>();
    private final Map<String, Integer> newColumnMap = new HashMap<String, Integer>();
    private final String name;

    public TextSchema(String name, List<ColumnKey> keyList)
    {
        int pos = 0;

        for (ColumnKey col : keyList) {
            columnMap.put(col.getKeyName(), pos++);
        }

        this.name = name;
    }

    public TextSchema(String name, ColumnKey... keys)
    {
        this(name, Arrays.asList(keys));
    }

    public TextSchema(String name)
    {
        this(name, new ArrayList<ColumnKey>());
    }

    public TextSchema(String name, TextSchema schema)
    {
        this.name = name;
        //intentionally does not copy newColumnMap
        this.columnMap.putAll(schema.columnMap);
    }

    public int getColNum(ColumnKey key) throws RowAccessException
    {
        Integer pos = columnMap.get(key.getKeyName());

        if (pos == null) {
            pos = newColumnMap.get(key.getKeyName());
        }

        if (pos == null) {
            throw new RowAccessException("unable to map column " + key.getKeyName());
        }

        return pos;
    }

    public boolean hasColumnKey(ColumnKey key)
    {
        return columnMap.containsKey(key.getKeyName()) || newColumnMap.containsKey(key.getKeyName());
    }

    public void addCol(ColumnKey key, int pos)
    {
        if (columnMap.containsKey(key.getKeyName()) || newColumnMap.put(key.getKeyName(), pos) != null) {
            throw new IllegalArgumentException("cannot add duplicate key: " + key);
        }
    }

    public int getNumBaseCols()
    {
        return columnMap.size();
    }

    public void write(DataOutput out) throws IOException
    {
        writeMap(out, columnMap);
        writeMap(out, newColumnMap);
    }

    private void writeMap(DataOutput out, Map<String, Integer> map) throws IOException
    {
        out.writeInt(map.size());

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException
    {
        readMap(in, columnMap);
        readMap(in, newColumnMap);
    }

    private void readMap(DataInput in, Map<String, Integer> theMap) throws IOException
    {
        theMap.clear();

        int numEntries = in.readInt();

        for (int i = 0; i < numEntries; i++) {
            theMap.put(in.readUTF(), in.readInt());
        }
    }

    public String getName()
    {
        return name;
    }

    public int compareTo(Object o)
    {
        TextSchema thing = (TextSchema) o;
        int diff = Integer.signum(hashCode() - thing.hashCode());

        if (diff == 0 && !equals(o)) {
            return 1;
        }

        return diff;
    }

    public int hashCode()
    {
        return columnMap.hashCode() ^ newColumnMap.hashCode();
    }

    public boolean equals(Object o)
    {
        return o == this || o instanceof TextSchema &&
            columnMap.equals(((TextSchema) o).columnMap) &&
            newColumnMap.equals(((TextSchema) o).newColumnMap);
    }

    public String toString()
    {
        return String.format("base map: %s, addendum map: %s", columnMap.toString(), newColumnMap.toString());
    }
}
