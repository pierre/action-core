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

package com.ning.metrics.action.hdfs.data;

import com.ning.metrics.action.hdfs.data.schema.ColumnKey;
import com.ning.metrics.action.hdfs.data.schema.RowSchema;
import com.ning.metrics.action.hdfs.data.transformer.ColumnKeyTransformer;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

/**
 * Abstraction layer for a line in a file in HDFS.
 *
 * @see com.ning.metrics.action.hdfs.data.schema.RowSchema
 * @see com.ning.metrics.action.hdfs.data.schema.ColumnKey
 */
public abstract class Row<T extends Comparable, Serializable> implements WritableComparable
{
    public static final String JSON_ROW_ENTRIES = "entries";

    private static final String DELIM = ",";

    protected List<T> data;
    protected RowSchema schema;

    /**
     * Return the data associated to a key
     *
     * @param key RowSchema key
     * @return data associated with this key
     * @throws RowAccessException if the key maps to a column greater than the number of columns contained in the row
     */
    public T get(ColumnKey key) throws RowAccessException
    {
        if (key instanceof ColumnKeyTransformer) {
            applyTransformer((ColumnKeyTransformer) key);
        }

        int pos = schema.getColNum(key);

        if (pos >= data.size()) {
            throw new RowAccessException(String.format("column key %s maps to position %d and max position in data is %d", key, pos, data.size() - 1));
        }

        return data.get(schema.getColNum(key));
    }

    /**
     * Add some data to a row
     *
     * @param key   RowSchema key associated with the data
     * @param value value to add
     * @return the current row
     */
    public Row addCol(ColumnKey key, T value)
    {
        int pos = data.size();

        schema.addCol(key, pos);
        data.add(value);

        return this;
    }

    /**
     * Whether the schema associated with the row contains a specified key
     *
     * @param key key to lookup
     * @return true if the RowSchema contains the specified column
     */
    public boolean hasKey(ColumnKey key)
    {
        return schema.hasColumnKey(key);
    }

    /**
     * Serialize the row into the DataOutput
     *
     * @param out DataOutput to write
     * @throws IOException generic serialization error
     */
    public abstract void write(DataOutput out) throws IOException;

    /**
     * Replace the current row content with a specified DataInput
     *
     * @param in DataInput to read
     * @throws IOException generic serialization error
     */
    public abstract void readFields(DataInput in) throws IOException;

    /**
     * Get a Jackson-friendly representation of an item
     *
     * @param item data item to represent
     * @return json representation
     */
    protected abstract Object getJsonValue(T item);

    @JsonValue
    @SuppressWarnings({"unchecked", "unused"})
    public ImmutableMap toMap()
    {
        ImmutableMap.Builder b = new ImmutableMap.Builder();

        int i = 0;
        for (T item : data) {
            b.put(schema.getFieldNameByPosition(i), getJsonValue(item));
            i++;
        }

        return b.build();
    }

    @Override
    public int compareTo(Object o)
    {
        Row thing = (Row) o;
        int result = schema.compareTo(thing.schema);

        if (result == 0 && o instanceof WritableComparable) {
            for (int i = 0; i < data.size() && i < thing.data.size(); i++) {
                WritableComparable thisDataItem = (WritableComparable) data.get(i);
                T thingDataItem = (T) thing.data.get(i);

                result = thisDataItem.compareTo(thingDataItem);

                if (result != 0) {
                    return result;
                }
            }

            if (data.size() > thing.data.size()) {
                return 1;
            }
            else if (data.size() < thing.data.size()) {
                return -1;
            }

            return 0;
        }

        return result;
    }

    protected void applyTransformer(ColumnKeyTransformer keyTransformer)
    {
        if (!this.hasKey(keyTransformer)) {
            keyTransformer.transform(this);
        }
    }

    public String toString(String delimiter, boolean showHidden)
    {
        StringBuilder sb = new StringBuilder(data.size() * 32);
        boolean first = true;

        for (T item : data) {
            if (!first) {
                sb.append(delimiter);
            }

            if (item != null) {
                sb.append(item.toString());
            }
            first = false;
        }

        return new String(sb);
    }

    @Override
    public String toString()
    {
        return toString(DELIM);
    }

    public String toString(String delimiter)
    {
        return toString(delimiter, false);
    }

    public void toJSON(final JsonGenerator generator) throws IOException
    {
        int i = 0;
        generator.writeStartObject();
        for (T item : data) {
            generator.writeFieldName(schema.getFieldNameByPosition(i));
            generator.writeObject(getJsonValue(item));
            i++;
        }
        generator.writeEndObject();
        generator.flush();
    }

    public String toJSON() throws IOException
    {
        final StringWriter s = new StringWriter();
        final JsonGenerator generator = new JsonFactory().createJsonGenerator(s);
        generator.setPrettyPrinter(new DefaultPrettyPrinter());
        toJSON(generator);
        generator.close();

        return s.toString();
    }

    @Override
    public int hashCode()
    {
        return data.hashCode() ^ schema.hashCode();
    }

    public boolean equals(Object o)
    {
        return this == o || o != null && o instanceof Row && data.equals(((Row) o).data) && schema.equals(((Row) o).schema);
    }
}
