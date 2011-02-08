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

package com.ning.metrics.action.hdfs.data;

import com.google.common.collect.ImmutableMap;
import com.ning.metrics.action.hdfs.data.schema.ColumnKey;
import com.ning.metrics.action.hdfs.data.schema.RowSchema;
import com.ning.metrics.action.hdfs.data.transformer.ColumnKeyTransformer;
import com.ning.metrics.serialization.thrift.item.DataItem;
import com.ning.metrics.serialization.thrift.item.DataItemDeserializer;
import com.ning.metrics.serialization.thrift.item.DataItemFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.protocol.TType;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Abstraction layer for a line in a file in HDFS.
 *
 * @see com.ning.metrics.action.hdfs.data.schema.RowSchema
 * @see com.ning.metrics.action.hdfs.data.schema.ColumnKey
 */
public class Row implements WritableComparable
{
    private static final String DELIM = ",";

    private final RowSchema schema;
    private List<DataItem> data;

    public static final String JSON_ROW_ENTRIES = "entries";

    @JsonCreator
    @SuppressWarnings("unused")
    public Row(
        @JsonProperty(JSON_ROW_ENTRIES) List<String> entries
    )
    {
        this.schema = null;

        ArrayList<DataItem> items = new ArrayList<DataItem>();
        for (String e : entries) {
            items.add(DataItemFactory.create(e));
        }
        this.data = items;
    }

    // TODO: consider doing a value-copy of RowSchema to ensure each row has its own copy

    public Row(RowSchema schema, List<DataItem> data)
    {
        this.schema = schema;
        this.data = data;
    }

    public Row(RowSchema schema, DataItem... data)
    {
        this(schema, Arrays.asList(data));
    }

    public Row(RowSchema schema)
    {
        this(schema, new ArrayList<DataItem>());
    }

    public DataItem get(ColumnKey key) throws RowAccessException
    {
        if (key instanceof ColumnKeyTransformer) {
            applyTransformer((ColumnKeyTransformer) key);
        }

        int pos = schema.getColNum(key);

        if (pos >= data.size()) {
            throw new RowAccessException(
                String.format("column key %s maps to position %d and max position in data is %d", key, pos, data.size() - 1));
        }

        return data.get(schema.getColNum(key));
    }

    private void applyTransformer(ColumnKeyTransformer keyTransformer)
    {
        if (!this.hasKey(keyTransformer)) {
            keyTransformer.transform(this);
        }
    }

    public boolean hasKey(ColumnKey key)
    {
        return schema.hasColumnKey(key);
    }

    public Row addCol(ColumnKey key, DataItem value)
    {
        int pos = data.size();

        schema.addCol(key, pos);
        data.add(value);

        return this;
    }

    public void write(DataOutput out) throws IOException
    {
        schema.write(out);
        WritableUtils.writeVInt(out, data.size());

        for (DataItem dataItem : data) {
            dataItem.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException
    {
        schema.readFields(in);

        int size = WritableUtils.readVInt(in);

        data = new ArrayList<DataItem>(size);

        for (int i = 0; i < size; i++) {
            data.add(new DataItemDeserializer().fromHadoop(in));
        }
    }

    public int compareTo(Object o)
    {
        Row thing = (Row) o;
        int result = schema.compareTo(thing.schema);

        if (result == 0 && o instanceof WritableComparable) {
            for (int i = 0; i < data.size() && i < thing.data.size(); i++) {
                WritableComparable thisDataItem = (WritableComparable) data.get(i);
                DataItem thingDataItem = thing.data.get(i);

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

    public String toJSON() throws IOException
    {
        JsonFactory f = new JsonFactory();
        StringWriter s = new StringWriter();
        JsonGenerator g = f.createJsonGenerator(s);

        int i = 0;
        g.writeStartObject();
        for (DataItem item : data) {
            g.writeObjectField(schema.getFieldNameByPosition(i), getJsonValue(item));
            i++;
        }
        g.writeEndObject();
        g.flush();

        return s.toString();
    }

    public String toString(String delimiter)
    {
        return toString(delimiter, false);
    }

    public String toString(String delimiter, boolean showHidden)
    {
        StringBuilder sb = new StringBuilder(data.size() * 32);
        boolean first = true;

        for (DataItem item : data) {
            if (!first) {
                sb.append(delimiter);
            }

            sb.append(item.toString());

            first = false;
        }

        return new String(sb);
    }

    public String toString()
    {
        return toString(DELIM);
    }

    public int hashCode()
    {
        return data.hashCode() ^ schema.hashCode();
    }

    public boolean equals(Object o)
    {
        return this == o || o != null && o instanceof Row && data.equals(((Row) o).data) && schema.equals(((Row) o).schema);
    }

    @JsonValue
    @SuppressWarnings({"unchecked", "unused"})
    public ImmutableMap toMap()
    {
        ImmutableMap.Builder b = new ImmutableMap.Builder();

        int i = 0;
        for (DataItem item : data) {
            b.put(schema.getFieldNameByPosition(i), getJsonValue(item));
            i++;
        }

        return b.build();
    }

    private Object getJsonValue(DataItem dataItem)
    {
        switch (dataItem.getThriftType()) {
            case TType.BOOL:
                return dataItem.getBoolean();
            case TType.BYTE:
                return dataItem.getByte();
            case TType.I16:
                return dataItem.getShort();
            case TType.I32:
                return dataItem.getInteger();
            case TType.I64:
                return dataItem.getLong();
            case TType.DOUBLE:
                return dataItem.getDouble();
            case TType.STRING:
                return dataItem.getString();
            default:
                return dataItem.getString();
        }
    }
}
