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

import com.ning.metrics.action.hdfs.data.schema.RowSchema;
import com.ning.metrics.serialization.thrift.item.DataItem;
import com.ning.metrics.serialization.thrift.item.DataItemDeserializer;
import com.ning.metrics.serialization.thrift.item.DataItemFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.protocol.TType;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstraction layer for a line in a file in HDFS.
 *
 * @see com.ning.metrics.action.hdfs.data.schema.RowSchema
 * @see com.ning.metrics.action.hdfs.data.schema.ColumnKey
 */
public class RowThrift extends Row<DataItem, Serializable>
{
    @JsonCreator
    @SuppressWarnings("unused")
    public RowThrift(@JsonProperty(JSON_ROW_ENTRIES) List<String> entries)
    {
        this.schema = null;

        ArrayList<DataItem> items = new ArrayList<DataItem>();
        for (String e : entries) {
            items.add(DataItemFactory.create(e));
        }
        this.data = items;
    }

    // TODO: consider doing a value-copy of RowSchema to ensure each row has its own copy

    /**
     * Create a row with data
     *
     * @param schema RowSchema of this row
     * @param data   row data
     */
    public RowThrift(RowSchema schema, List<DataItem> data)
    {
        this.schema = schema;
        this.data = data;
    }

    /**
     * Create an empty row given a RowSchema
     *
     * @param schema RowSchema of this row
     */
    public RowThrift(RowSchema schema)
    {
        this(schema, new ArrayList<DataItem>());
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        schema.write(out);
        WritableUtils.writeVInt(out, data.size());

        for (DataItem dataItem : data) {
            dataItem.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        schema.readFields(in);
        int size = WritableUtils.readVInt(in);

        data = new ArrayList<DataItem>(size);
        for (int i = 0; i < size; i++) {
            data.add(new DataItemDeserializer().fromHadoop(in));
        }
    }

    @Override
    protected Object getJsonValue(DataItem dataItem)
    {
        if (dataItem == null) {
            return "";
        }

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
