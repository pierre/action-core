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
import org.apache.hadoop.io.WritableUtils;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class RowSmile extends Row<JsonNodeComparable, Serializable>
{
    private static final ObjectMapper objectMapper = new ObjectMapper(new JsonFactory());

    /**
     * Create a row with data
     *
     * @param schema RowSchema of this row
     * @param data   row data
     */
    public RowSmile(RowSchema schema, List<JsonNodeComparable> data)
    {
        this.schema = schema;
        this.data = data;
    }

    /**
     * Serialize the row into the DataOutput
     *
     * @param out DataOutput to write
     * @throws java.io.IOException generic serialization error
     */
    @Override
    public void write(DataOutput out) throws IOException
    {
        schema.write(out);
        WritableUtils.writeVInt(out, data.size());

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        JsonGenerator gen = objectMapper.getJsonFactory().createJsonGenerator(outStream, JsonEncoding.UTF8);
        for (JsonNodeComparable dataItem : data) {
            objectMapper.writeValue(gen, dataItem);
        }
        gen.close();

        // Size of Smile payload. Needed for deserialization, see below
        WritableUtils.writeVInt(out, outStream.size());

        out.write(outStream.toByteArray());
    }

    /**
     * Replace the current row content with a specified DataInput
     *
     * @param in DataInput to read
     * @throws java.io.IOException generic serialization error
     */
    @Override
    public void readFields(DataInput in) throws IOException
    {
        schema.readFields(in);
        int numberOfItems = WritableUtils.readVInt(in);
        int smilePayloadSize = WritableUtils.readVInt(in);

        int itemsRead = 0;

        byte[] smilePayload = new byte[smilePayloadSize];
        in.readFully(smilePayload);

        JsonParser jp = objectMapper.getJsonFactory().createJsonParser(smilePayload);
        while (jp.nextToken() != null && itemsRead < numberOfItems) {
            objectMapper.readValue(jp, JsonNodeComparable.class);
            itemsRead++;
        }
        jp.close();
    }

    /**
     * Get a Jackson-friendly representation of an item
     *
     * @param item data item to represent
     * @return json representation
     */
    @Override
    protected Object getJsonValue(JsonNodeComparable item)
    {
        if (item.getDelegate().isNumber()) {
            return item.getDelegate().getNumberValue();
        }
        else if (item.getDelegate().isBoolean()) {
            return item.getDelegate().getBooleanValue();
        }
        else if (item.getDelegate().isTextual()) {
            return item.getDelegate().getTextValue();
        }
        else if (item.getDelegate().isNull()) {
            return null;
        }
        else {
            return item.getDelegate();
        }
    }
}
