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
import org.codehaus.jackson.JsonNode;

import java.util.List;

public class RowFactory
{
    /**
     * Return the right row associated with some data.
     * <p/>
     * The decoding from Hadoop was done in the RowSerializers. This utility class simply maps the right Row representation
     * given the decoded datatype.
     *
     * @param rowSchema      schema associated with the row
     * @param data           decoded Data
     * @param <T>            decoded column types
     * @param <Serializable> serialization
     * @return a row instance with associated schema and data
     * @see com.ning.metrics.action.hdfs.data.parser.RowSerializer
     */
    public static <T extends Comparable, Serializable> Row getRow(RowSchema rowSchema, List<T> data)
    {
        if (data.get(0) instanceof DataItem) {
            return new RowThrift(rowSchema, (List<DataItem>) data);
        }
        else if (data.get(0) instanceof JsonNode) {
            return new RowSmile(rowSchema, (List<JsonNodeComparable>) data);
        }
        else {
            return new RowText(rowSchema, (List<String>) data);
        }
    }
}
