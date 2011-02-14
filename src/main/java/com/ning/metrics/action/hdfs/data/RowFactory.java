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
