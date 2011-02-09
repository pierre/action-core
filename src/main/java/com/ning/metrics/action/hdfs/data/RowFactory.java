package com.ning.metrics.action.hdfs.data;

import com.ning.metrics.action.hdfs.data.schema.RowSchema;
import com.ning.metrics.serialization.thrift.item.DataItem;

import java.util.List;

public class RowFactory
{
    public static <T extends Comparable, Serializable> Row getRow(RowSchema rowSchema, List<T> data)
    {
        if (data.get(0) instanceof DataItem) {
            return new RowThrift(rowSchema, (List<DataItem>) data);
        } else {
            return new RowText(rowSchema, (List<String>) data);
        }
    }
}
