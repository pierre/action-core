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

package com.ning.metrics.action.hdfs.data.transformer;

import com.ning.metrics.action.hdfs.data.schema.ColumnKey;
import com.ning.metrics.action.hdfs.data.schema.DynamicColumnKey;
import com.ning.metrics.action.hdfs.data.Row;

public class IdentityColumnKeyTransformer implements ColumnKeyTransformer
{
    private ColumnKey inputKey = DynamicColumnKey.NULL;
    private ColumnKey outputKey = new DynamicColumnKey();

    public Row transform(Row row)
    {
        return row;
    }

    protected IdentityColumnKeyTransformer(ColumnKey inputKey, ColumnKey outputKey)
    {
        this.inputKey = inputKey;
        this.outputKey = outputKey;
    }

    protected ColumnKey getInputKey()
    {
        return inputKey;
    }

    protected final void setInputKey(ColumnKey inputKey)
    {
        this.inputKey = inputKey;
    }

    protected ColumnKey getOutputKey()
    {
        return outputKey;
    }

    protected final void setOutputKey(ColumnKey outputKey)
    {
        this.outputKey = outputKey;
    }

    public String getKeyName()
    {
        return outputKey.getKeyName();
    }

    public RowTransformer getRowTransformer()
    {
        return this;
    }

    public int hashCode()
    {
        int inputKeyHash = inputKey == null ? 0 : inputKey.hashCode();
        int outputKeyHash = outputKey == null ? 0 : outputKey.hashCode();
        int classHash = this.getClass().getName().hashCode();

        return 31 * ((31 * inputKeyHash) ^ outputKeyHash) ^ classHash;
    }

    public boolean equals(Object o)
    {
        return o.getClass().equals(this.getClass()) &&
            inputKey.equals(((IdentityColumnKeyTransformer) o).inputKey) &&
            outputKey.equals(((IdentityColumnKeyTransformer) o).outputKey);
    }

}
