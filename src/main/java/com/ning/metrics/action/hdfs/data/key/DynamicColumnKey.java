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

package com.ning.metrics.action.hdfs.data.key;

import com.ning.metrics.action.hdfs.data.transformer.IdentityRowTransformer;
import com.ning.metrics.action.hdfs.data.transformer.RowTransformer;

public class DynamicColumnKey implements ColumnKey
{
    private final String keyName;
    private RowTransformer transformer = new IdentityRowTransformer();

    public static ColumnKey NULL = new DynamicColumnKey("");

    public DynamicColumnKey()
    {
        this(ColumnKeyBuilder.createColumnKey());
    }

    public DynamicColumnKey(String keyName)
    {
        this.keyName = keyName;
    }

    public DynamicColumnKey(String keyName, RowTransformer transformer)
    {
        this.keyName = keyName;
        this.transformer = transformer;
    }

    public DynamicColumnKey(ColumnKey sourceKey)
    {
        this(sourceKey.getKeyName(), sourceKey.getRowTransformer());
    }

    public String getKeyName()
    {
        return keyName;
    }

    public String toString()
    {
        return getKeyName();
    }

    public int hashCode()
    {
        return keyName.hashCode();
    }

    public boolean equals(Object o)
    {
        return o instanceof ColumnKey && keyName.equals(((ColumnKey) o).getKeyName());
    }

    public RowTransformer getRowTransformer()
    {
        return transformer;
    }
}
