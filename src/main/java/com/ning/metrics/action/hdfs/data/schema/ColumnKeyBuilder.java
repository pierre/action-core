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

package com.ning.metrics.action.hdfs.data.schema;

import com.ning.metrics.action.hdfs.data.transformer.IdentityRowTransformer;
import com.ning.metrics.action.hdfs.data.transformer.RowTransformer;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class ColumnKeyBuilder
{
    private static AtomicLong nextKey = new AtomicLong(0L);

    public static ColumnKey createColumnKey() throws IllegalStateException
    {
        return new ColumnKeyImpl(String.valueOf("__" + nextKey.getAndIncrement()));
    }

    private static class ColumnKeyImpl implements ColumnKey, Serializable
    {
        private final String keyName;
        private final RowTransformer transformer = new IdentityRowTransformer();

        private ColumnKeyImpl(String keyName)
        {
            this.keyName = keyName;
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
}