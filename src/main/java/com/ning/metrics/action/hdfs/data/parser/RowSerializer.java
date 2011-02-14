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

package com.ning.metrics.action.hdfs.data.parser;

import com.ning.metrics.action.hdfs.data.RowAccessException;
import com.ning.metrics.action.hdfs.data.Rows;
import com.ning.metrics.action.schema.Registrar;

/**
 * Read a row worth of data from Hadoop and decode it in the right format.
 * The is where the whole magic happens.
 */
public interface RowSerializer
{
    public boolean accept(Object o);

    /**
     * Create a row with decoded data from a generic Object o.
     * <p/>
     * The Hadoop SequenceFile.Reader returns an Object back that we need to understand. This method
     * generate the Row and decode the data as necessary.
     * If a valid registrar is specified, some more advanced decoding can be performed
     *
     * @param r Registrar, describing the columns
     * @param o Object from Hadoop to decode (e.g. written by the collector)
     * @return a Row representation with associated data decoded
     * @throws RowAccessException Generic deserialization error
     * @see com.ning.metrics.action.hdfs.data.schema.RowSchema
     */
    public Rows toRows(Registrar r, Object o) throws RowAccessException;
}
