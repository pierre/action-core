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

import com.ning.metrics.action.hdfs.data.schema.DynamicColumnKey;
import com.ning.metrics.action.hdfs.data.schema.RowSchema;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class RowTextFileContentsIterator implements Iterator<Row>, Closeable
{
    private static final Logger log = Logger.getLogger(RowTextFileContentsIterator.class);

    private String line;
    private final BufferedReader reader;
    private boolean isReaderClosed = false;

    public RowTextFileContentsIterator(BufferedReader reader)
    {
        this.reader = reader;
    }

    @Override
    public boolean hasNext()
    {
        try {
            if (line == null) {
                line = reader.readLine();
            }

            boolean hasNext = line != null;

            if (!hasNext) {
                close();
            }

            return hasNext;
        }
        catch (IOException e1) {
            close();

            return false;
        }
    }

    @Override
    public Row next()
    {
        hasNext();

        if (line == null) {
            throw new NoSuchElementException();
        }

        ArrayList<String> list = new ArrayList<String>();
        list.add(line);
        Row row = RowFactory.getRow(new RowSchema("ad-hoc", new DynamicColumnKey("record")), list);

        line = null;

        return row;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("remove not implemented; read-only iterator");
    }

    @Override
    public void close()
    {
        try {
            if (!isReaderClosed) {
                reader.close();
                isReaderClosed = true;
            }
        }
        catch (IOException e) {
            log.warn("Unable to close hdfs reader", e);
        }
    }
}
