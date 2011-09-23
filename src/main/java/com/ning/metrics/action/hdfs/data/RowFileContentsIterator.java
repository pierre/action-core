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

import com.ning.metrics.action.hdfs.data.parser.RowParser;
import com.ning.metrics.action.schema.Registrar;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * File wrapper - turn a file into a Row iterator
 */
abstract class RowFileContentsIterator implements Iterator<Row>, Closeable
{
    static final Logger log = Logger.getLogger(RowFileContentsIterator.class);

    final boolean rawContents;
    final String pathname;
    final RowParser rowParser;
    private Row row;
    private final Rows batchedRows = new Rows();
    boolean readerClosed = false;
    final Registrar registrar;

    public static final String JSON_CONTENT_PATH = "path";
    public static final String JSON_CONTENT_ENTRIES = "entries";

    @JsonCreator
    @SuppressWarnings("unused")
    public RowFileContentsIterator(
        @JsonProperty(JSON_CONTENT_PATH) final String path,
        @JsonProperty(JSON_CONTENT_ENTRIES) final List<Row> entries
    )
    {
        this(path, null, null, true);
    }

    public RowFileContentsIterator(final String pathname, final RowParser rowParser, final Registrar registrar, final boolean rawContents)
    {
        this.pathname = pathname;
        this.rowParser = rowParser;
        this.registrar = registrar;
        this.rawContents = rawContents;
    }

    @Override
    public boolean hasNext()
    {
        if (row == null) {
            // Make sure not to produce faster than the client can consume
            if (batchedRows.size() == 0) {
                final Rows newRows = readNextRows();
                if (newRows != null) {
                    batchedRows.addAll(newRows);
                }
            }

            row = batchedRows.poll();
        }

        return row != null;
    }

    @Override
    public Row next()
    {
        hasNext();

        final Row returnRow = row;
        row = null;

        if (returnRow == null) {
            throw new NoSuchElementException();
        }

        return returnRow;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("remove not implemented; read-only iterator");
    }

    /**
     * Read one or more rows
     *
     * @return the next row(s)
     */
    abstract Rows readNextRows();
}
