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

package com.ning.metrics.action.hdfs.data;

import com.google.common.collect.ImmutableMap;
import com.ning.metrics.action.hdfs.data.parser.RowParser;
import com.ning.metrics.action.hdfs.data.schema.DynamicColumnKey;
import com.ning.metrics.action.hdfs.data.schema.RowSchema;
import com.ning.metrics.action.schema.Registrar;
import com.ning.metrics.serialization.thrift.item.DataItemFactory;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonValue;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class RowFileContentsIterator implements Iterator<Row>, Closeable
{
    private static final Logger log = Logger.getLogger(RowFileContentsIterator.class);

    private final SequenceFile.Reader reader;
    private final boolean renderAsRow;
    private final String pathname;
    private final RowParser rowParser;
    private Row row;
    private boolean readerClosed = false;
    private final Registrar registrar;

    public static final String JSON_CONTENT_PATH = "path";
    public static final String JSON_CONTENT_ENTRIES = "entries";

    @JsonCreator
    @SuppressWarnings("unused")
    public RowFileContentsIterator(
        @JsonProperty(JSON_CONTENT_PATH) String path,
        @JsonProperty(JSON_CONTENT_ENTRIES) List<Row> entries
    )
    {
        this(path, null, null, null, true);
    }

    public RowFileContentsIterator(String pathname, RowParser rowParser, Registrar registrar, SequenceFile.Reader reader, boolean rawContents)
    {
        this.pathname = pathname;
        this.rowParser = rowParser;
        this.registrar = registrar;
        this.reader = reader;
        this.renderAsRow = rawContents;
    }

    @Override
    public boolean hasNext()
    {
        if (row == null) {
            row = readRow();
        }

        return row != null;
    }

    @Override
    public Row next()
    {
        hasNext();

        Row returnRow = row;

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

    @Override
    public void close()
    {
        if (!readerClosed) {
            try {
                reader.close();
                readerClosed = true;
            }
            catch (IOException e) {
                log.warn(String.format("Unable to close reader: %s", e));
            }
        }
    }

    private Row readRow()
    {
        try {
            if (readerClosed) {
                return null;
            }
            else {
                Object key = reader.next((Object) null);
                Row row = null;

                if (key != null) {
                    log.debug(String.format("Read object [%s]", key));

                    Object value = reader.getCurrentValue((Object) null);

                    if (renderAsRow) {
                        row = new Row(new RowSchema("ad-hoc"));
                        row.addCol(new DynamicColumnKey("record"), DataItemFactory.create(value.toString()));
                    }
                    else {
                        row = rowParser.valueToRow(registrar, value);
                    }
                }
                else {
                    close();
                }

                return row;
            }
        }
        catch (IOException e) {
            log.info(String.format("IOException reading file %s, skipping", pathname));

            close();

            return null;
        }
    }

    @JsonValue
    @SuppressWarnings({"unchecked", "unused"})
    public ImmutableMap toMap()
    {
        ArrayList<Row> rows = new ArrayList<Row>();

        while (hasNext()) {
            rows.add(next());
        }

        return new ImmutableMap.Builder()
            .put(JSON_CONTENT_PATH, pathname)
            .put(JSON_CONTENT_ENTRIES, rows)
            .build();
    }
}
