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
import com.ning.metrics.action.hdfs.data.schema.DynamicColumnKey;
import com.ning.metrics.action.hdfs.data.schema.RowSchema;
import com.ning.metrics.action.schema.Registrar;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class RowSequenceFileContentsIterator extends RowFileContentsIterator
{
    private final SequenceFile.Reader reader;

    public RowSequenceFileContentsIterator(final String pathname, final RowParser rowParser, final Registrar registrar, final SequenceFile.Reader reader, final boolean rawContents)
    {
        super(pathname, rowParser, registrar, rawContents);
        this.reader = reader;
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
                log.warn("Unable to close reader", e);
            }
        }
    }

    /**
     * Read one or more rows
     *
     * @return the next row(s)
     */
    @Override
    Rows readNextRows()
    {
        try {
            if (readerClosed) {
                return null;
            }
            else {
                final Object key = reader.next((Object) null);
                Rows rows = new Rows();

                if (key != null) {
                    log.debug("Read object [{}]", key);

                    final Object value = reader.getCurrentValue((Object) null);

                    if (value == null) {
                        close();
                        return rows;
                    }

                    if (rawContents) {
                        final List<String> list = new ArrayList<String>();
                        list.add(value.toString());
                        rows.add(RowFactory.getRow(new RowSchema("ad-hoc", new DynamicColumnKey("record")), list));
                    }
                    else {
                        rows = rowParser.valueToRows(registrar, value);
                    }
                }
                else {
                    close();
                }

                return rows;
            }
        }
        catch (IOException e) {
            log.info("IOException reading file {}, skipping", pathname);

            close();

            return null;
        }
    }
}
