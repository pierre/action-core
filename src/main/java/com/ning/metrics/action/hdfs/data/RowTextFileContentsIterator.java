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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Iterator for flat file (can contain binary data)
 */
class RowTextFileContentsIterator extends RowFileContentsIterator
{
    private final BufferedReader reader;

    public RowTextFileContentsIterator(final String pathname, final RowParser rowParser, final Registrar registrar, final BufferedReader reader, final boolean rawContents)
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
                log.warn(String.format("Unable to close reader: %s", e));
            }
        }
    }

    @Override
    Rows readRows()
    {
        try {
            if (readerClosed) {
                return null;
            }
            else {
                Rows rows = new Rows();
                final String value = reader.readLine();

                if (value == null) {
                    close();
                    return rows;
                }

                if (renderAsRow) {
                    final List<String> list = new ArrayList<String>();
                    list.add(value);
                    rows.add(RowFactory.getRow(new RowSchema("ad-hoc", new DynamicColumnKey("record")), list));
                }
                else {
                    rows = rowParser.valueToRows(registrar, value);
                }

                return rows;
            }
        }
        catch (IOException e) {
            log.info(String.format("IOException reading file %s, skipping", pathname));

            close();

            return null;
        }
    }
}
