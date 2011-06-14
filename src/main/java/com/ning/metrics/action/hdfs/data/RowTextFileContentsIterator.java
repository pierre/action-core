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
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Iterator for flat file (can contain binary data)
 */
class RowTextFileContentsIterator extends RowFileContentsIterator
{
    private final InputStream in;
    private BufferedReader reader = null;

    public RowTextFileContentsIterator(final String pathname, final RowParser rowParser, final Registrar registrar, final InputStream in, final boolean rawContents)
    {
        super(pathname, rowParser, registrar, rawContents);
        this.in = in;
    }

    @Override
    public void close()
    {
        if (!readerClosed) {
            try {
                if (reader != null) {
                    reader.close();
                }
                else {
                    in.close();
                }
                readerClosed = true;
            }
            catch (IOException e) {
                log.warn(String.format("Unable to close reader: %s", e));
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
                if (reader == null) {
                    // First time we come here
                    // If the file needs to be read as a whole (e.g. Smile), read the whole thing, otherwise read line by line
                    if (readByLine()) {
                        this.reader = new BufferedReader(new InputStreamReader(in));
                    }
                    else {
                        final Rows rows = rowParser.streamToRows(registrar, pathname, in);
                        close(); // Everything has been deserialized, nothing left to do here
                        return rows;
                    }
                }

                Rows rows = new Rows();
                final String value = reader.readLine();

                if (value == null) {
                    close();
                    return rows;
                }

                if (rawContents) {
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

    /**
     * @return true if we can read the file line by line, false otherwise
     */
    private boolean readByLine()
    {
        final String[] tokenizedPathname = StringUtils.split(pathname, ".");
        final String suffix = tokenizedPathname[tokenizedPathname.length - 1];

        return !suffix.equals("smile") && !suffix.equals("thrift");
    }
}
