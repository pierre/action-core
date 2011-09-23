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

import com.ning.metrics.action.hdfs.data.parser.BufferedRowsReader;
import com.ning.metrics.action.hdfs.data.parser.BufferedSmileReader;
import com.ning.metrics.action.hdfs.data.parser.BufferedThriftReader;
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
    private final boolean binary;
    private final BufferedReader reader;
    private final BufferedRowsReader streamReader;

    public RowTextFileContentsIterator(final String pathname, final RowParser rowParser, final Registrar registrar, final InputStream in, final boolean rawContents) throws IOException
    {
        super(pathname, rowParser, registrar, rawContents);
        this.in = in;
        final String[] tokenizedPathname = StringUtils.split(pathname, ".");
        final String suffix = tokenizedPathname[tokenizedPathname.length - 1];

        binary = suffix.equals("smile") || suffix.equals("thrift");
        if (suffix.equals("smile")) {
            reader = null;
            streamReader = new BufferedSmileReader(registrar, in);
        }
        else if (suffix.equals("thrift")) {
            reader = null;
            streamReader = new BufferedThriftReader(registrar, in);
        }
        else {
            streamReader = null;
            reader = new BufferedReader(new InputStreamReader(in));
        }
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
            // Non-binary payload
            else if (!binary) {
                return readLine();
            }
            else {
                final Rows rows = streamReader.readNext();

                if (rows == null) {
                    close();
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

    private Rows readLine() throws IOException
    {
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
