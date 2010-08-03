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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.ning.metrics.action.hdfs.data.parser.RowParser;
import com.ning.metrics.action.schema.Registrar;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

@Singleton
public class RowFileContentsIteratorFactory
{
    private final RowParser rowParser;
    private final Registrar registrar;

    @Inject
    public RowFileContentsIteratorFactory(
        RowParser rowParser,
        Registrar registrar
    )
    {
        this.rowParser = rowParser;
        this.registrar = registrar;
    }

    public RowFileContentsIterator build(FileSystem fs, Path path, boolean raw) throws IOException
    {
        return new RowFileContentsIterator(
            path.toUri().getPath(),
            rowParser,
            registrar,
            new SequenceFile.Reader(fs, path, fs.getConf()),
            raw);
    }
}
