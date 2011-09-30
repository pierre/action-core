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

package com.ning.metrics.action.hdfs.data.codec;

import com.ning.compress.lzf.LZFInputStream;

import java.io.IOException;
import java.io.InputStream;

public class DecompressedStreamFactory
{
    public static InputStream wrapStream(final String fileSuffix, final InputStream stream) throws IOException
    {
        if (fileSuffix.equals("lzf")) {
            return new LZFInputStream(stream);
        }
        else {
            return null;
        }
    }
}
