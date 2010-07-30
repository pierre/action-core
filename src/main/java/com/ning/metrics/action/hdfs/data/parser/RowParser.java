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

import com.google.inject.Inject;
import com.ning.metrics.action.binder.config.ActionCoreConfig;
import com.ning.metrics.action.hdfs.data.Row;
import com.ning.metrics.action.hdfs.data.RowAccessException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RowParser implements Serializable
{
    private List<RowSerializer> serializations = new ArrayList<RowSerializer>();
    private final Logger log = Logger.getLogger(RowParser.class);
    private final ClassLoader classLoader;

    @Inject
    public RowParser(
        ActionCoreConfig conf
    )
    {
        classLoader = RowParser.class.getClassLoader();

        // TODO ServiceLoader
        for (String serializerName : StringUtils.split(conf.getRowSerializations(), ",")) {
            try {
                add(serializerName);
            }
            catch (ClassNotFoundException e) {
                log.warn(String.format("Ignoring specified RowSerializer [%s], as it is not in classpath", serializerName));
            }
            catch (InstantiationException e) {
                log.warn(String.format("Ignoring specified RowSerializer [%s]", serializerName), e);
            }
            catch (IllegalAccessException e) {
                log.warn(String.format("Ignoring specified RowSerializer [%s]", serializerName), e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void add(String serializationName) throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        Class<? extends RowSerializer> serializionClass = (Class<? extends RowSerializer>) Class.forName(serializationName, true, classLoader);
        serializations.add(serializionClass.newInstance());
    }

    public Row valueToRow(Object c) throws RowAccessException
    {
        for (RowSerializer serialization : serializations) {
            if (serialization.accept(c)) {
                return serialization.toRow(c);
            }
        }
        throw new RowAccessException(String.format("unknown class type: %s", c.getClass().getName()));
    }
}
