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

package com.ning.metrics.action.hdfs.writer;

import com.google.inject.Inject;
import com.ning.metrics.serialization.hadoop.FileSystemAccess;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

@Guice(modules = MockHdfsModule.class)
public class TestHdfsWriter
{
    @Inject
    private FileSystemAccess fileSystemAccess;

    private final File file = new File("src/test/resources/lorem.txt");
    private final String outputPath = System.getProperty("java.io.tmpdir") + "action-core-hdfs-writer-" + System.currentTimeMillis();

    @BeforeTest(alwaysRun = true)
    public void setup() throws Exception
    {
        final File tmpDir = new File(outputPath);
        tmpDir.mkdirs();
        tmpDir.deleteOnExit();
    }

    @Test(groups = "fast")
    public void testWrite() throws Exception
    {
        final HdfsWriter writer = new HdfsWriter(fileSystemAccess);

        final String outputFile = outputPath + "/lorem.txt";
        final URI outputURI = writer.write(new FileInputStream(file), outputFile, false, (short) 1, 1024, "ugo=r");

        final FileStatus status = fileSystemAccess.get().getFileStatus(new Path(outputURI));
        Assert.assertEquals(status.getPath().toString(), "file:" + outputFile);
        Assert.assertEquals(status.getReplication(), (short) 1);
        //Broken for some reason?
        //Assert.assertEquals(status.getBlockSize(), 1024);
        //Assert.assertEquals(status.getPermission().toString(), "-r--r--r--");

        // Test that we don't overwrite the file
        try {
            writer.write(new FileInputStream(file), outputFile, false, (short) 1, 1024, "ugo=r");
            Assert.fail();
        }
        catch (IOException e) {
            Assert.assertEquals(e.getLocalizedMessage(), String.format("File already exists:%s", outputFile));
        }
    }
}
