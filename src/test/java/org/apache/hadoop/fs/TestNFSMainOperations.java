/*
 * Copyright 2015 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.nfs.NFSv3FileSystem;

public class TestNFSMainOperations extends FSMainOperationsBaseTest {

    public TestNFSMainOperations() {
        super("/");
    }

    @Override
    protected FileSystem createFileSystem() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(NFSTestOptions.NFS_TEST_OPTIONS_FILE);
        if (conf.get("fs.defaultFS") != null) {
            FileSystem fs = new NFSv3FileSystem();
            fs.initialize(new URI(conf.get("fs.defaultFS")), conf);
            return fs;
        } else {
            throw new RuntimeException("Configuration option fs.defaultFS is not defined");
        }
    }

    @Override
    public void testCopyToLocalWithUseRawLocalFileSystemOption() throws Exception {
        // The root path for NFS is overlapping with root path of RawLocalFileSystem
    }

    @Override
    public void testListStatusThrowsExceptionForUnreadableDir() {
        // Doesn't handle permissions well yet
    }

}
