/**
 * Copyright 2014 NetApp Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.nfs.NFSv3FileSystem;
import org.junit.BeforeClass;

public class NFSFilesystemContractTest extends FileSystemContractBaseTest {
  
  @BeforeClass
  protected void setUp() throws Exception {
    Configuration conf = new Configuration();
    String workingDir = System.getProperty("user.dir");
    conf.addResource(new FileInputStream(workingDir + "/src/test/resources/contract-test-options.xml"));
    
    String fsName = conf.get("fs.defaultFS");
    String mountDirectory = conf.get("fs.nfs.mountdir");

    if(!fsName.startsWith("nfs") || mountDirectory != null) {
      fs = new NFSv3FileSystem();
      fs.initialize(new URI(fsName), conf);
      fs.delete(path("/test"), true);
    }
    // Configuration could not be loaded
    else {
      fs = null;
      throw new IOException("Could not load test NFS configuration!");
    }
  }
  
}
