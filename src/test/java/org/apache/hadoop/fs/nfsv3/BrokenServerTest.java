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

package org.apache.hadoop.fs.nfsv3;

import java.io.IOException;
import java.net.BindException;
import java.net.URI;
import java.util.EnumSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.nfs.NFSv3FileSystemStore;
import org.apache.hadoop.fs.nfsv3.server.FileObject;
import org.apache.hadoop.fs.nfsv3.server.MockNfs3Server;
import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3SetAttr;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3.SetAttrField;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokenServerTest {

  static final int NFS_SERVER_PORT = 8211;
  static final String MOUNT_PATH = "/somepath";
  static NFSv3FileSystemStore store;
  static MockNfs3Server nfsServer;
  static Thread serverThread;
  static int nfsServerPort;
  
  public final static Logger LOG = LoggerFactory.getLogger(BrokenServerTest.class);
  
  @BeforeClass
  public static void setUp() throws Exception {
    
    // Start the Mock NFS server
    nfsServerPort = NFS_SERVER_PORT;
    while(nfsServerPort < 50000) {
      try {
        nfsServer = new MockNfs3Server(true, nfsServerPort);
        serverThread = new Thread(nfsServer);
        serverThread.start();
        LOG.info("Started mock NFS3 server ...");
        break;
      } catch(BindException exception) {
        nfsServerPort++;
        continue;
      }
    }

    // Connect to NFS
    Configuration conf = new Configuration();
    conf.setBoolean("mambo.test", true);
    conf.set("mambo.test.mountpath", MOUNT_PATH);
    store = new NFSv3FileSystemStore(new URI("nfs://localhost:" + nfsServerPort + "/"), conf, 2);
    store.initialize(new URI("nfs://localhost:" + nfsServerPort + "/"), conf);
  }

  @SuppressWarnings("deprecation")
  @AfterClass
  public static void tearDown() throws Exception {
    serverThread.stop();
    
  }

  @Test(expected = IOException.class)
  public void CREATE() throws Exception {
    store.create(new Nfs3FileHandle(0), "create_guarded_file1", Nfs3Constant.CREATE_GUARDED, new Nfs3SetAttr(), 0L, new CredentialsNone());
  }

  @Test(expected = IOException.class)
  public void COMMIT() throws Exception {
    store.commit(new Nfs3FileHandle(0L), 0L, 4096, new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void FSINFO() throws Exception {
    store.fsinfo(new Nfs3FileHandle(0L), new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void FSINFO2() throws Exception {
    store.getFilesystemInfo(new Nfs3FileHandle(0L), new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void GETATTR() throws Exception {
    store.getattr(new Nfs3FileHandle(0L), new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void GETATTR2() throws Exception {
    store.getFileAttributes(new Nfs3FileHandle(0L), new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void LOOKUP() throws Exception {
    store.lookup(new Nfs3FileHandle(0L), "lookup1", new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void LOOKUP2() throws Exception {
    store.getFileHandle(new Nfs3FileHandle(0L), "lookup1", new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void MKDIR() throws Exception {
    store.mkdir(new Nfs3FileHandle(0L), "dir2", new Nfs3SetAttr(), new CredentialsNone());
  }

  @Test(expected = IOException.class)
  public void NULL() throws IOException {
    store.nullOp();
  }

  @Test(expected = IOException.class)
  public void READ() throws Exception {
    store.read(new Nfs3FileHandle(0L), (long) FileObject.MAX_FILE_SIZE, 1, new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void READDIR() throws Exception {
    store.readdir(new Nfs3FileHandle(0L), 0L, 0L, 65536, new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void READDIR2() throws Exception {
    store.getDirectoryList(new Nfs3FileHandle(0L), 0L, 0L, 65536, new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void RENAME() throws Exception {
    store.rename(new Nfs3FileHandle(0L), "rename1", new Nfs3FileHandle(0L), "rename2", new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void REMOVE() throws Exception {
    store.remove(new Nfs3FileHandle(0L), "remove1", new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void RMDIR() throws Exception {
    store.rmdir(new Nfs3FileHandle(0L), "remove1", new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void SETATTR() throws Exception {
    Nfs3SetAttr attr = new Nfs3SetAttr();
    attr.setUid(100);
    attr.setGid(100);
    EnumSet<SetAttrField> updates = EnumSet.of(SetAttrField.UID, SetAttrField.GID);
    attr.setUpdateFields(updates);
    store.setattr(new Nfs3FileHandle(0L), attr, false, new NfsTime(System.currentTimeMillis()), new CredentialsNone());
  }
  
  @Test(expected = IOException.class)
  public void WRITE() throws Exception {
    byte[] largeData = new byte[100];
    store.write(new Nfs3FileHandle(0L), (long) FileObject.MAX_FILE_SIZE, largeData.length, WriteStableHow.FILE_SYNC, largeData, new CredentialsNone());
  }
}
