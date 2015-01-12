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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.BindException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.nfs.NFSv3FileSystemStore;
import org.apache.hadoop.fs.nfs.mount.MountClient;
import org.apache.hadoop.fs.nfs.mount.MountMNTResponse;
import org.apache.hadoop.fs.nfs.rpc.RpcClient;
import org.apache.hadoop.fs.nfsv3.server.FileObject;
import org.apache.hadoop.fs.nfsv3.server.MockNfs3Filesystem;
import org.apache.hadoop.fs.nfsv3.server.MockNfs3Server;
import org.apache.hadoop.mount.MountInterface;
import org.apache.hadoop.mount.MountResponse;
import org.apache.hadoop.nfs.NfsFileType;
import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Info;
import org.apache.hadoop.nfs.nfs3.Nfs3SetAttr;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3.SetAttrField;
import org.apache.hadoop.nfs.nfs3.response.COMMIT3Response;
import org.apache.hadoop.nfs.nfs3.response.CREATE3Response;
import org.apache.hadoop.nfs.nfs3.response.FSINFO3Response;
import org.apache.hadoop.nfs.nfs3.response.GETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.LOOKUP3Response;
import org.apache.hadoop.nfs.nfs3.response.MKDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.READ3Response;
import org.apache.hadoop.nfs.nfs3.response.REMOVE3Response;
import org.apache.hadoop.nfs.nfs3.response.RENAME3Response;
import org.apache.hadoop.nfs.nfs3.response.RMDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.SETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.RpcAcceptedReply.AcceptState;
import org.apache.hadoop.oncrpc.RpcMessage;
import org.apache.hadoop.oncrpc.RpcReply.ReplyState;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.oncrpc.security.CredentialsSys;
import org.apache.hadoop.oncrpc.security.RpcAuthInfo.AuthFlavor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Nfs3Test {

  static final int NFS_SERVER_PORT = 8211;
  static final String MOUNT_PATH = "/somepath";
  static NFSv3FileSystemStore store;
  static MockNfs3Server nfsServer;
  static Thread serverThread;
  static int nfsServerPort;
  
  public final static Logger LOG = LoggerFactory.getLogger(Nfs3Test.class);
  
  @BeforeClass
  public static void setUp() throws Exception {
    
    // Start the Mock NFS server
    nfsServerPort = NFS_SERVER_PORT;
    while(nfsServerPort < 50000) {
      try {
        nfsServer = new MockNfs3Server(false, nfsServerPort);
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

  @Test
  public void NullConfiguration() throws Exception {
    boolean errored = true;
    
    // Null configuration
    {
      NFSv3FileSystemStore temp = new NFSv3FileSystemStore(new URI("nfs://localhost:" + nfsServerPort + "/"), null, 1);
      try {
        temp.initialize(new URI("nfs://localhost:" + nfsServerPort + "/"), null);
        errored = false;
      } catch(IOException exception) {
        assertTrue(errored);
      } finally {
        temp.shutdown();
      }
    }
    
    {
      Configuration conf = new Configuration();
      conf.setBoolean("mambo.test", true);
      conf.set("mambo.test.mountpath", MOUNT_PATH);
      NFSv3FileSystemStore temp = new NFSv3FileSystemStore(new URI("nfs://localhost:" + nfsServerPort + "/"), conf, 1);
      temp.initialize(new URI("nfs://localhost:" + nfsServerPort + "/"), conf);
      temp.shutdown();
    }
    
    {
      Configuration conf = new Configuration();
      conf.setBoolean("mambo.test", true);
      conf.set("mambo.test.mountpath", MOUNT_PATH);
      conf.set("fs.nfs.auth.flavor", "BLAH");
      errored = false;
      NFSv3FileSystemStore temp = null;
      try {
        temp = new NFSv3FileSystemStore(new URI("nfs://localhost:" + nfsServerPort + "/"), conf, 1);
        temp.initialize(new URI("nfs://localhost:" + nfsServerPort + "/"), conf);
        temp.shutdown();
      } catch(IOException exception) {
        errored = true;
      } finally {
        assertTrue(errored);
      }
    }
    
    {
      Configuration conf = new Configuration();
      conf.setBoolean("mambo.test", true);
      conf.set("mambo.test.mountpath", MOUNT_PATH);
      conf.set("fs.nfs.auth.flavor", "AUTH_NONE");
      NFSv3FileSystemStore temp = new NFSv3FileSystemStore(new URI("nfs://localhost:" + nfsServerPort + "/"), conf, 1);
      assertEquals(AuthFlavor.AUTH_NONE, temp.getCredentials().getFlavor());
      temp.shutdown();
    }
    
    
    {
      Configuration conf = new Configuration();
      conf.setBoolean("mambo.test", true);
      conf.set("mambo.test.mountpath", MOUNT_PATH);
      conf.set("fs.nfs.auth.flavor", "AUTH_SYS");
      NFSv3FileSystemStore temp = new NFSv3FileSystemStore(new URI("nfs://localhost:" + nfsServerPort + "/"), conf, 1);
      assertEquals(AuthFlavor.AUTH_SYS, temp.getCredentials().getFlavor());
      temp.shutdown();
    } 
    
    {
      Configuration conf = new Configuration();
      conf.setBoolean("mambo.test", true);
      conf.set("mambo.test.mountpath", MOUNT_PATH);
      conf.set("fs.nfs.auth.flavor", "AUTH_UNIX");
      NFSv3FileSystemStore temp = new NFSv3FileSystemStore(new URI("nfs://localhost:" + nfsServerPort + "/"), conf, 1);
      assertEquals(AuthFlavor.AUTH_SYS, temp.getCredentials().getFlavor());
      temp.shutdown();
    }
    
  }
  
  @Test
  public void Mount() throws Exception {
    RpcClient client = new RpcClient("localhost", nfsServerPort);
    
    // Send the RPC request
    XDR in = new XDR();
    in.writeString("/mountpath");
    XDR out = new XDR();
    RpcMessage reply = client.service(MountClient.MOUNTD_PROGRAM, MountClient.MOUNTD_VERSION, MountInterface.MNTPROC.MNT.getValue(), in, out, new CredentialsNone());
    assertEquals(RpcMessage.Type.RPC_REPLY, reply.getMessageType());
    
    RpcAcceptedReply accepted = (RpcAcceptedReply) reply;
    assertEquals(ReplyState.MSG_ACCEPTED, accepted.getState());
    
    client.shutdown();
    
  }
  
  @SuppressWarnings("unused")
  @Test
  public void MountClient() throws Exception {
    
    {
      MountClient client = new MountClient("localhost", nfsServerPort, null);
      MountMNTResponse response = client.mnt("/mountpath");
      assertNotNull(response);
      assertEquals(MountResponse.MNT_OK, response.getStatus());
      assertNotNull(response.getAuthFlavors());
      assertEquals(AuthFlavor.AUTH_NONE.getValue(), response.getAuthFlavors()[0]);
      
      MountMNTResponse response2 = client.mnt("/blah");
      assertNotNull(response2);
      assertEquals(MountResponse.MNT_OK, response2.getStatus());
      assertNull(response2.getAuthFlavors());
      
      client.shutdown();
    }
    
    {
      Configuration conf = new Configuration();
      conf.set("fs.nfs.mount.auth.flavor", "AUTH_NONE");
      MountClient client = new MountClient("localhost", nfsServerPort, conf);
      MountMNTResponse response = client.mnt("/mountpath");
      assertNotNull(response);
      assertEquals(MountResponse.MNT_OK, response.getStatus());
      assertNotNull(response.getAuthFlavors());
      assertEquals(AuthFlavor.AUTH_NONE.getValue(), response.getAuthFlavors()[0]);
      client.shutdown();
    }
    
    {
      Configuration conf = new Configuration();
      conf.set("fs.nfs.mount.auth.flavor", "BLAH");
      MountClient client = new MountClient("localhost", nfsServerPort, conf);
      MountMNTResponse response = client.mnt("/mountpath");
      assertNotNull(response);
      assertEquals(MountResponse.MNT_OK, response.getStatus());
      assertNotNull(response.getAuthFlavors());
      assertEquals(AuthFlavor.AUTH_NONE.getValue(), response.getAuthFlavors()[0]);
      client.shutdown();
    }
    
    {
      Configuration conf = new Configuration();
      conf.set("fs.nfs.mount.auth.flavor", "AUTH_SYS");
      MountClient client2 = new MountClient("localhost", nfsServerPort, conf);
      boolean errored = true;
      try {
        MountMNTResponse response3 = client2.mnt("/mountpath");
        errored = false;
      } catch(IOException exception) {
        assertTrue(errored);
      }
    }
  }
  
  @Test
  public void ProgramMismatch() throws Exception {
    RpcClient client = new RpcClient("localhost", nfsServerPort);
    
    // Should be a reply message
    RpcMessage reply = client.service(1, 1, 1, new XDR(), new XDR(), new CredentialsNone());
    assertEquals(RpcMessage.Type.RPC_REPLY, reply.getMessageType());
    
    // Should be mismatch
    RpcAcceptedReply accepted = (RpcAcceptedReply) reply;
    assertEquals(ReplyState.MSG_ACCEPTED, accepted.getState());
    assertEquals(AcceptState.PROG_MISMATCH, accepted.getAcceptState());
    
    client.shutdown();
    
  }
  
  @SuppressWarnings("unused")
  @Test
  public void StrongCredentialsNotAccepted() throws Exception {
    FileHandle rootHandle = store.getRootfh();
    boolean errored = true;
    try {
      GETATTR3Response nfsGETATTRResponse = store.getattr(rootHandle, new CredentialsSys());
      errored = false;
    } catch(IOException exception) {
      assertTrue(errored);
    }
  }
  
  @Test
  public void CREATE() throws Exception {
    FileHandle rootHandle = store.getRootfh();
    
    // Create new file, should not exist already
    CREATE3Response nfsCREATEResponse1 = store.create(rootHandle, "create_guarded_file1", Nfs3Constant.CREATE_GUARDED, new Nfs3SetAttr(), 0L, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsCREATEResponse1.getStatus());
    
    // Create new file, check that it says that file already exists
    CREATE3Response nfsCREATEResponse2 = store.create(rootHandle, "create_guarded_file1", Nfs3Constant.CREATE_GUARDED, new Nfs3SetAttr(), 0L, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3ERR_EXIST, nfsCREATEResponse2.getStatus());
    
    // Create new file, it's ok if file already exists
    CREATE3Response nfsCREATEResponse3 = store.create(rootHandle, "create_guarded_file1", Nfs3Constant.CREATE_UNCHECKED, new Nfs3SetAttr(), 0L, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsCREATEResponse3.getStatus());
    
    // Create new file
    CREATE3Response nfsCREATEResponse4 = store.create(rootHandle, "create_guarded_file2", Nfs3Constant.CREATE_UNCHECKED, new Nfs3SetAttr(), 0L, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsCREATEResponse4.getStatus());
    
    // Create but no handle is sent back
    CREATE3Response nfsCREATEResponse5 = store.create(rootHandle, "sendnohandle", Nfs3Constant.CREATE_UNCHECKED, new Nfs3SetAttr(), 0L, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsCREATEResponse5.getStatus());
    
    // Invalid credentials
    boolean errored = true;
    try {
      store.create(rootHandle, "create_guarded_file3", Nfs3Constant.CREATE_UNCHECKED, new Nfs3SetAttr(), 0L, new CredentialsSys());
      errored = false;
    } catch(IOException exception) {
      assertTrue(errored);
    }
    
  }

  @Test
  public void COMMIT() throws Exception {
    FileHandle rootHandle = store.getRootfh();
    
    // Create a temporary file
    CREATE3Response nfsCREATEResponse = store.create(rootHandle, "commit_test", Nfs3Constant.CREATE_UNCHECKED, new Nfs3SetAttr(), 0L, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsCREATEResponse.getStatus());
    
    FileHandle fileHandle = nfsCREATEResponse.getObjHandle();
    byte[] largeData = new byte[65536];
    Random random = new Random();
    random.nextBytes(largeData);
    
    // Write some data and commit
    for(int i = 0; i < FileObject.MAX_FILE_SIZE; i += largeData.length) {
      WRITE3Response nfsWRITEResponse = store.write(fileHandle, (long) i, largeData.length, WriteStableHow.FILE_SYNC, largeData, new CredentialsNone());
      assertEquals(Nfs3Status.NFS3_OK, nfsWRITEResponse.getStatus());
      COMMIT3Response nfsCOMMITResponse = store.commit(fileHandle, (long) i, largeData.length, new CredentialsNone());
      assertEquals(Nfs3Status.NFS3_OK, nfsCOMMITResponse.getStatus());
    }
    
    // Commit using a bad handle
    COMMIT3Response nfsCOMMITResponse = store.commit(new Nfs3FileHandle(0L), 0L, 4096, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3ERR_BADHANDLE, nfsCOMMITResponse.getStatus());
    
    // Invalid credentials
    boolean errored = true;
    try {
      store.commit(fileHandle, (long) 0, largeData.length, new CredentialsSys());
      errored = false;
    } catch(IOException exception) {
      assertTrue(errored);
    }
    
  }
  
  @Test
  public void FSINFO() throws Exception {
    FileHandle handle = store.getRootfh();
    
    //Good response
    FSINFO3Response response = store.fsinfo(handle, store.getCredentials());
    assertEquals(Nfs3Status.NFS3_OK, response.getStatus());
    
    // Bad response due to bad handle
    FSINFO3Response response2 = store.fsinfo(new FileHandle(0), store.getCredentials());
    assertEquals(Nfs3Status.NFS3ERR_BADHANDLE, response2.getStatus());
    
    //Good response
    Nfs3Info info = store.getFilesystemInfo(handle, store.getCredentials());
    assertNotNull(info);
    
    // Bad response due to bad handle
    Nfs3Info info2 = store.getFilesystemInfo(new FileHandle(0), store.getCredentials());
    assertNull(info2);
    
  }
  
  
  @Test
  public void GETATTR() throws Exception {
    FileHandle rootHandle = store.getRootfh();
    
    // Create directory
    MKDIR3Response nfsMKDIRResponse = store.mkdir(rootHandle, "getattr1", new Nfs3SetAttr(), new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsMKDIRResponse.getStatus());
    
    // Get the attributes of directory
    GETATTR3Response nfsGETATTRResponse = store.getattr(nfsMKDIRResponse.getObjFileHandle(), new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsGETATTRResponse.getStatus());
    
    // Get the attributes of directory
    Nfs3FileAttributes nfsGETATTRResponse2 = store.getFileAttributes(nfsMKDIRResponse.getObjFileHandle(), new CredentialsNone());
    assertEquals(NfsFileType.NFSDIR.toValue(), nfsGETATTRResponse2.getType());
    assertEquals(MockNfs3Filesystem.MOCK_FSID, (int) nfsGETATTRResponse2.getFsid());
    
    // Bad file handle
    GETATTR3Response nfsGETATTRResponse3 = store.getattr(new Nfs3FileHandle(0), new CredentialsNone());
    assertEquals(Nfs3Status.NFS3ERR_BADHANDLE, nfsGETATTRResponse3.getStatus());
    
    // Bad file handle
    boolean errored = false;
    try {
      store.getFileAttributes(new Nfs3FileHandle(0L), new CredentialsNone());
    } catch(IOException exception) {
      errored = true;
    } finally {
      assertTrue(errored);
    }
    
    // Invalid credentials
    errored = true;
    try {
      store.getFileAttributes(nfsMKDIRResponse.getObjFileHandle(), new CredentialsSys());
      errored = false;
    } catch(IOException exception) {
      assertTrue(errored);
    }
    
  }
  
  @Test
  public void LOOKUP() throws Exception {
    FileHandle rootHandle = store.getRootfh();
    
    // Before directory creation
    LOOKUP3Response nfsLOOKUPResponse1 = store.lookup(rootHandle, "lookup1", new CredentialsNone());
    assertEquals(Nfs3Status.NFS3ERR_NOENT, nfsLOOKUPResponse1.getStatus());
    
    // Create directory
    MKDIR3Response nfsMKDIRResponse = store.mkdir(rootHandle, "lookup1", new Nfs3SetAttr(), new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsMKDIRResponse.getStatus());
    
    // Check again
    LOOKUP3Response nfsLOOKUPResponse2 = store.lookup(rootHandle, "lookup1", new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsLOOKUPResponse2.getStatus());
    
    // Invalid credentials
    boolean errored = true;
    try {
      store.lookup(rootHandle, "lookup1", new CredentialsSys());
      errored = false;
    } catch(IOException exception) {
      assertTrue(errored);
    }
    
  }
  
  @Test
  public void MKDIR() throws Exception {
    FileHandle rootHandle = store.getRootfh();
    
    // Create directory
    MKDIR3Response nfsMKDIRResponse = store.mkdir(rootHandle, "dir2", new Nfs3SetAttr(), new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsMKDIRResponse.getStatus());
    
    // Create invalid directory
    MKDIR3Response nfsMKDIRResponse2 = store.mkdir(new Nfs3FileHandle(0L), "dir2", new Nfs3SetAttr(), new CredentialsNone());
    assertEquals(Nfs3Status.NFS3ERR_BADHANDLE, nfsMKDIRResponse2.getStatus());
    
  }

  @Test
  public void NULL() throws Exception {
    store.nullOp();
  }

  @Test
  public void READ() throws Exception {
    FileHandle rootHandle = store.getRootfh();
    byte[] smallData = new byte[4096];
    byte[] mediumData = new byte[16384];
    byte[] largeData = new byte[65536];
    
    CREATE3Response nfsCREATEResponse1 = store.create(rootHandle, "read_test_file1", Nfs3Constant.CREATE_GUARDED, new Nfs3SetAttr(), 0L, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsCREATEResponse1.getStatus());
    
    // Get file handle
    FileHandle fileHandle = nfsCREATEResponse1.getObjHandle();
    
    // Initialize the data
    Random random = new Random();
    random.nextBytes(smallData);
    random.nextBytes(mediumData);
    random.nextBytes(largeData);
    
    // Write small data
    for(int i = 0; i < FileObject.MAX_FILE_SIZE; i += smallData.length) {
      WRITE3Response nfsWRITEResponse = store.write(fileHandle, (long) i, smallData.length, WriteStableHow.FILE_SYNC, smallData, new CredentialsNone());
      assertEquals(Nfs3Status.NFS3_OK, nfsWRITEResponse.getStatus());
      
      READ3Response nfsREADResponse = store.read(fileHandle, (long) i, smallData.length, new CredentialsNone());
      assertEquals(Nfs3Status.NFS3_OK, nfsREADResponse.getStatus());
      byte[] readData = nfsREADResponse.getData().array();
      for(int j = 0; j < smallData.length; ++j) {
        assertEquals(smallData[j], readData[j]);
      }
    }
    
    // Write medium sized data
    for(int i = 0; i < FileObject.MAX_FILE_SIZE; i += mediumData.length) {
      WRITE3Response nfsWRITEResponse = store.write(fileHandle, (long) i, mediumData.length, WriteStableHow.FILE_SYNC, mediumData, new CredentialsNone());
      assertEquals(Nfs3Status.NFS3_OK, nfsWRITEResponse.getStatus());
      
      READ3Response nfsREADResponse = store.read(fileHandle, (long) i, mediumData.length, new CredentialsNone());
      assertEquals(Nfs3Status.NFS3_OK, nfsREADResponse.getStatus());
      byte[] readData = nfsREADResponse.getData().array();
      for(int j = 0; j < mediumData.length; ++j) {
        assertEquals(mediumData[j], readData[j]);
      }
    }
    
    // Write large sized data
    for(int i = 0; i < FileObject.MAX_FILE_SIZE; i += largeData.length) {
      WRITE3Response nfsWRITEResponse = store.write(fileHandle, (long) i, largeData.length, WriteStableHow.FILE_SYNC, largeData, new CredentialsNone());
      assertEquals(Nfs3Status.NFS3_OK, nfsWRITEResponse.getStatus());
      
      READ3Response nfsREADResponse = store.read(fileHandle, (long) i, largeData.length, new CredentialsNone());
      assertEquals(Nfs3Status.NFS3_OK, nfsREADResponse.getStatus());
      byte[] readData = nfsREADResponse.getData().array();
      for(int j = 0; j < largeData.length; ++j) {
        assertEquals(largeData[j], readData[j]);
      }
    }
    
    // Read beyond the file
    READ3Response nfsREADResponse = store.read(fileHandle, (long) FileObject.MAX_FILE_SIZE, 1, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3ERR_INVAL, nfsREADResponse.getStatus());
    
    // Invalid credentials
    boolean errored = true;
    try {
      store.read(fileHandle, (long) FileObject.MAX_FILE_SIZE, 1, new CredentialsNone());
      errored = false;
    } catch(IOException exception) {
      assertTrue(errored);
    }
    
  }
  
  @Test
  public void RENAME() throws Exception {
    
    FileHandle rootHandle = store.getRootfh();
    
    // Create a test file
    CREATE3Response nfsCREATEResponse1 = store.create(rootHandle, "rename1", Nfs3Constant.CREATE_UNCHECKED, new Nfs3SetAttr(), 0L, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsCREATEResponse1.getStatus());
    
    // Rename existing file
    RENAME3Response nfsRENAMEResponse1 = store.rename(rootHandle, "rename1", rootHandle, "rename2", new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsRENAMEResponse1.getStatus());
    
    // Rename unknown file
    RENAME3Response nfsRENAMEResponse2 = store.rename(rootHandle, "renameblahblah", rootHandle, "renamefoobar", new CredentialsNone());
    assertEquals(Nfs3Status.NFS3ERR_NOENT, nfsRENAMEResponse2.getStatus());
    
    // Invalid credentials
    boolean errored = true;
    try {
      store.rename(rootHandle, "renameblahblah", rootHandle, "renamefoobar", new CredentialsSys());
      errored = false;
    } catch(IOException exception) {
      assertTrue(errored);
    }
    
  }
  
  @Test
  public void REMOVE() throws Exception {
    FileHandle rootHandle = store.getRootfh();
    
    // Create a test file
    CREATE3Response nfsCREATEResponse1 = store.create(rootHandle, "remove1", Nfs3Constant.CREATE_UNCHECKED, new Nfs3SetAttr(), 0L, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsCREATEResponse1.getStatus());
    
    // Remove existing file
    REMOVE3Response nfsREMOVEResponse1 = store.remove(rootHandle, "remove1", new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsREMOVEResponse1.getStatus());
    
    LOOKUP3Response nfsLOOKUPResponse1 = store.lookup(rootHandle, "remove1", new CredentialsNone());
    assertEquals(Nfs3Status.NFS3ERR_NOENT, nfsLOOKUPResponse1.getStatus());
    
    // Remove unknown file
    REMOVE3Response nfsREMOVEResponse2 = store.remove(rootHandle, "remove2", new CredentialsNone());
    assertEquals(Nfs3Status.NFS3ERR_NOENT, nfsREMOVEResponse2.getStatus());
    
    // Invalid credentials
    boolean errored = true;
    try {
      store.remove(rootHandle, "remove2", new CredentialsSys());
      errored = false;
    } catch(IOException exception) {
      assertTrue(errored);
    }
    
  }
  
  @Test
  public void RMDIR() throws Exception {
    FileHandle rootHandle = store.getRootfh();
    
    // Create directory
    MKDIR3Response nfsMKDIRResponse = store.mkdir(rootHandle, "rmdir1", new Nfs3SetAttr(), new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsMKDIRResponse.getStatus());
    
    // Remove existing directory
    RMDIR3Response nfsRMDIRResponse1 = store.rmdir(rootHandle, "rmdir1", new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsRMDIRResponse1.getStatus());
    
    LOOKUP3Response nfsLOOKUPResponse1 = store.lookup(rootHandle, "rmdir1", new CredentialsNone());
    assertEquals(Nfs3Status.NFS3ERR_NOENT, nfsLOOKUPResponse1.getStatus());
    
    // Remove unknown directory
    RMDIR3Response nfsRMDIRResponse2 = store.rmdir(rootHandle, "rmdir2", new CredentialsNone());
    assertEquals(Nfs3Status.NFS3ERR_NOENT, nfsRMDIRResponse2.getStatus());
    
    // Invalid credentials
    boolean errored = true;
    try {
      store.rmdir(rootHandle, "rmdir3", new CredentialsSys());
      errored = false;
    } catch(IOException exception) {
      assertTrue(errored);
    }
    
  }
  
  @Test
  public void SETATTR() throws Exception {
    FileHandle rootHandle = store.getRootfh();
    
    // Create a test file
    CREATE3Response nfsCREATEResponse1 = store.create(rootHandle, "setattr1", Nfs3Constant.CREATE_UNCHECKED, new Nfs3SetAttr(), 0L, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsCREATEResponse1.getStatus());
    
    // Set attributes
    Nfs3SetAttr attr = new Nfs3SetAttr();
    attr.setUid(100);
    attr.setGid(100);
    EnumSet<SetAttrField> updates = EnumSet.of(SetAttrField.UID, SetAttrField.GID);
    attr.setUpdateFields(updates);
    SETATTR3Response nfsSETATTRResponse = store.setattr(nfsCREATEResponse1.getObjHandle(), attr, false, new NfsTime(System.currentTimeMillis()), new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsSETATTRResponse.getStatus());
    assertEquals(100, nfsSETATTRResponse.getWccData().getPostOpAttr().getGid());
    assertEquals(100, nfsSETATTRResponse.getWccData().getPostOpAttr().getUid());
    
    // Set attr with time check
    Nfs3SetAttr attr2 = new Nfs3SetAttr();
    attr2.setUid(100);
    attr2.setGid(100);
    EnumSet<SetAttrField> updates2 = EnumSet.of(SetAttrField.UID, SetAttrField.GID);
    attr2.setUpdateFields(updates2);
    SETATTR3Response nfsSETATTRResponse2 = store.setattr(nfsCREATEResponse1.getObjHandle(), attr2, true, new NfsTime(System.currentTimeMillis()), new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsSETATTRResponse2.getStatus());
    assertEquals(100, nfsSETATTRResponse2.getWccData().getPostOpAttr().getGid());
    assertEquals(100, nfsSETATTRResponse2.getWccData().getPostOpAttr().getUid());
    
    // Invalid credentials
    boolean errored = true;
    try {
      store.setattr(nfsCREATEResponse1.getObjHandle(), attr, false, new NfsTime(System.currentTimeMillis()), new CredentialsSys());
      errored = false;
    } catch(IOException exception) {
      assertTrue(errored);
    }
    
  }
  
  @Test
  public void WRITE() throws Exception {
    FileHandle rootHandle = store.getRootfh();
    byte[] smallData = new byte[4096];
    byte[] mediumData = new byte[16384];
    byte[] largeData = new byte[65536];
    
    CREATE3Response nfsCREATEResponse1 = store.create(rootHandle, "write_test_file1", Nfs3Constant.CREATE_GUARDED, new Nfs3SetAttr(), 0L, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3_OK, nfsCREATEResponse1.getStatus());
    
    // Get file handle
    FileHandle fileHandle = nfsCREATEResponse1.getObjHandle();
    
    // Initialize the data
    Random random = new Random();
    random.nextBytes(smallData);
    random.nextBytes(mediumData);
    random.nextBytes(largeData);
    
    // Write small data
    for(int i = 0; i < FileObject.MAX_FILE_SIZE; i += smallData.length) {
      WRITE3Response nfsWRITEResponse = store.write(fileHandle, (long) i, smallData.length, WriteStableHow.FILE_SYNC, smallData, new CredentialsNone());
      assertEquals(Nfs3Status.NFS3_OK, nfsWRITEResponse.getStatus());
      
      Nfs3FileAttributes nfsGETATTRResponse = store.getFileAttributes(fileHandle, new CredentialsNone());
      assertEquals((long) (i + smallData.length), nfsGETATTRResponse.getSize());
      
    }
    
    for(int i = 0; i < FileObject.MAX_FILE_SIZE; i += mediumData.length) {
      WRITE3Response nfsWRITEResponse = store.write(fileHandle, (long) i, mediumData.length, WriteStableHow.FILE_SYNC, mediumData, new CredentialsNone());
      assertEquals(Nfs3Status.NFS3_OK, nfsWRITEResponse.getStatus());
    }
    
    for(int i = 0; i < FileObject.MAX_FILE_SIZE; i += largeData.length) {
      WRITE3Response nfsWRITEResponse = store.write(fileHandle, (long) i, largeData.length, WriteStableHow.FILE_SYNC, largeData, new CredentialsNone());
      assertEquals(Nfs3Status.NFS3_OK, nfsWRITEResponse.getStatus());
    }
    
    // Write beyond the file
    WRITE3Response nfsWRITEResponse = store.write(fileHandle, (long) FileObject.MAX_FILE_SIZE, largeData.length, WriteStableHow.FILE_SYNC, largeData, new CredentialsNone());
    assertEquals(Nfs3Status.NFS3ERR_FBIG, nfsWRITEResponse.getStatus());
    
    boolean errored = true;
    try {
      store.write(fileHandle, (long) FileObject.MAX_FILE_SIZE, largeData.length, WriteStableHow.FILE_SYNC, largeData, new CredentialsSys());
      errored = false;
    } catch(IOException exception) {
      assertTrue(errored);
    }
  }
  
  
}
