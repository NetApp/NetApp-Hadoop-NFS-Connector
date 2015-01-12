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

package org.apache.hadoop.fs.nfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3DirList;
import org.apache.hadoop.nfs.nfs3.Nfs3DirList.Nfs3DirEntry;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Info;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.response.COMMIT3Response;
import org.apache.hadoop.nfs.nfs3.response.CREATE3Response;
import org.apache.hadoop.nfs.nfs3.response.FSINFO3Response;
import org.apache.hadoop.nfs.nfs3.response.GETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.LOOKUP3Response;
import org.apache.hadoop.nfs.nfs3.response.MKDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.READ3Response;
import org.apache.hadoop.nfs.nfs3.response.READDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.READDIR3Response.DirList3;
import org.apache.hadoop.nfs.nfs3.response.READDIR3Response.Entry3;
import org.apache.hadoop.nfs.nfs3.response.REMOVE3Response;
import org.apache.hadoop.nfs.nfs3.response.RENAME3Response;
import org.apache.hadoop.nfs.nfs3.response.RMDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.SETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;
import org.apache.hadoop.nfs.nfs3.response.WccAttr;
import org.apache.hadoop.nfs.nfs3.response.WccData;
import org.apache.hadoop.oncrpc.XDR;

public class NFSv3ResponseBuilder {

  public final static Log LOG = LogFactory.getLog(NFSv3ResponseBuilder.class);
  
  public static COMMIT3Response buildCOMMIT3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    
    if(status == Nfs3Status.NFS3_OK) {
      WccData fileWcc;
      long verf;
      fileWcc = buildWccData(xdr);
      verf = xdr.readHyper();
      return new COMMIT3Response(status, fileWcc, verf);
    } else {
      WccData fileWcc;
      fileWcc = buildWccData(xdr);
      return new COMMIT3Response(status, fileWcc, Nfs3Constant.WRITE_COMMIT_VERF);
    }
  }
  
  public static CREATE3Response buildCREATE3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    
    if(status == Nfs3Status.NFS3_OK) {
      FileHandle handle = null;
      Nfs3FileAttributes postOpObjAttr;
      WccData dirWcc;
      if(xdr.readBoolean()) {
        handle = buildFileHandle(xdr);
      }
      postOpObjAttr = buildPostOpAttributes(xdr);      
      dirWcc = buildWccData(xdr);
      return new CREATE3Response(status, handle, postOpObjAttr, dirWcc);
    } else {
      WccData dirWcc;
      dirWcc = buildWccData(xdr);
      return new CREATE3Response(status, null, null, dirWcc);
    }
  }
  
  public static FSINFO3Response buildFSINFO3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    
    if(status == Nfs3Status.NFS3_OK) {
      Nfs3FileAttributes postOpAttr;
      int rtmax, rtperf, rtmult, wtmax, wtperf, wtmult, dtperf;
      long maxFileSize;
      NfsTime timeDelta;
      int properties;

      postOpAttr = buildPostOpAttributes(xdr);
      rtmax = xdr.readInt();
      rtperf = xdr.readInt();
      rtmult = xdr.readInt();
      wtmax = xdr.readInt();
      wtperf = xdr.readInt();
      wtmult = xdr.readInt();
      dtperf = xdr.readInt();
      maxFileSize = xdr.readHyper();
      timeDelta = NfsTime.deserialize(xdr);
      properties = xdr.readInt();
      return new FSINFO3Response(status, postOpAttr, rtmax, rtperf, rtmult, wtmax, wtperf, wtmult, dtperf, maxFileSize, timeDelta, properties);
    } else {
      Nfs3FileAttributes postOpAttr;
      postOpAttr = buildPostOpAttributes(xdr);
      return new FSINFO3Response(status, postOpAttr,  0, 0, 0, 0, 0, 0, 0, 0, null, 0);
    }
  }
  
  public static GETATTR3Response buildGETATTR3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    
    if(status == Nfs3Status.NFS3_OK) {
      Nfs3FileAttributes attrs;
      attrs = Nfs3FileAttributes.deserialize(xdr);
      return new GETATTR3Response(status, attrs);
    } else {
      return new GETATTR3Response(status);
    }
  }
  
  public static LOOKUP3Response buildLOOKUP3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    
    if(status == Nfs3Status.NFS3_OK) {
      FileHandle fileHandle = buildFileHandle(xdr);
      Nfs3FileAttributes postOpObjAttr = buildPostOpAttributes(xdr);
      Nfs3FileAttributes postOpDirAttributes = buildPostOpAttributes(xdr);
      return new LOOKUP3Response(status, fileHandle, postOpObjAttr, postOpDirAttributes);
    } else {
      return new LOOKUP3Response(status);
    }
  }
  
  public static MKDIR3Response buildMKDIR3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    
    if(status == Nfs3Status.NFS3_OK) {
      boolean hasFileHandle;
      FileHandle handle = null;
      Nfs3FileAttributes attr;
      WccData dirWcc;
      
      hasFileHandle = xdr.readBoolean();
      if(hasFileHandle) {
        handle = new Nfs3FileHandle();
        handle.deserialize(xdr);
      }
      attr = Nfs3FileAttributes.deserialize(xdr);
      dirWcc = buildWccData(xdr);
      return new MKDIR3Response(status, handle, attr, dirWcc);
    } else {
      return new MKDIR3Response(status);
    }
  }
  
  public static READ3Response buildREAD3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    
    if(status == Nfs3Status.NFS3_OK) {
      Nfs3FileAttributes postOpAttr;
      int count;
      boolean eof;
      ByteBuffer data;
      
      postOpAttr = buildPostOpAttributes(xdr);
      count = xdr.readInt();
      eof = xdr.readBoolean();
      data = ByteBuffer.wrap(xdr.readVariableOpaque());
      return new READ3Response(status, postOpAttr, count, eof, data);
    } else {
      Nfs3FileAttributes postOpAttr = buildPostOpAttributes(xdr);
      return new READ3Response(status, postOpAttr, 0, false, null);
    }
  }
  
  public static READDIR3Response buildREADDIR3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    
    if(status == Nfs3Status.NFS3_OK) {
      Nfs3FileAttributes postOpAttr;
      long cookieVerf;
      List<Entry3> entries;
      DirList3 dirList;
      boolean eof;
      
      postOpAttr = buildPostOpAttributes(xdr);
      cookieVerf = xdr.readHyper();
      entries = buildDirEntryList(xdr);
      eof = xdr.readBoolean();
      dirList = new DirList3(entries.toArray(new Entry3[] {}), eof);
      return new READDIR3Response(status, postOpAttr, cookieVerf, dirList);
    } else {
      Nfs3FileAttributes postOpAttr;
      postOpAttr = Nfs3FileAttributes.deserialize(xdr);
      return new READDIR3Response(status, postOpAttr);
    }
  }
  
  public static REMOVE3Response buildREMOVE3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    WccData wccData = buildWccData(xdr);
    return new REMOVE3Response(status, wccData);
  }
  
  public static RENAME3Response buildRENAME3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    WccData fromWccData, toWccData;
    fromWccData = buildWccData(xdr);
    toWccData = buildWccData(xdr);
    return new RENAME3Response(status, fromWccData, toWccData);
  }
  
  public static RMDIR3Response buildRMDIR3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    WccData wccData = buildWccData(xdr);
    return new RMDIR3Response(status, wccData);
  }
  
  public static SETATTR3Response buildSETATTR3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    WccData wccData = buildWccData(xdr);
    return new SETATTR3Response(status, wccData);
  }
  
  public static WRITE3Response buildWRITE3Response(XDR xdr) throws IOException {
    int status = xdr.readInt();
    
    if(status == Nfs3Status.NFS3_OK) {
      WccData fileWcc;
      int count;
      WriteStableHow stableHow;
      long verifier;
      
      fileWcc = buildWccData(xdr);
      count = xdr.readInt();
      stableHow = WriteStableHow.fromValue(xdr.readInt());
      verifier = xdr.readHyper();
      return new WRITE3Response(status, fileWcc, count, stableHow, verifier);
    } else {
      WccData fileWcc;
      fileWcc = buildWccData(xdr);
      return new WRITE3Response(status, fileWcc, 0, WriteStableHow.UNSTABLE, Nfs3Constant.WRITE_COMMIT_VERF);
    }
  }
  
  public static FileHandle buildFileHandle(XDR xdr) throws IOException {
    FileHandle handle = new Nfs3FileHandle();
    handle.deserialize(xdr);
    return handle;
  }
  
  public static Nfs3Info buildFilesystemInfo(XDR xdr) throws IOException {
    int status = xdr.readInt();
    
    if(status == Nfs3Status.NFS3_OK) {
      int rtmax, rtperf, rtmult, wtmax, wtperf, wtmult, dtperf;
      long maxFileSize;
      NfsTime timeDelta;
      int properties;

      buildPostOpAttributes(xdr);
      rtmax = xdr.readInt();
      rtperf = xdr.readInt();
      rtmult = xdr.readInt();
      wtmax = xdr.readInt();
      wtperf = xdr.readInt();
      wtmult = xdr.readInt();
      dtperf = xdr.readInt();
      maxFileSize = xdr.readHyper();
      timeDelta = NfsTime.deserialize(xdr);
      properties = xdr.readInt();
      return new Nfs3Info(rtmax, rtperf, rtmult, wtmax, wtperf, wtmult, dtperf, maxFileSize, timeDelta, properties);
    } else {
      buildPostOpAttributes(xdr);
      return null;
    }
  }
  
  public static Nfs3DirList buildDirectoryList(XDR xdr) throws IOException {
    int status = xdr.readInt();
    
    if(status == Nfs3Status.NFS3_OK) {
      List<Nfs3DirEntry> entries;
      Nfs3DirList dirList;
      boolean eof;
      long cookieVerf;
      
      buildPostOpAttributes(xdr);
      cookieVerf = xdr.readHyper();
      entries = new LinkedList<Nfs3DirEntry>();
      
      while(xdr.readBoolean()) {
        long fileId = xdr.readHyper();
        byte[] name = xdr.readVariableOpaque();
        long cookie = xdr.readHyper();
        Nfs3DirEntry entry = new Nfs3DirEntry(fileId, new String(name), cookie);
        entries.add(entry);
      }
      eof = xdr.readBoolean();
      dirList = new Nfs3DirList(entries.toArray(new Nfs3DirEntry[] {}), cookieVerf, eof);
      return dirList;
    } else {
      Nfs3FileAttributes.deserialize(xdr);
      return null;
    }
  }
  
  public static WccData buildWccData(XDR xdr) throws IOException {
    WccAttr preOpAttr;
    Nfs3FileAttributes postOpAttr;
    
    boolean b1 = xdr.readBoolean();
    if(b1 == true) {
      preOpAttr = buildWccAttr(xdr);
    } else {
      preOpAttr = null;
    }
    
    boolean b2 = xdr.readBoolean();
    if(b2 == true) {
      postOpAttr = Nfs3FileAttributes.deserialize(xdr);
    } else {
      postOpAttr = null;
    }
    
    return new WccData(preOpAttr, postOpAttr);
    
  }
  
  
  public static WccAttr buildWccAttr(XDR xdr) throws IOException {
    long size;
    NfsTime mtime;
    NfsTime ctime;
    
    size = xdr.readHyper();
    mtime = NfsTime.deserialize(xdr);
    ctime = NfsTime.deserialize(xdr);
    
    return new WccAttr(size, mtime, ctime);
    
  }
  
  public static Nfs3FileAttributes buildPostOpAttributes(XDR xdr) throws IOException {
    boolean hasPostOpAttr;
    Nfs3FileAttributes postOpAttributes;
    
    hasPostOpAttr = xdr.readBoolean();
    if(hasPostOpAttr == true) {
      postOpAttributes = Nfs3FileAttributes.deserialize(xdr);
    } else {
      postOpAttributes = null;
    }
    
    return postOpAttributes;
  }
  
  public static List<Entry3> buildDirEntryList(XDR xdr) throws IOException {
    List<Entry3> entries = new LinkedList<Entry3>();
    boolean haveMore = xdr.readBoolean();
    System.out.println("Has list of entries=" + haveMore);
    while(haveMore) {
      long fileId = xdr.readHyper();
      byte[] name = xdr.readVariableOpaque();
      long cookie = xdr.readHyper();
      Entry3 entry = new Entry3(fileId, new String(name), cookie);
      entries.add(entry);
      haveMore = xdr.readBoolean();
      System.out.println("haveMore=" + haveMore);
    }
    return entries;
  }
  
}
