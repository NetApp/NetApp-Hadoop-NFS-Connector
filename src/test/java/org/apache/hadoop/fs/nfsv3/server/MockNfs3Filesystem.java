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

package org.apache.hadoop.fs.nfsv3.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.nfs.mount.MountMNTResponse;
import org.apache.hadoop.fs.nfsv3.server.FsObject.FileType;
import org.apache.hadoop.mount.MountResponse;
import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.COMMIT3Request;
import org.apache.hadoop.nfs.nfs3.request.CREATE3Request;
import org.apache.hadoop.nfs.nfs3.request.FSINFO3Request;
import org.apache.hadoop.nfs.nfs3.request.GETATTR3Request;
import org.apache.hadoop.nfs.nfs3.request.LOOKUP3Request;
import org.apache.hadoop.nfs.nfs3.request.MKDIR3Request;
import org.apache.hadoop.nfs.nfs3.request.READ3Request;
import org.apache.hadoop.nfs.nfs3.request.READDIR3Request;
import org.apache.hadoop.nfs.nfs3.request.REMOVE3Request;
import org.apache.hadoop.nfs.nfs3.request.RENAME3Request;
import org.apache.hadoop.nfs.nfs3.request.RMDIR3Request;
import org.apache.hadoop.nfs.nfs3.request.SETATTR3Request;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
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
import org.apache.hadoop.oncrpc.security.RpcAuthInfo.AuthFlavor;

public class MockNfs3Filesystem {

  final Map<Long,FsObject> contents;
  final DirectoryObject root;
  public static final int MOCK_FSID = 1000;
  
  public MockNfs3Filesystem() {
    contents = new HashMap<Long,FsObject>();
    root = new DirectoryObject("/");
    contents.put(root.getId(), root);
  }
  
  public MountMNTResponse getRoot() throws IOException {
    return new MountMNTResponse(MountResponse.MNT_OK, new Nfs3FileHandle(root.getId()), new int[] {AuthFlavor.AUTH_NONE.getValue()});
  }
  
  public CREATE3Response CREATE(CREATE3Request request) throws IOException {
    FileHandle handle = request.getHandle();
    String filename = request.getName();
    int mode = request.getMode();
    
    if(contents.containsKey(handle.getFileId())) {
      
      // Check contents of the directory
      DirectoryObject dirObject;
      FileObject newFile = null;
      
      FsObject fsObject = contents.get(handle.getFileId());
      if(fsObject.getType() != FileType.TYPE_DIRECTORY) {
        return new CREATE3Response(Nfs3Status.NFS3ERR_NOTDIR);
      }
      dirObject = (DirectoryObject) fsObject;
      
      for(FsObject obj : dirObject.listDirectoryContents()) {
        if(obj.getFilename().equals(filename)) {
          if(obj instanceof FileObject) {
            newFile = (FileObject) obj;
            break;
          } else {
            return new CREATE3Response(Nfs3Status.NFS3ERR_IO);
          }
        }
      }
      
      // Don't check if the file already exists
      if(mode == Nfs3Constant.CREATE_UNCHECKED) {
        WccAttr preOpAttr = dirObject.getWccAttr();
        
        if(newFile != null) {
          dirObject.removeFileFromDirectory(newFile);
          contents.remove(newFile.getId());
        }
        
        newFile = new FileObject(filename);
        newFile.setAttr(request.getObjAttr());
        dirObject.addNewFile(newFile);
        contents.put(newFile.getId(), newFile);
        WccData dirWcc = new WccData(preOpAttr, dirObject.getAttr());
         return new CREATE3Response(Nfs3Status.NFS3_OK, new Nfs3FileHandle(newFile.getId()), newFile.getAttr(), dirWcc);
      }
      // Checks if the file already exists; error otherwise
      else if(mode == Nfs3Constant.CREATE_GUARDED) {
        if(newFile != null) {
          return new CREATE3Response(Nfs3Status.NFS3ERR_EXIST);
        } 
        else {
          WccAttr preOpAttr = dirObject.getWccAttr();
          
          newFile = new FileObject(filename);
          newFile.setAttr(request.getObjAttr());
          dirObject.addNewFile(newFile);
          contents.put(newFile.getId(), newFile);
          WccData dirWcc = new WccData(preOpAttr, dirObject.getAttr());
          return new CREATE3Response(Nfs3Status.NFS3_OK, new Nfs3FileHandle(newFile.getId()), newFile.getAttr(), dirWcc);
        }
      }
      // TODO: Exclusive creation not supported in this test
      else {
        return new CREATE3Response(Nfs3Status.NFS3ERR_IO);
      }
    } else {
      return new CREATE3Response(Nfs3Status.NFS3ERR_BADHANDLE);
    }
  }
  
  public COMMIT3Response COMMIT(COMMIT3Request request) throws IOException {
    
    FileHandle handle = request.getHandle();
    
    if(contents.containsKey(handle.getFileId())) {
      FsObject obj = contents.get(handle.getFileId());
      if(obj instanceof FileObject) {
        FileObject file = (FileObject) obj;
        WccAttr wccAttr = file.getWccAttr();
        return new COMMIT3Response(Nfs3Status.NFS3_OK, new WccData(wccAttr, file.getAttr()), 0L); 
      } else {
        return new COMMIT3Response(Nfs3Status.NFS3ERR_IO);
      }
    } else {
      return new COMMIT3Response(Nfs3Status.NFS3ERR_BADHANDLE);
    }
    
  }
  
  public FSINFO3Response FSINFO(FSINFO3Request request) throws IOException {
    FileHandle handle = request.getHandle();
    
    if(contents.containsKey(handle.getFileId())) {
      if(handle.getFileId() == root.getId()) {
        int rtmax = 65536;
        int rtpref = 4096;
        int rtmult = 1;
        int wtmax = 65536;
        int wtpref = 4096;
        int wtmult = 1;
        int dtperf = 4096;
        long maxfilesize = 10 * 1024 * 1024;
        NfsTime delta = new NfsTime(1);
        int properties = 0;
        return new FSINFO3Response(Nfs3Status.NFS3_OK, new Nfs3FileAttributes(),
            rtmax, rtpref, rtmult, wtmax, wtpref, wtmult, dtperf, maxfilesize,
            delta, properties);
      } else {
        return new FSINFO3Response(Nfs3Status.NFS3ERR_BADHANDLE);
      }
    } else {
      return new FSINFO3Response(Nfs3Status.NFS3ERR_BADHANDLE);
    }
  }
  
  public GETATTR3Response GETATTR(GETATTR3Request request) throws IOException {
    FileHandle handle = request.getHandle();
    if(contents.containsKey(handle.getFileId())) {
      FsObject obj = contents.get(handle.getFileId());
      return new GETATTR3Response(Nfs3Status.NFS3_OK, obj.getAttr());
    } else {
      return new GETATTR3Response(Nfs3Status.NFS3ERR_BADHANDLE);
    }
  }
  
  public LOOKUP3Response LOOKUP(LOOKUP3Request request) throws IOException {
    FileHandle handle = request.getHandle();
    String name = request.getName();
    
    if(contents.containsKey(handle.getFileId())) {
      FsObject obj = contents.get(handle.getFileId());
      if(obj.getType() != FileType.TYPE_DIRECTORY) {
        return new LOOKUP3Response(Nfs3Status.NFS3ERR_NOTDIR);
      } else {
        DirectoryObject dir = (DirectoryObject) obj;
        FsObject objInDir = dir.getItemInDirectory(name);
        if(objInDir == null) {
          return new LOOKUP3Response(Nfs3Status.NFS3ERR_NOENT);
        } else {
          return new LOOKUP3Response(Nfs3Status.NFS3_OK, new Nfs3FileHandle(obj.getId()), objInDir.getAttr(), dir.getAttr());
        }
      }
    } else {
      return new LOOKUP3Response(Nfs3Status.NFS3ERR_BADHANDLE);
    }
  }
  
  public MKDIR3Response MKDIR(MKDIR3Request request) throws IOException {
    FileHandle handle = request.getHandle();
    String dirName = request.getName();
    SetAttr3 attr = request.getObjAttr();
    WccAttr preAttr;
    
    if(contents.containsKey(handle.getFileId())) {
      // Find parent directory
      FsObject obj = contents.get(handle.getFileId());
      assert(obj.getType() == FileType.TYPE_DIRECTORY);
      
      // Create new directory
      DirectoryObject newDir = new DirectoryObject(dirName);
      newDir.setAttr(attr);
      contents.put(newDir.getId(), newDir);

      // Add to parent
      DirectoryObject parentDir = (DirectoryObject) obj;
      preAttr = parentDir.getWccAttr();
      parentDir.addNewDirectory(newDir);
      WccData postOpAttr = new WccData(preAttr, parentDir.getAttr());
      return new MKDIR3Response(Nfs3Status.NFS3_OK, new Nfs3FileHandle(newDir.getId()), newDir.getAttr(), postOpAttr);
    } else {
     return new MKDIR3Response(Nfs3Status.NFS3ERR_BADHANDLE); 
    }
  }
  
  public READ3Response READ(READ3Request request) throws IOException {
    FileHandle handle = request.getHandle();
    long offset = request.getOffset();
    int count = request.getCount();
    
    if(contents.containsKey(handle.getFileId())) {
      FsObject fsObject = contents.get(handle.getFileId());
      
      if(fsObject instanceof FileObject) {
        FileObject fileObject = (FileObject) fsObject;
        byte[] readData = new byte[FsObject.MAX_FILE_SIZE];
        int readBytes;
        try {
          readBytes = fileObject.read(offset, count, readData);
        } catch(IOException exception) {
          return new READ3Response(Nfs3Status.NFS3ERR_INVAL);
        }
        return new READ3Response(Nfs3Status.NFS3_OK, fileObject.getAttr(), readBytes, ((offset + count) >= fileObject.getSize()), ByteBuffer.wrap(readData));
      } 
      else {
       return new READ3Response(Nfs3Status.NFS3ERR_BADHANDLE); 
      }
    } else {
      return new READ3Response(Nfs3Status.NFS3ERR_BADHANDLE);
    }
  }
  
  public READDIR3Response READDIR(READDIR3Request request) throws IOException {
    FileHandle handle = request.getHandle();
    long cookie = request.getCookie();
    
    if(contents.containsKey(handle.getFileId())) {
      FsObject fsObject = contents.get(handle.getFileId());
      if(fsObject instanceof DirectoryObject) {
        DirectoryObject dirObject = (DirectoryObject) fsObject;
        Set<FsObject> entries = new TreeSet<FsObject>(dirObject.listDirectoryContents());
        
        int position = 0;
        Entry3[] array = new Entry3[6];
        for(FsObject entry : entries) {
          if(entry.getId() > cookie) {
            array[position] = new Entry3(entry.getId(), entry.getFilename(), entry.getId());
            position++;
            if(position >= 5) {
              break;
            }
          }
        }
        
        DirList3 dirList;
        if(position == 0) {
          dirList = new DirList3(null, true);
        } else {
          dirList = new DirList3(Arrays.copyOf(array, position), false);
        }
        
        return new READDIR3Response(Nfs3Status.NFS3_OK, dirObject.getAttr(), 0L, dirList);
        
      } else {
        return new READDIR3Response(Nfs3Status.NFS3ERR_NOTDIR);
      }
    } else {
      return new READDIR3Response(Nfs3Status.NFS3ERR_BADHANDLE);
    }
  }

  public RENAME3Response RENAME(RENAME3Request request) throws IOException {
    FileHandle fromDirHandle = request.getFromDirHandle();
    String fromName = request.getFromName();
    FileHandle toDirHandle = request.getToDirHandle();
    String toName = request.getToName();
    
    if(contents.containsKey(fromDirHandle.getFileId()) && contents.containsKey(toDirHandle.getFileId())) {
      DirectoryObject fsFromDir = (DirectoryObject) contents.get(fromDirHandle.getFileId());
      DirectoryObject fsToDir = (DirectoryObject) contents.get(toDirHandle.getFileId());

      // Must exist in old directory
      if(fsFromDir.getItemInDirectory(fromName) == null) {
        return new RENAME3Response(Nfs3Status.NFS3ERR_NOENT);
      }
      
      // Must not exist in new directory
      if(fsToDir.getItemInDirectory(toName) != null) {
        return new RENAME3Response(Nfs3Status.NFS3ERR_EXIST);
      }
      
      // Remove it from old one
      for(FsObject obj : fsFromDir.listDirectoryContents()) {
        if(obj.getFilename().equals(fromName)) {
          if(obj instanceof FileObject) {
            FileObject file = (FileObject) obj;
            WccAttr fromPreOpAttr = fsFromDir.getWccAttr();
            WccAttr toPreOpAttr = fsToDir.getWccAttr();
            fsFromDir.removeFileFromDirectory(file); 
            fsToDir.addNewFile(file);
            return new RENAME3Response(Nfs3Status.NFS3_OK, new WccData(fromPreOpAttr, fsFromDir.getAttr()), new WccData(toPreOpAttr, fsToDir.getAttr()));
          } else {
            return new RENAME3Response(Nfs3Status.NFS3ERR_IO);
          }
        }
      }
      
      return new RENAME3Response(Nfs3Status.NFS3ERR_NOENT);
      
    } else {
      return new RENAME3Response(Nfs3Status.NFS3ERR_BADHANDLE);
    }
    
  }

  public REMOVE3Response REMOVE(REMOVE3Request request) throws IOException {
    FileHandle dirHandle = request.getHandle();
    String name = request.getName();
    
    if(contents.containsKey(dirHandle.getFileId())) {
      FsObject obj = contents.get(dirHandle.getFileId());
      if(obj instanceof DirectoryObject) {
        DirectoryObject dir = (DirectoryObject) obj;
        for(FsObject f : dir.listDirectoryContents()) {
          if(f.getFilename().equals(name) && (f instanceof FileObject)) {
            WccAttr preOpAttr = dir.getWccAttr();
            dir.removeFileFromDirectory((FileObject) f);
            return new REMOVE3Response(Nfs3Status.NFS3_OK, new WccData(preOpAttr, dir.getAttr()));
          }
        }
        return new REMOVE3Response(Nfs3Status.NFS3ERR_NOENT);
      } else {
        return new REMOVE3Response(Nfs3Status.NFS3ERR_NOTDIR);
      }
    } else {
      return new REMOVE3Response(Nfs3Status.NFS3ERR_BADHANDLE);
    }
  }
  
  public RMDIR3Response RMDIR(RMDIR3Request request) throws IOException {
    FileHandle dirHandle = request.getHandle();
    String name = request.getName();
    
    if(contents.containsKey(dirHandle.getFileId())) {
      FsObject obj = contents.get(dirHandle.getFileId());
      if(obj instanceof DirectoryObject) {
        DirectoryObject dir = (DirectoryObject) obj;
        for(FsObject f : dir.listDirectoryContents()) {
          if(f.getFilename().equals(name) && (f instanceof DirectoryObject)) {
            WccAttr preOpAttr = dir.getWccAttr();
            dir.removeDirectoryFromDirectory((DirectoryObject) f);
            return new RMDIR3Response(Nfs3Status.NFS3_OK, new WccData(preOpAttr, dir.getAttr()));
          }
        }
        return new RMDIR3Response(Nfs3Status.NFS3ERR_NOENT);
      } else {
        return new RMDIR3Response(Nfs3Status.NFS3ERR_NOTDIR);
      }
    } else {
      return new RMDIR3Response(Nfs3Status.NFS3ERR_BADHANDLE);
    }
  }
  
  public SETATTR3Response SETATTR(SETATTR3Request request) throws IOException {
    FileHandle handle = request.getHandle();
    SetAttr3 attr = request.getAttr();
    @SuppressWarnings("unused")
    boolean check = request.isCheck();
    
    if(contents.containsKey(handle.getFileId())) {
      FsObject obj = contents.get(handle.getFileId());
      WccAttr preOpAttr = obj.getWccAttr();
      obj.setAttr(attr);
      return new SETATTR3Response(Nfs3Status.NFS3_OK, new WccData(preOpAttr, obj.getAttr()));
    } else {
      return new SETATTR3Response(Nfs3Status.NFS3ERR_BADHANDLE);
    }
  }
  
  public WRITE3Response WRITE(WRITE3Request request) throws IOException {
    FileHandle handle = request.getHandle();
    long offset = request.getOffset();
    int count = request.getCount();
    ByteBuffer data = request.getData();
    
    if(contents.containsKey(handle.getFileId())) {
      FsObject fsObject = contents.get(handle.getFileId());
      if(fsObject instanceof FileObject) {
        FileObject fileObject = (FileObject) fsObject;
        WccAttr preOpAttr = fileObject.getWccAttr();
        try {
          fileObject.write(offset, count, data);
        } catch(IOException exception) {
          return new WRITE3Response(Nfs3Status.NFS3ERR_FBIG);
        }
        WccData fileWcc = new WccData(preOpAttr, fileObject.getAttr());
        return new WRITE3Response(Nfs3Status.NFS3_OK, fileWcc, count, WriteStableHow.FILE_SYNC, 0L);
      } 
      else {
       return new WRITE3Response(Nfs3Status.NFS3ERR_BADHANDLE); 
      }
    } else {
      return new WRITE3Response(Nfs3Status.NFS3ERR_BADHANDLE);
    }
    
  }

}
