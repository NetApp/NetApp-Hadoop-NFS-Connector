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

import org.apache.hadoop.nfs.NfsFileType;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3;


public class FileObject extends FsObject {
  
  byte[] data;
  int size;
  
  protected FileObject(String name) {
    super(FileType.TYPE_FILE, name);
    size = 0;
    data = new byte[MAX_FILE_SIZE];
  }
  
  public int getSize() {
    return size;
  }
  
  @Override
  public void setAttr(SetAttr3 attr) {
    NfsFileType nfsType = (this.type.equals(FileType.TYPE_DIRECTORY)) ? NfsFileType.NFSDIR : NfsFileType.NFSREG;
    int nlink = 0;
    short mode = (short) attr.getMode();
    int uid = attr.getUid();
    int gid = attr.getGid();
    int fsid = MockNfs3Filesystem.MOCK_FSID;
    int fileId = (int) this.getId();
    int mtime = (attr.getMtime() == null) ? ( (int) System.currentTimeMillis() / 1000) : attr.getMtime().getSeconds();
    int atime = (attr.getAtime() == null) ? ( (int) System.currentTimeMillis() / 1000) : attr.getAtime().getSeconds();
    super.attr = new Nfs3FileAttributes(nfsType, nlink, mode, uid, gid, this.size, fsid, fileId, mtime, atime);
  }
  
  public void write(long offset, int length, ByteBuffer writeData) throws IOException {

    int newSize = (int) (offset + length);
    if( (offset + length) > FsObject.MAX_FILE_SIZE) {
      throw new IOException("File is too big!");
    }

    // Byte buffer is allocated to maximum file size
    // so just copy the contents into the right place
    System.arraycopy(writeData.array(), 0, data, (int) offset, length);
    size = (size > newSize) ? size : newSize;
    
    // Update attributes
    this.attr.setSize(size);
    
    return;
  }
  
  public int read(long offset, int length, byte[] readData) throws IOException {
    
    if( (offset + length) > size) {
      throw new IOException("Can't read beyond file length");
    }
    assert(length > 0 && length < FsObject.MAX_FILE_SIZE);
   
    if( ((int) offset + length) > size) {
      length = size - (int) offset;
    }
    System.arraycopy(data, (int) offset, readData, 0, length);
    
    return length;
    
  }
  
}
