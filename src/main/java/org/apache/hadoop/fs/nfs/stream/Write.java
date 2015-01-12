/**
 * Copyright 2014 NetApp Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.nfs.stream;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.nfs.NFSv3FileSystemStore;
import org.apache.hadoop.fs.nfs.StreamStatistics;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;

public class Write implements Callable<Write> {

  final NFSv3FileSystemStore store;
  final FileHandle handle;
  final StreamStatistics statistics;
  final Long blockId;
  final StreamBlock block;
  
  public Write(NFSv3FileSystemStore store, FileHandle handle, StreamStatistics statistics, Long blockId, StreamBlock block) {
    this.store = store;
    this.handle = handle;
    this.statistics = statistics;
    this.blockId = blockId;
    this.block = block;
  }
  
  @Override
  public Write call() throws Exception {
    long startTime = System.currentTimeMillis();
    
    long writeOffset = (blockId << block.getBlockSizeBits()) + block.getDataStartOffset();
    byte buffer[] = new byte[block.getDataLength()];
    System.arraycopy(block.array(), block.getDataStartOffset(), buffer, 0, block.getDataLength());
    
    WRITE3Response response = store.write(handle, writeOffset, block.getDataLength(), WriteStableHow.UNSTABLE, buffer, store.getCredentials());
    int status = response.getStatus();
    if (status != Nfs3Status.NFS3_OK) {
      throw new IOException("NFS write error: status=" + status);
    }
    if(response.getCount() != block.getDataLength()) {
      throw new IOException("NFS write error: status=" + status + " short write copied=" + response.getCount() + " but length=" + block.getDataLength());
    }
    
    // Update statistics
    statistics.incrementNFSWriteOps(1);
    statistics.incrementBytesNFSWritten(block.getDataLength());
    statistics.incrementTimeNFSWritten(System.currentTimeMillis() - startTime);
    
    return this;
  }

}
