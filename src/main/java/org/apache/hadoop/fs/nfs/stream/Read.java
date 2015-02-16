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

package org.apache.hadoop.fs.nfs.stream;

import java.util.concurrent.Callable;

import org.apache.hadoop.fs.nfs.NFSv3FileSystemStore;
import org.apache.hadoop.fs.nfs.StreamStatistics;
import org.apache.hadoop.fs.nfs.rpc.RpcException;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.response.READ3Response;
import org.apache.hadoop.oncrpc.security.Credentials;

public class Read implements Callable<Read> {

  final NFSv3FileSystemStore store;
  final FileHandle handle;
  final StreamStatistics statistics;
  final Long blockId;
  final StreamBlock block;
  final Credentials credentials;

  public Read(NFSv3FileSystemStore store, FileHandle handle, Credentials credentials, StreamStatistics statistics,
      Long blockId, StreamBlock block) {
    this.store = store;
    this.handle = handle;
    this.credentials = credentials;
    this.statistics = statistics;
    this.blockId = blockId;
    this.block = block;
  }

  @Override
  public Read call() throws Exception {
    long startTime = System.currentTimeMillis(), stopTime;
    long readOffset = (blockId << block.getBlockSizeBits());

    READ3Response read3Response =
        store.read(handle, readOffset, block.getBlockSize(), credentials);
    int status = read3Response.getStatus();
    if (status != Nfs3Status.NFS3_OK) {
      throw new RpcException("NFS_READ error: status=" + status);
    }

    int readBytes = read3Response.getCount();
    if (readBytes == 0) {
      if (!read3Response.isEof()) {
        throw new RpcException("read 0 bytes while not reaching EOF");
      }
    }

    if (readBytes > 0) {
      block.writeToBlock(read3Response.getData().array(), 0, 0, readBytes);
    }
    stopTime = System.currentTimeMillis();

    // Update statistics
    if (readBytes >= 0) {
      statistics.incrementBytesNFSRead(readBytes);
      statistics.incrementNFSReadOps(1);
      statistics.incrementTimeNFSRead((stopTime - startTime));
    }

    block.setReady(true);
    return this;
  }

}
