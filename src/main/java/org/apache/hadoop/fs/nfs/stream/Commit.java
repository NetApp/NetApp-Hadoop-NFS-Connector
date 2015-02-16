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
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.response.COMMIT3Response;
import org.apache.hadoop.oncrpc.security.Credentials;

public class Commit implements Callable<Commit> {

  final NFSv3FileSystemStore store;
  final FileHandle handle;
  final Credentials credentials;
  
  public Commit(NFSBufferedOutputStream stream, NFSv3FileSystemStore store, FileHandle handle, Credentials credentials, Long offset, Integer length) {
    this.store = store;
    this.handle = handle;
    this.credentials = credentials;
  }
  
  @Override
  public Commit call() throws Exception {
    COMMIT3Response response = store.commit(handle, 0L, 0, credentials);
    int status = response.getStatus();
    if (status != Nfs3Status.NFS3_OK) {
      throw new IOException("Commit error: status=" + status);
    }
    return this;
  }

}
