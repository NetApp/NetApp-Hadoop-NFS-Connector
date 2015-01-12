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

package org.apache.hadoop.fs.nfs.mount;

import java.io.IOException;

import org.apache.hadoop.mount.MountResponse;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3FileHandle;
import org.apache.hadoop.oncrpc.XDR;

public class MountMNTResponse {
  int status;
  FileHandle filehandle;
  int []authFlavor;

  public MountMNTResponse(int returnValue, FileHandle filehandle, int[] authFlavor) {
    this.status = returnValue;
    this.filehandle = filehandle;
    this.authFlavor = authFlavor;
  }

  public MountMNTResponse(XDR xdr) throws IOException {
    status = xdr.readInt();
    
    if (status == MountResponse.MNT_OK) {
      filehandle = new Nfs3FileHandle();
      filehandle.deserialize(xdr);
      
      int flavorNum = xdr.readInt();
      if (flavorNum > 0) {
        authFlavor = new int[flavorNum];
        for(int i = 0; i < flavorNum; i++) {
          authFlavor[i] = xdr.readInt();
        }
      }
    } 
    else {
      filehandle = null;
    }
  }

  public int getStatus() {
    return status;
  }

  public FileHandle getFilehandle() {
    return filehandle;
  }

  public int[] getAuthFlavors() {
    return authFlavor;
  }

}
