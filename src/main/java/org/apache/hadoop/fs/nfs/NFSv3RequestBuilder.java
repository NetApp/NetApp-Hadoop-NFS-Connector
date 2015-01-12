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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3SetAttr;
import org.apache.hadoop.oncrpc.XDR;

public class NFSv3RequestBuilder {

  public final static Log LOG = LogFactory.getLog(NFSv3RequestBuilder.class);
  
  public static XDR buildCOMMIT3Request(XDR xdr, FileHandle handle, long offset, int count) {
    LOG.debug("Building COMMIT3 request with handle=" + handle + " offset=" + offset + " count=" + count);
    handle.serialize(xdr);
    xdr.writeLongAsHyper(offset);
    xdr.writeInt(count);
    return xdr;
  }
  
  public static XDR buildCREATE3Request(XDR xdr, FileHandle handle, String name, int mode, Nfs3SetAttr objAttr, long verf) {
    LOG.debug("Building CREATE3 request with handle=" + handle + " name=" + name + " mode=" + mode + " objAttr=" + objAttr + " verf=" + verf);
    handle.serialize(xdr);
    xdr.writeInt(name.length());
    xdr.writeFixedOpaque(name.getBytes(), name.length());
    xdr.writeInt(mode);
    objAttr.serialize(xdr);
    return xdr;
  }
  
  public static XDR buildFSINFO3Request(XDR xdr, FileHandle handle) {
    LOG.debug("Building FSINFO3 request with handle=" + handle);
    handle.serialize(xdr);
    return xdr;
  }
  
  public static XDR buildGETATTR3Request(XDR xdr, FileHandle handle) {
    LOG.debug("Building GETATTR3 request with handle=" + handle);
    handle.serialize(xdr);
    return xdr;
  }
  
  public static XDR buildLOOKUP3Request(XDR xdr, FileHandle handle, String name) {
    LOG.debug("Building LOOKUP3 request with handle=" + handle + " name=" + name);
    handle.serialize(xdr);
    xdr.writeInt(name.getBytes().length);
    xdr.writeFixedOpaque(name.getBytes());
    return xdr;
  }
  
  public static XDR buildMKDIR3Request(XDR xdr, FileHandle handle, String name, Nfs3SetAttr objAttr) {
    LOG.debug("Building MKDIR3 request with handle=" + handle + " name=" + name + " objAttr=" + objAttr);
    handle.serialize(xdr);
    xdr.writeInt(name.getBytes().length);
    xdr.writeFixedOpaque(name.getBytes());
    objAttr.serialize(xdr);
    return xdr;
  }
  
  public static XDR buildREAD3Request(XDR xdr, FileHandle handle, long offset, int count) {
    LOG.debug("Building READ3 request with handle=" + handle + " offset=" + offset + " count=" + count);
    handle.serialize(xdr);
    xdr.writeLongAsHyper(offset);
    xdr.writeInt(count);
    return xdr;
  }
  
  public static XDR buildREADDIR3Request(XDR xdr, FileHandle handle, long cookie, long cookieVerf, int count) {
    LOG.debug("Building READDIR3 request with handle=" + handle + " cookie=" + cookie + " cookieVerf=" + cookieVerf + " count=" + count);
    handle.serialize(xdr);
    xdr.writeLongAsHyper(cookie);
    xdr.writeLongAsHyper(cookieVerf);
    xdr.writeInt(count);
    return xdr;
  }
  
  public static XDR buildREMOVE3Request(XDR xdr, FileHandle handle, String name) {
    LOG.debug("Building REMOVE3 request with handle=" + handle + " name=" + name);
    handle.serialize(xdr);
    xdr.writeInt(name.getBytes().length);
    xdr.writeFixedOpaque(name.getBytes());
    return xdr;
  }
  
  public static XDR buildRENAME3Request(XDR xdr, FileHandle fromDir, String fromName, FileHandle toDir, String toName) {
    LOG.debug("Building RENAME3 request with fromDir=" + fromDir + " fromName=" + fromName + " toDir=" + toDir + " toName=" + toName);
    fromDir.serialize(xdr);
    xdr.writeInt(fromName.getBytes().length);
    xdr.writeFixedOpaque(fromName.getBytes());
    toDir.serialize(xdr);
    xdr.writeInt(toName.getBytes().length);
    xdr.writeFixedOpaque(toName.getBytes());
    return xdr;
  }
  
  public static XDR buildRMDIR3Request(XDR xdr, FileHandle handle, String name) {
    LOG.debug("Building RMDIR3 request with handle=" + handle + " name=" + name);
    handle.serialize(xdr);
    xdr.writeInt(name.getBytes().length);
    xdr.writeFixedOpaque(name.getBytes());
    return xdr;
  }
  
  public static XDR buildSETATTR3Request(XDR xdr, FileHandle handle, Nfs3SetAttr attr, boolean check, NfsTime ctime) {
    LOG.debug("Building SETATTR3 request with handle=" + handle + " attr=" + attr + " check=" + check + " ctime=" + ctime);
    handle.serialize(xdr);
    attr.serialize(xdr);
    xdr.writeBoolean(check);
    if(check) {
      ctime.serialize(xdr);
    }
    return xdr;
  }
  
  public static XDR buildWRITE3Request(XDR xdr, FileHandle handle, long offset, int count, WriteStableHow stableHow, byte[] data) {
    LOG.debug("Building WRITE3 request with handle=" + handle + " offset=" + offset + " count=" + count + " stablehow=" + stableHow + " data=" + "not-shown");
    handle.serialize(xdr);
    xdr.writeLongAsHyper(offset);
    xdr.writeInt(count);
    xdr.writeInt(stableHow.getValue());
    xdr.writeInt(count);
    xdr.writeFixedOpaque(data, count);
    return xdr;
  }
  
}
