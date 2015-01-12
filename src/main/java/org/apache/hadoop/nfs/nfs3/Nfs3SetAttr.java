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

package org.apache.hadoop.nfs.nfs3;

import java.util.EnumSet;

import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3.SetAttrField;
import org.apache.hadoop.oncrpc.XDR;

/***
 * SetAttr copied from SetAttr3 in hadoop-nfs as the members are marked private.
 * 
 * @author gokuls
 * 
 */
public class Nfs3SetAttr {

  public static final int TIME_DONT_CHANGE = 0;
  public static final int TIME_SET_TO_SERVER_TIME = 1;
  public static final int TIME_SET_TO_CLIENT_TIME = 2;

  int mode;
  int uid;
  int gid;
  long size;
  NfsTime atime;
  NfsTime mtime;
  EnumSet<SetAttrField> updateFields;

  public Nfs3SetAttr() {
    mode = 0;
    uid = 0;
    gid = 0;
    size = 0;
    updateFields = EnumSet.noneOf(SetAttrField.class);
  }

  public Nfs3SetAttr(int mode, int uid, int gid, long size, NfsTime atime,
      NfsTime mtime, EnumSet<SetAttrField> updateFields) {
    this.mode = mode;
    this.uid = uid;
    this.gid = gid;
    this.size = size;
    this.atime = atime;
    this.mtime = mtime;
    this.updateFields = updateFields;
  }

  public int getMode() {
    return mode;
  }

  public int getUid() {
    return uid;
  }

  public int getGid() {
    return gid;
  }

  public void setGid(int gid) {
    this.gid = gid;
  }

  public long getSize() {
    return size;
  }

  public NfsTime getAtime() {
    return atime;
  }

  public void setMode(int mode) {
    this.mode = mode;
  }

  public void setUid(int uid) {
    this.uid = uid;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public void setAtime(NfsTime atime) {
    this.atime = atime;
  }

  public void setMtime(NfsTime mtime) {
    this.mtime = mtime;
  }

  public NfsTime getMtime() {
    return mtime;
  }

  public EnumSet<SetAttrField> getUpdateFields() {
    return updateFields;
  }

  public void setUpdateFields(EnumSet<SetAttrField> updateFields) {
    this.updateFields = updateFields;
  }

  public void serialize(XDR xdr) {
    if (!updateFields.contains(SetAttrField.MODE)) {
      xdr.writeBoolean(false);
    } else {
      xdr.writeBoolean(true);
      xdr.writeInt(mode);
    }
    if (!updateFields.contains(SetAttrField.UID)) {
      xdr.writeBoolean(false);
    } else {
      xdr.writeBoolean(true);
      xdr.writeInt(uid);
    }
    if (!updateFields.contains(SetAttrField.GID)) {
      xdr.writeBoolean(false);
    } else {
      xdr.writeBoolean(true);
      xdr.writeInt(gid);
    }
    if (!updateFields.contains(SetAttrField.SIZE)) {
      xdr.writeBoolean(false);
    } else {
      xdr.writeBoolean(true);
      xdr.writeLongAsHyper(size);
    }
    if (!updateFields.contains(SetAttrField.ATIME)) {
      xdr.writeBoolean(false);
    } else {
      xdr.writeBoolean(true);
      atime.serialize(xdr);
    }
    if (!updateFields.contains(SetAttrField.MTIME)) {
      xdr.writeBoolean(false);
    } else {
      xdr.writeBoolean(true);
      mtime.serialize(xdr);
    }
  }

  public void deserialize(XDR xdr) {
    if (xdr.readBoolean()) {
      mode = xdr.readInt();
      updateFields.add(SetAttrField.MODE);
    }
    if (xdr.readBoolean()) {
      uid = xdr.readInt();
      updateFields.add(SetAttrField.UID);
    }
    if (xdr.readBoolean()) {
      gid = xdr.readInt();
      updateFields.add(SetAttrField.GID);

    }
    if (xdr.readBoolean()) {
      size = xdr.readHyper();
      updateFields.add(SetAttrField.SIZE);

    }
    int timeSetHow = xdr.readInt();
    if (timeSetHow == TIME_SET_TO_CLIENT_TIME) {
      atime = NfsTime.deserialize(xdr);
      updateFields.add(SetAttrField.ATIME);
    } else if (timeSetHow == TIME_SET_TO_SERVER_TIME) {
      atime = new NfsTime(System.currentTimeMillis());
      updateFields.add(SetAttrField.ATIME);
    }

    timeSetHow = xdr.readInt();
    if (timeSetHow == TIME_SET_TO_CLIENT_TIME) {
      mtime = NfsTime.deserialize(xdr);
      updateFields.add(SetAttrField.MTIME);
    } else if (timeSetHow == TIME_SET_TO_SERVER_TIME) {
      mtime = new NfsTime(System.currentTimeMillis());
      updateFields.add(SetAttrField.MTIME);
    }
  }
}
