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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Nfs3DirList {
  final List<Nfs3DirEntry> entries;
  final boolean eof;
  final long cookieVerf;
  
  public static class Nfs3DirEntry {
    private final long fileId;
    private final String name;
    private final long cookie;
    
    public Nfs3DirEntry(long fileId, String name, long cookie) {
      this.fileId = fileId;
      this.name = name;
      this.cookie = cookie;
    }

    public long getFileId() {
      return fileId;
    }

    public String getName() {
      return name;
    }

    public long getCookie() {
      return cookie;
    }
  }
  
  public Nfs3DirList(Nfs3DirEntry[] entries, long cookieVerf, boolean eof) {
    this.entries = Collections.unmodifiableList(Arrays.asList(entries));
    this.eof = eof;
    this.cookieVerf = cookieVerf;
  }
  
  public List<Nfs3DirEntry> getEntries() {
    return this.entries;
  }
  
  public boolean isEof() {
    return eof;
  }
  
  public long getCookieVerf() {
    return cookieVerf;
  }
  
}
