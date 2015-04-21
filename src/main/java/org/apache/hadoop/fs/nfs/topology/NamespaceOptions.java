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
package org.apache.hadoop.fs.nfs.topology;

import java.io.IOException;

import com.google.gson.annotations.Expose;

import org.apache.hadoop.fs.nfs.NFSv3FileSystem;


public class NamespaceOptions {
    
    @Expose
    String nfsExportPath;
    @Expose
    int    nfsReadSizeBits;
    @Expose
    int    nfsWriteSizeBits;
    @Expose
    int    nfsSplitSizeBits;
    @Expose
    String nfsAuthScheme;
    @Expose
    String nfsUsername;
    @Expose
    String nfsGroupname;
    @Expose
    String nfsUserConfigFile;
    @Expose
    String nfsGroupConfigFile;
    @Expose
    String nfsUserGroupAuthImpl;
    @Expose
    int    nfsUid;
    @Expose
    int    nfsGid;
    @Expose
    int    nfsPort;
    @Expose
    int    nfsMountPort;
    @Expose
    int    nfsRpcbindPort;
    @Expose
    String nfsAuthFile;
    
    UserGroupAuthentication auth;
    
    public static final int INVALID_PORT = -1;
    
    static final NamespaceOptions DEFAULT;
    
    static {
        DEFAULT = new NamespaceOptions();
    }
    
    public NamespaceOptions() {
        setNfsReadSizeBits(NFSv3FileSystem.DEFAULT_READ_BLOCK_SIZE_BITS);
        setNfsWriteSizeBits(NFSv3FileSystem.DEFAULT_WRITE_BLOCK_SIZE_BITS);
        setNfsSplitSizeBits(NFSv3FileSystem.DEFAULT_NFS_SPLIT_SIZE_BITS);
        setNfsUsername(NFSv3FileSystem.NFS_USER_NAME_DEFAULT);
        setNfsGroupname(NFSv3FileSystem.NFS_GROUP_NAME_DEFAULT);
        setNfsUid(NFSv3FileSystem.NFS_UID_DEFAULT);
        setNfsGid(NFSv3FileSystem.NFS_GID_DEFAULT);
        setNfsExportPath("/");
        setNfsPort(NFSv3FileSystem.DEFAULT_NFS_PORT);
        setNfsMountPort(INVALID_PORT);
        setNfsRpcbindPort(111);
        setNfsAuthScheme("AUTH_NONE");
    }
    
    public static NamespaceOptions getDefaultOptions() {
        return DEFAULT;
    }

    public String getNfsExportPath() {
        return nfsExportPath;
    }

    public int getNfsReadSizeBits() {
        return nfsReadSizeBits;
    }

    public int getNfsWriteSizeBits() {
        return nfsWriteSizeBits;
    }

    public int getNfsSplitSizeBits() {
        return nfsSplitSizeBits;
    }

    public String getNfsAuthScheme() {
        return nfsAuthScheme;
    }

    public String getNfsUsername() {
        return nfsUsername;
    }

    public String getNfsGroupname() {
        return nfsGroupname;
    }
    
    public String getNFSUserConfigFile() {
    	return nfsUserConfigFile;
    }
    
    public String getNFSGroupConfigFile() {
    	return nfsGroupConfigFile;
    }

    public int getNfsUid() {
        return nfsUid;
    }

    public int getNfsGid() {
        return nfsGid;
    }

    public int getNfsPort() {
        return nfsPort;
    }

    public int getNfsMountPort() {
        return nfsMountPort;
    }

    public int getNfsRpcbindPort() {
        return nfsRpcbindPort;
    }

    public String getNfsAuthFile() {
        return nfsAuthFile;
    }
    
    public String getUserNameFromUserId(String userid) {
        return auth.getUserNameFromUserId(userid);
    }

    public String getUserIdFromUserName(String username) {
        return auth.getUserIdFromUserName(username);
    }

    public String getGroupNameFromGroupId(String groupid) {
        return auth.getGroupNameFromGroupId(groupid);
    }

    public String getGroupIdFromGroupName(String groupname) {
        return auth.getGroupIdFromGroupName(groupname);
    }
    
    public void setNfsExportPath(String nfsExportPath) {
        this.nfsExportPath = nfsExportPath;
    }
    
    public void setNfsAuthScheme(String authScheme) {
        this.nfsAuthScheme = authScheme;
    }
    
    public void setNfsReadSizeBits(int nfsReadSizeBits) {
        this.nfsReadSizeBits = nfsReadSizeBits;
    }
    
    public void setNfsWriteSizeBits(int nfsWriteSizeBits) {
        this.nfsWriteSizeBits = nfsWriteSizeBits;
    }
    
    public void setNfsSplitSizeBits(int nfsSplitSizeBits) {
        this.nfsSplitSizeBits = nfsSplitSizeBits;
    }
    
    public void setNfsUsername(String username) {
        this.nfsUsername = username;
    }
    
    public void setNfsGroupname(String groupname) {
        this.nfsGroupname = groupname;
    }
    
    public void setNfsUserConfigFile(String userConfigFile) {
    	this.nfsUserConfigFile = userConfigFile;
    }
    
    public void setNfsGroupConfigFile(String groupConfigFile) {
    	this.nfsGroupConfigFile = groupConfigFile;
    }
    
    public void setNfsUid(int uid) {
        this.nfsUid = uid;
    }
    
    public void setNfsGid(int gid) {
        this.nfsGid = gid;
    }
    
    public void setNfsPort(int port) {
        this.nfsPort = port;
    }
    
    public void setNfsMountPort(int port) {
        this.nfsMountPort = port;
    }
    
    public void setNfsRpcbindPort(int port) {
        this.nfsRpcbindPort = port;
    }
    
    public boolean UserGroupMappingCreated() {
        return (auth != null);
    }
    //TODO
    public void CreatUserGroupMapping() throws Exception {
        if (nfsUserGroupAuthImpl == null) {
            nfsUserGroupAuthImpl = "org.apache.hadoop.fs.nfs.topology.SimpleUserGroupAuthentication";
        }
        
        auth = (UserGroupAuthentication) Class.forName(nfsUserGroupAuthImpl).newInstance();
        auth = new SimpleUserGroupAuthentication();
        auth.initialize(nfsUserConfigFile, nfsGroupConfigFile, null);
    }
}
