/**
 * Copyright 2014 NetApp Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.fs.nfs;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.nfs.mount.MountClient;
import org.apache.hadoop.fs.nfs.mount.MountMNTResponse;
import org.apache.hadoop.fs.nfs.portmap.PortmapClient;
import org.apache.hadoop.fs.nfs.rpc.RpcClient;
import org.apache.hadoop.fs.nfs.rpc.RpcException;
import org.apache.hadoop.fs.nfs.topology.Endpoint;
import org.apache.hadoop.fs.nfs.topology.Namespace;
import org.apache.hadoop.fs.nfs.topology.NamespaceOptions;
import org.apache.hadoop.mount.MountResponse;
import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3DirList;
import org.apache.hadoop.nfs.nfs3.Nfs3FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.NFSPROC3;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Info;
import org.apache.hadoop.nfs.nfs3.Nfs3SetAttr;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.response.COMMIT3Response;
import org.apache.hadoop.nfs.nfs3.response.CREATE3Response;
import org.apache.hadoop.nfs.nfs3.response.FSINFO3Response;
import org.apache.hadoop.nfs.nfs3.response.GETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.LOOKUP3Response;
import org.apache.hadoop.nfs.nfs3.response.MKDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.READ3Response;
import org.apache.hadoop.nfs.nfs3.response.READDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.REMOVE3Response;
import org.apache.hadoop.nfs.nfs3.response.RENAME3Response;
import org.apache.hadoop.nfs.nfs3.response.RMDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.SETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.RpcAcceptedReply.AcceptState;
import org.apache.hadoop.oncrpc.RpcMessage;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Credentials;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.oncrpc.security.CredentialsSys;
import org.apache.hadoop.portmap.PortmapMapping;

@InterfaceAudience.Private
public final class NFSv3FileSystemStore extends RpcClient {

    NFSv3FileSystem fs;
    Namespace space;
    Endpoint ep;
    FileHandle rootHandle;
    Nfs3Info fsInfo;
    Long fsId;
    
    public static final Log LOG = LogFactory.getLog(NFSv3FileSystemStore.class);

    public NFSv3FileSystemStore(NFSv3FileSystem fs, Namespace space) throws IOException {
        this(fs, space, space.getDefaultEndpoint());
    }

    public NFSv3FileSystemStore(NFSv3FileSystem fs, Namespace space, Endpoint ep) throws IOException {

        super(ep.getUri().getHost(), ep.getUri().getPort());

        NamespaceOptions options = space.getConfiguration();
        URI uri = ep.getUri();
        String mountDirectory = null;
        this.fs = fs;
        this.space = space;
        this.ep = ep;

        // Find MOUNT port
        int mountPort = options.getNfsMountPort();
        if (options.getNfsMountPort() == NamespaceOptions.INVALID_PORT) {
            PortmapClient portmap = new PortmapClient(uri.getHost(), options.getNfsRpcbindPort());
            mountPort = portmap.getport(MountClient.MOUNTD_PROGRAM, MountClient.MOUNTD_VERSION, PortmapMapping.TRANSPORT_TCP);
            portmap.shutdown();
        }
        
        MountClient mount = new MountClient(space, uri.getHost(), mountPort);
        if (ep.getExportPath() != null && ep.getExportPath().length() > 0) {
            mountDirectory = ep.getExportPath();
        } else if (options.getNfsExportPath() != null && options.getNfsExportPath().length() > 0) {
            mountDirectory = options.getNfsExportPath();
        } else {
            mountDirectory = NamespaceOptions.getDefaultOptions().getNfsExportPath();
        }

        MountMNTResponse mntResponse = mount.mnt(mountDirectory);
        if (mntResponse == null || mntResponse.getStatus() != MountResponse.MNT_OK) {
            LOG.error("Could not get root file handle for endpoint ep=" + ep);
            throw new IOException("Could not get root file handle");
        }
        rootHandle = mntResponse.getFilehandle();
        
        Nfs3FileAttributes attr = this.getFileAttributes(getRootFileHandle(), fs.getCredentials());
        if(attr == null) {
            throw new IOException("Could not get filesystem id");
        }
        fsId = attr.getFsid();
        System.out.println("Store with ep " + ep + " has fsId " + fsId);
        
        mount.shutdown(); 
    }

    public void initialize() throws IOException {
        // FSINFO and set limits on read/write size
        Nfs3Info info = getFilesystemInfo(getRootFileHandle(), fs.getCredentials());
        if(info == null) {
            throw new IOException("Could not get filesystem info for uri=" + ep.getUri());
        }
        fsInfo = info;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 61 * hash + Objects.hashCode(this.fs);
        hash = 61 * hash + Objects.hashCode(this.space);
        hash = 61 * hash + Objects.hashCode(this.fsId);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final NFSv3FileSystemStore other = (NFSv3FileSystemStore) obj;
        if (!Objects.equals(this.fs, other.fs)) {
            return false;
        }
        if (!Objects.equals(this.space, other.space)) {
            return false;
        }
        if (!Objects.equals(this.fsId, other.fsId)) {
            return false;
        }
        return true;
    }


    
    public Endpoint getEndpoint() {
        return ep;
    }
    
    public long getFilesystemId() {
        return fsId;
    }
    
    public FileHandle getRootFileHandle() {
        return rootHandle;
    }

    public int getReadSizeBits() {
        int rtMax = fsInfo.getRtmax();
        int readBlockSizeBits = space.getConfiguration().getNfsReadSizeBits();
        if((rtMax > 0) && ((1 << readBlockSizeBits) > rtMax)) {
            readBlockSizeBits = 31 - Integer.numberOfLeadingZeros(rtMax);
        }
        return readBlockSizeBits;
    }
    
    public int getWriteSizeBits() {
        int wtMax = fsInfo.getWtmax();
        int writeBlockSizeBits = space.getConfiguration().getNfsWriteSizeBits();
        if((wtMax > 0) && ((1 << writeBlockSizeBits) > wtMax)) {
            writeBlockSizeBits = 31 - Integer.numberOfLeadingZeros(wtMax);
        }
        return writeBlockSizeBits;
    }
    
    public int getDirListSize() {
        return fsInfo.getDtpref();
    }
    
    public void nullOp() throws IOException {

        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Issue the call
        acceptState = service(NFSPROC3.NULL, in, out, new CredentialsNone());
        if (acceptState != AcceptState.SUCCESS) {
            LOG.error("NFS NULL resulted in accept state=" + acceptState);
            throw new IOException("NFS NULL resulted in accept state=" + acceptState);
        }

    }

    public COMMIT3Response commit(FileHandle file, long offset, int count, Credentials credentials)
            throws IOException {

        COMMIT3Response commit3Response;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildCOMMIT3Request(in, file, offset, count);

        // Issue the call
        acceptState = service(NFSPROC3.COMMIT, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            commit3Response = NFSv3ResponseBuilder.buildCOMMIT3Response(out.asReadOnlyWrap());
            return commit3Response;
        } else {
            LOG.error("NFS COMMIT3 resulted in accept state=" + acceptState);
            throw new IOException("NFS COMMIT3 resulted in accept state=" + acceptState);
        }

    }

    public CREATE3Response create(FileHandle handle, String name, int mode, Nfs3SetAttr objAttr,
            long verf, Credentials credentials) throws IOException {

        CREATE3Response create3Response;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildCREATE3Request(in, handle, name, mode, objAttr, verf);

        // Issue the call
        acceptState = service(NFSPROC3.CREATE, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            create3Response = NFSv3ResponseBuilder.buildCREATE3Response(out.asReadOnlyWrap());
            return create3Response;
        } else {
            LOG.error("NFS CREATE3 resulted in accept state=" + acceptState);
            throw new IOException("NFS CREATE3 resulted in accept state=" + acceptState);
        }
    }

    public FSINFO3Response fsinfo(FileHandle handle, Credentials credentials) throws IOException {

        FSINFO3Response fsinfo3Response;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildFSINFO3Request(in, handle);

        // Issue the call
        acceptState = service(NFSPROC3.FSINFO, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            fsinfo3Response = NFSv3ResponseBuilder.buildFSINFO3Response(out.asReadOnlyWrap());
            return fsinfo3Response;
        } else {
            LOG.error("NFS FSINFO3 resulted in accept state=" + acceptState);
            throw new IOException("NFS FSINFO3 resulted in accept state=" + acceptState);
        }
    }

    public GETATTR3Response getattr(FileHandle handle, Credentials credentials) throws IOException {

        GETATTR3Response getattr3Response;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildGETATTR3Request(in, handle);

        // Issue the call
        acceptState = service(NFSPROC3.GETATTR, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            getattr3Response = NFSv3ResponseBuilder.buildGETATTR3Response(out.asReadOnlyWrap());
            return getattr3Response;
        } else {
            LOG.error("NFS GETATTR3 resulted in accept state=" + acceptState);
            throw new IOException("NFS GETATTR3 resulted in accept state=" + acceptState);
        }
    }

    public LOOKUP3Response lookup(FileHandle dir, String name, Credentials credentials)
            throws IOException {

        LOOKUP3Response lookup3Response;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildLOOKUP3Request(in, dir, name);

        // Issue the call
        acceptState = service(NFSPROC3.LOOKUP, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            lookup3Response = NFSv3ResponseBuilder.buildLOOKUP3Response(out.asReadOnlyWrap());
            return lookup3Response;
        } else {
            LOG.error("NFS LOOKUP3 resulted in accept state=" + acceptState);
            throw new IOException("NFS LOOKUP3 resulted in accept state=" + acceptState);
        }
    }

    public MKDIR3Response mkdir(FileHandle handle, String name, Nfs3SetAttr objAttr,
            Credentials credentials) throws IOException {

        MKDIR3Response mkdir3Response;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildMKDIR3Request(in, handle, name, objAttr);

        // Issue the call
        acceptState = service(NFSPROC3.MKDIR, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            mkdir3Response = NFSv3ResponseBuilder.buildMKDIR3Response(out.asReadOnlyWrap());
            return mkdir3Response;
        } else {
            LOG.error("NFS MKDIR3 resulted in accept state=" + acceptState);
            throw new IOException("NFS MKDIR3 resulted in accept state=" + acceptState);
        }
    }

    public READ3Response read(FileHandle handle, long offset, int count, Credentials credentials)
            throws IOException {

        READ3Response read3Response;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildREAD3Request(in, handle, offset, count);

        // Issue the call
        acceptState = service(NFSPROC3.READ, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            read3Response = NFSv3ResponseBuilder.buildREAD3Response(out.asReadOnlyWrap());
            return read3Response;
        } else {
            LOG.error("NFS READ3 resulted in accept state=" + acceptState);
            throw new IOException("NFS READ3 resulted in accept state=" + acceptState);
        }
    }

    public READDIR3Response readdir(FileHandle dir, long cookie, long cookieVerf, int count,
            Credentials credentials) throws IOException {

        READDIR3Response readdir3Response;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildREADDIR3Request(in, dir, cookie, cookieVerf, count);

        // Issue the call
        acceptState = service(NFSPROC3.READDIR, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            readdir3Response = NFSv3ResponseBuilder.buildREADDIR3Response(out.asReadOnlyWrap());
            return readdir3Response;
        } else {
            LOG.error("NFS READDIR3 resulted in accept state=" + acceptState);
            throw new IOException("NFS READDIR3 resulted in accept state=" + acceptState);
        }
    }

    public REMOVE3Response remove(FileHandle dir, String name, Credentials credentials)
            throws IOException {

        REMOVE3Response remove3Response = null;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildREMOVE3Request(in, dir, name);

        // Issue the call
        acceptState = service(NFSPROC3.REMOVE, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            remove3Response = NFSv3ResponseBuilder.buildREMOVE3Response(out.asReadOnlyWrap());
            return remove3Response;
        } else {
            LOG.error("NFS REMOVE3 resulted in accept state=" + acceptState);
            throw new IOException("NFS REMOVE3 resulted in accept state=" + acceptState);
        }
    }

    public RENAME3Response rename(FileHandle fromDir, String fromName, FileHandle toDir,
            String toName, Credentials credentials) throws IOException {

        RENAME3Response rename3Response = null;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildRENAME3Request(in, fromDir, fromName, toDir, toName);

        // Issue the call
        long start = System.currentTimeMillis();
        acceptState = service(NFSPROC3.RENAME, in, out, credentials);
        LOG.info("RPC RENAME took " + (System.currentTimeMillis() - start) + " ms");
        if (acceptState == AcceptState.SUCCESS) {
            rename3Response = NFSv3ResponseBuilder.buildRENAME3Response(out.asReadOnlyWrap());
            return rename3Response;
        } else {
            LOG.error("NFS RENAME3 resulted in accept state=" + acceptState);
            throw new IOException("NFS RENAME3 resulted in accept state=" + acceptState);
        }
    }

    public RMDIR3Response rmdir(FileHandle dir, String name, Credentials credentials)
            throws IOException {

        RMDIR3Response rmdir3Response = null;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildRMDIR3Request(in, dir, name);

        // Issue the call
        acceptState = service(NFSPROC3.RMDIR, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            rmdir3Response = NFSv3ResponseBuilder.buildRMDIR3Response(out.asReadOnlyWrap());
            return rmdir3Response;
        } else {
            LOG.error("NFS RMDIR3 resulted in accept state=" + acceptState);
            throw new IOException("NFS RMDIR3 resulted in accept state=" + acceptState);
        }
    }

    public SETATTR3Response setattr(FileHandle handle, Nfs3SetAttr attr, Boolean check,
            NfsTime ctime, Credentials credentials) throws IOException {

        SETATTR3Response setattr3Response = null;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildSETATTR3Request(in, handle, attr, check, ctime);

        // Issue the call
        acceptState = service(NFSPROC3.SETATTR, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            setattr3Response = NFSv3ResponseBuilder.buildSETATTR3Response(out.asReadOnlyWrap());
            return setattr3Response;
        } else {
            LOG.error("NFS SETATTR3 resulted in accept state=" + acceptState);
            throw new IOException("NFS SETATTR3 resulted in accept state=" + acceptState);
        }
    }

    public WRITE3Response write(FileHandle file, long offset, int count, WriteStableHow stableHow,
            byte[] data, Credentials credentials) throws IOException {

        WRITE3Response write3Response = null;
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildWRITE3Request(in, file, offset, count, stableHow, data);

        // Issue the call
        acceptState = service(NFSPROC3.WRITE, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            write3Response = NFSv3ResponseBuilder.buildWRITE3Response(out.asReadOnlyWrap());
            return write3Response;
        } else {
            LOG.error("NFS WRITE3 resulted in accept state=" + acceptState);
            throw new IOException("NFS WRITE3 resulted in accept state=" + acceptState);
        }
    }

    public Nfs3FileAttributes getFileAttributes(FileHandle handle, Credentials credentials)
            throws IOException {

        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildGETATTR3Request(in, handle);

        // Issue the call
        acceptState = service(NFSPROC3.GETATTR, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            XDR buffer = out.asReadOnlyWrap();
            int status = buffer.readInt();
            if (status == Nfs3Status.NFS3_OK) {
                Nfs3FileAttributes attrs = Nfs3FileAttributes.deserialize(buffer);
                return attrs;
            } else {
                LOG.error("NFS GetFileAttributes resulted in status=" + status);
                throw new IOException("NFS GetFileAttributes resulted in status=" + status);
            }
        } else {
            LOG.error("NFS GetFileAttributes resulted in accept state=" + acceptState);
            throw new IOException("NFS GetFileAttributes resulted in accept state=" + acceptState);
        }
    }

    public FileHandle getFileHandle(FileHandle directory, String filename, Credentials credentials)
            throws IOException {

        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildLOOKUP3Request(in, directory, filename);

        // Issue the call
        acceptState = service(NFSPROC3.LOOKUP, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            XDR xdr = out.asReadOnlyWrap();
            int status = xdr.readInt();
            if (status == Nfs3Status.NFS3_OK) {
                FileHandle fileHandle = new Nfs3FileHandle();
                fileHandle.deserialize(xdr);
                return fileHandle;
            } else {
                return null;
                /* throw new IOException("NFS could not get file handle"); */
            }
        } else {
            LOG.error("NFS LOOKUP3 resulted in accept state=" + acceptState);
            throw new IOException("NFS LOOKUP3 resulted in accept state=" + acceptState);
        }
    }

    public Nfs3Info getFilesystemInfo(FileHandle handle, Credentials credentials) throws IOException {
        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildFSINFO3Request(in, handle);

        // Issue the call
        acceptState = service(NFSPROC3.FSINFO, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            return NFSv3ResponseBuilder.buildFilesystemInfo(out.asReadOnlyWrap());
        } else {
            LOG.error("NFS FSINFO3 resulted in accept state=" + acceptState);
            throw new IOException("NFS FSINFO3 resulted in accept state=" + acceptState);
        }
    }

    public Nfs3DirList getDirectoryList(FileHandle dir, long cookie, long cookieVerf, int count,
            Credentials credentials) throws IOException {

        XDR in = new XDR();
        XDR out = new XDR();
        AcceptState acceptState;

        // Construct the NFS request
        NFSv3RequestBuilder.buildREADDIR3Request(in, dir, cookie, cookieVerf, count);

        // Issue the call
        acceptState = service(NFSPROC3.READDIR, in, out, credentials);
        if (acceptState == AcceptState.SUCCESS) {
            return NFSv3ResponseBuilder.buildDirectoryList(out.asReadOnlyWrap());
        } else {
            LOG.error("NFS READDIR3 resulted in accept state=" + acceptState);
            throw new IOException("NFS READDIR3 resulted in accept state=" + acceptState);
        }
    }

    private AcceptState service(NFSPROC3 procedure, XDR in, XDR out, Credentials credentials)
            throws IOException {

        // Make the NFS request
        try {
            RpcMessage reply;
            reply
                    = service(Nfs3Constant.PROGRAM, Nfs3Constant.VERSION, procedure.getValue(), in, out,
                            credentials);
            RpcAcceptedReply accepted = (RpcAcceptedReply) reply;
            return accepted.getAcceptState();
        } catch (RpcException exception) {
            LOG.error("Got a RPC exception");
            exception.printStackTrace();
            throw new IOException("Got a RPC exception", exception);
        }
    }

}
