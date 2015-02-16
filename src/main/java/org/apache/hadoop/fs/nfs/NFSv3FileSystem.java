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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.nfs.NfsFileType;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3DirList;
import org.apache.hadoop.nfs.nfs3.Nfs3DirList.Nfs3DirEntry;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3SetAttr;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3.SetAttrField;
import org.apache.hadoop.nfs.nfs3.response.CREATE3Response;
import org.apache.hadoop.nfs.nfs3.response.GETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.MKDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.REMOVE3Response;
import org.apache.hadoop.nfs.nfs3.response.RENAME3Response;
import org.apache.hadoop.nfs.nfs3.response.RMDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.SETATTR3Response;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.nfs.stream.NFSBufferedInputStream;
import org.apache.hadoop.fs.nfs.stream.NFSBufferedOutputStream;
import org.apache.hadoop.fs.nfs.topology.Endpoint;
import org.apache.hadoop.fs.nfs.topology.Mapping;
import org.apache.hadoop.fs.nfs.topology.Namespace;
import org.apache.hadoop.fs.nfs.topology.NamespaceOptions;
import org.apache.hadoop.fs.nfs.topology.SimpleTopologyRouter;
import org.apache.hadoop.fs.nfs.topology.TopologyRouter;
import org.apache.hadoop.oncrpc.security.Credentials;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.oncrpc.security.CredentialsSys;

/**
 * <p>
 * A {@link FileSystem} backed by NFS.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NFSv3FileSystem extends FileSystem {


    private Configuration configuration;
    private Path workingDir;
    private URI uri;
    private Namespace space;

    private HandleCache handleCache;

    private TopologyRouter router;

    // Actual UID, GID, user name and group name to use when creating a new file/directory. Values
    // from the configuration file will overwrite default values defined above.
    private int NFS_UID;
    private int NFS_GID;
    private String NFS_USER_NAME;
    private String NFS_GROUP_NAME;

    public static final String NFS_URI_SCHEME = "nfs";
    public static final int FILE_HANDLE_CACHE_SIZE = 1048576;
    public static final int DEFAULT_NFS_PORT = 2049;
    public static final int DEFAULT_READ_BLOCK_SIZE_BITS = 20;
    public static final int DEFAULT_WRITE_BLOCK_SIZE_BITS = 20;

    // Default file split size
    public static final int DEFAULT_NFS_SPLIT_SIZE_BITS = 28;

    // Default UID and GID when creating a new file/directory
    public static final int NFS_UID_DEFAULT = 0;
    public static final int NFS_GID_DEFAULT = 0;
    public static final String NFS_USER_NAME_DEFAULT = "root";
    public static final String NFS_GROUP_NAME_DEFAULT = "root";

    public static final String UNIX_DOT_DIR = ".";
    public static final String UNIX_DOT_DOT_DIR = "..";

    public final static Log LOG = LogFactory.getLog(NFSv3FileSystem.class);

    public NFSv3FileSystem() {
        // set store in initialize()
    }

    public NFSv3FileSystem(URI uri, Configuration conf) throws IOException {
        initialize(uri, conf);
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {

        super.initialize(uri, conf);

        // Save configuration
        this.configuration = conf;
        this.uri = uri;

        // Here, we get either a config option pointing
        // to additional info or a simple URI (in which case we just use defaults)
        if (configuration.get("fs." + NFS_URI_SCHEME + ".configuration") != null) {
            space = Mapping.loadFromFile(configuration.get("fs." + NFS_URI_SCHEME + ".configuration")).getNamespace(uri);
        } // Create a namespace with defaults
        else {
            LOG.info("The URI " + uri + " has no additional config defined, resorting to defaults");
            space = new Mapping().buildNamespace(uri);
        }

        // Get configuration from namespace
        NFS_UID = space.getConfiguration().getNfsUid();
        NFS_GID = space.getConfiguration().getNfsGid();
        NFS_USER_NAME = space.getConfiguration().getNfsUsername();
        NFS_GROUP_NAME = space.getConfiguration().getNfsGroupname();

        // Initialize router
        // TODO: Make the router class configurable
        router = new SimpleTopologyRouter();
        router.initialize(this, space, configuration);

        setConf(conf);
        this.handleCache = new HandleCache(FILE_HANDLE_CACHE_SIZE);
        workingDir = getHomeDirectory();
    }

    public long getSplitSize() {
        return (1L << space.getConfiguration().getNfsSplitSizeBits());
    }

    @Override
    public String getScheme() {
        return NFS_URI_SCHEME;
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public void close() throws IOException {
        List<NFSv3FileSystemStore> stores = router.getAllStores();
        if(stores != null && stores.size() > 0) {
            for(NFSv3FileSystemStore store : stores) {
                store.shutdown();
            }
        }
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
        FileStatus file = getFileStatus(p);
        BlockLocation[] locations = getFileBlockLocations(file, start, len);
        return locations;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
        if (file == null) {
            return null;
        }
        if (start < 0 || len < 0) {
            throw new IllegalArgumentException("Invalid start or len parameter");
        }
        if (file.getLen() <= start) {
            LOG.info("END getFileBlockLocations(): length is <= start so no locations");
            return new BlockLocation[0];
        }

        //TODO: See if we can provide the actual endpoints for topology awareness scheduling
        String hostname = space.getUri().getHost();
        int port = space.getUri().getPort();
        
        String[] name = {hostname + ":" + port};
        String[] host = {hostname};
        BlockLocation locations[]
                = new BlockLocation[]{new BlockLocation(name, host, 0, file.getLen())};
        return locations;
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        workingDir = makeAbsolute(new_dir);
    }

    private Path makeAbsolute(Path path) {
        if (path == null) {
            return null;
        }
        if (path.isAbsolute() && !path.isAbsoluteAndSchemeAuthorityNull()) {
            return path;
        }
        Path newPath = new Path(workingDir, path);
        return newPath;
    }

    @Override
    public Path getHomeDirectory() {
        Path homeDir = makeQualified(new Path("/user/" + System.getProperty("user.name")));
        return homeDir;
    }

    @Override
    public Path makeQualified(Path path) {
        checkPath(path);
        Path p = path.makeQualified(this.getUri(), this.getWorkingDirectory());
        return p;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        f = makeAbsolute(f);
        NFSv3FileSystemStore store = router.getStore(f);

        // Directories cannot be opened for reading
        FileStatus status = getFileStatus(f);
        if (status != null && status.isDirectory()) {
            throw new FileNotFoundException("open(): cannot open a directory " + f + " for reading");
        }

        FileHandle handle = getAndVerifyFileHandle(f);
        if (handle == null) {
            LOG.error("open(): file handle is undefined for file" + f);
            return null;
        }

        return new FSDataInputStream(new BufferedFSInputStream(new NFSBufferedInputStream(store, handle, 
                f, this.getConf(), this.getSplitSize(), getCredentials(), statistics), bufferSize));

    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException {
        f = makeAbsolute(f);
        NFSv3FileSystemStore store = router.getStore(f);

        FileHandle handle = getAndVerifyFileHandle(f);
        if (handle == null) {
            throw new FileNotFoundException("append(): file " + f + " does not exist");
        }

        return new FSDataOutputStream(new NFSBufferedOutputStream(configuration, handle, f, store, getCredentials(), true), statistics);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
            int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        f = makeAbsolute(f);
        NFSv3FileSystemStore store = router.getStore(f);

        FileHandle handle = getAndVerifyFileHandle(f);
        if (handle == null) {
            Path parent = f.getParent();
            
            NFSv3FileSystemStore parentStore = router.getStore(parent);
            if(!parentStore.equals(store)) {
                throw new IOException("Cannot create across two junctions");
            }
            
            FileHandle parentHandle = null;
            if (parent.isRoot() || isRoot(parent)) {
                parentHandle = store.getRootFileHandle();
            } else {
                if (mkdirs(parent)) {
                    parentHandle = getAndVerifyFileHandle(parent);
                } else {
                    throw new IOException("create(): failed to create parent dir " + parent);
                }
            }

            // At this point, we should have a valid parent handle.
            if (parentHandle == null) {
                throw new IOException("create(): parent handle is null for creating " + f);
            }

            handle = create(store, parentHandle, f.getName(), permission);
        } else {
            FileStatus status = getFileStatus(f);
            if (status != null) {
                if (status.isDirectory()) {
                    throw new FileAlreadyExistsException("create(): path " + f + " is already a directory");
                } else {
                    if (overwrite != true) {
                        throw new FileAlreadyExistsException("create(): file already exists " + f);
                    }
                    truncate(store, handle, 0);
                }
            } else {
                throw new IOException("create(): could not get status of file " + f);
            }
        }
        return new FSDataOutputStream(new NFSBufferedOutputStream(configuration, handle, f, store,
                getCredentials(), false), statistics);
    }

    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
            EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
            Progressable progress) throws IOException {
        if (flags.contains(CreateFlag.APPEND)) {
            return append(f, bufferSize, progress);
        } else if (flags.contains(CreateFlag.OVERWRITE)) {
            return this.create(f, permission, true, bufferSize, replication, blockSize, progress);
        } else {
            return this.create(f, permission, false, bufferSize, replication, blockSize, progress);
        }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        src = makeAbsolute(src);
        dst = makeAbsolute(dst);

        NFSv3FileSystemStore srcStore = router.getStore(src);
        NFSv3FileSystemStore dstStore = router.getStore(dst);

        FileStatus srcStatus;
        FileStatus dstStatus;

        // Cannot rename across filesystems
        if (!srcStore.equals(dstStore)) {
            throw new IOException("rename(): cannot rename src=" + src + " dst=" + dst + " because they use two different junctions");
        }

        // Check status of src and dst paths
        try {
            srcStatus = getFileStatus(src);
        } catch (FileNotFoundException exception) {
            srcStatus = null;
        }

        try {
            dstStatus = getFileStatus(dst);
        } catch (FileNotFoundException exception) {
            dstStatus = null;
        }

        // Source path must exist
        if (srcStatus == null) {
            LOG.warn("rename(): source path " + src + " does not exist");
            return false;
        }

        if (src.getParent() == null) {
            LOG.warn("rename(): root directory " + src + " cannot be renamed");
            return false;
        }

        if (dst.getParent() == null) {
            LOG.warn("rename(): cannot rename directory to root " + dst);
            return false;
        }

        // All the paths must be in the same filesystem
        NFSv3FileSystemStore srcParentStore = router.getStore(src.getParent());
        NFSv3FileSystemStore dstParentStore = router.getStore(dst.getParent());
        if(!srcStore.equals(srcParentStore) && !srcStore.equals(dstParentStore)) {
            throw new IOException("Cannot rename across different junctions");
        }
        
        FileHandle srcParentHandle = getAndVerifyFileHandle(src.getParent());
        FileHandle dstParentHandle = getAndVerifyFileHandle(dst.getParent());

        if (srcParentHandle == null) {
            LOG.warn("rename(): parent of source " + src + " does not exist");
            return false;
        }

        if (dstParentHandle == null) {
            LOG.warn("rename(): parent of destination " + dst + " does not exist");
            return false;
        }

        int status = Nfs3Status.NFS3ERR_IO;

        // Destination is a file
        if (dstStatus != null && dstStatus.isFile()) {
            if (srcStatus.isDirectory()) {
                LOG.warn("rename(): trying to rename existing directory " + src + " into a file " + dst);
                return false;
            } else if (srcStatus.isFile()) {
                return src.equals(dst);
            } else {
                throw new IOException("rename(): source " + src + " is neither a file nor a directory");
            }
        } // Destination is a directory, so move source into destination
        else if (dstStatus != null && dstStatus.isDirectory()) {
            FileHandle dstHandle = getAndVerifyFileHandle(dst);
            if (dstHandle == null) {
                throw new IOException("rename(): destination " + dst + " is a directory but its handle cannot be found");
            }
            RENAME3Response rename3Response
                    = srcStore.rename(srcParentHandle, src.getName(), dstHandle, src.getName(),
                            getCredentials());
            status = rename3Response.getStatus();
        } // Destination does not exist
        else if (dstStatus == null) {
            RENAME3Response rename3Response
                    = srcStore.rename(srcParentHandle, src.getName(), dstParentHandle, dst.getName(),
                            getCredentials());
            status = rename3Response.getStatus();
        }

        if (status != Nfs3Status.NFS3_OK) {
            if (status == Nfs3Status.NFS3ERR_INVAL) {
                return false;
            }
            throw new IOException("rename(): rename of src " + src + " to dst " + dst + "  returned status " + status);
        }

        // Remove old handles
        handleCache.removeAll(Path.getPathWithoutSchemeAndAuthority(src).toString());

        return true;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        f = makeAbsolute(f);
        NFSv3FileSystemStore store = router.getStore(f);

        FileHandle handle = getAndVerifyFileHandle(f);
        if (handle == null) {
            LOG.warn("delete(): file " + f + " does not exist");
            return false;
        }

        Nfs3FileAttributes attributes = store.getFileAttributes(handle, getCredentials());
        if (attributes == null) {
            throw new IOException("delete(): could not get file attributes for " + f);
        }

        int fileType = attributes.getType();
        if (fileType == NfsFileType.NFSREG.toValue()) {
            return remove(f);
        } else if (fileType == NfsFileType.NFSDIR.toValue()) {
            Set<Path> subPaths = listSubPaths(f);
            if (recursive == false && (subPaths != null && subPaths.isEmpty() == false)) {
                throw new IOException("delete(): directory " + f + " is not empty so it cannot be deleted");
            }

            FileStatus[] files = listStatus(f);
            for (FileStatus fileStatus : files) {
                if (delete(fileStatus.getPath(), recursive) == false) {
                    LOG.warn("delete(): recursive delete failed for " + fileStatus.getPath());
                    return false;
                }
            }
            return rmdir(f);
        } else {
            throw new IOException("delete(): file " + f + " is neither a file nor directory");
        }
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        f = makeAbsolute(f);

        FileStatus fileStatus = getFileStatus(f);
        if (!fileStatus.isDirectory()) {
            return new FileStatus[]{fileStatus};
        }

        // Path f is a dir, return a list of all files/dir in this dir
        ArrayList<FileStatus> fileStatusList = new ArrayList<>();
        for (Path path : listSubPaths(f)) {
            FileStatus s1 = getFileStatus(path);
            fileStatusList.add(s1);
        }
        FileStatus[] array = fileStatusList.toArray(new FileStatus[0]);
        return array;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        f = makeAbsolute(f);
        NFSv3FileSystemStore store;
        
        // Capture root paths of all endpoints
        Set<String> rootPaths = new HashSet<>();
        for(Endpoint ep : space.getEndpoints()) {
            if(ep.getPath() !=null) {
                rootPaths.add(ep.getPath());
            }
        }

        // Will check up till the root of one of exported paths
        List<String> dirs = new LinkedList<>();
        Path path = Path.getPathWithoutSchemeAndAuthority(f);
        do {
            dirs.add(0, path.getName());
            path = path.getParent();
        } while (!path.isRoot() && !rootPaths.contains(path.toString()));

        store = router.getStore(path);
        FileHandle parentDir = store.getRootFileHandle();
        
        path = null;
        for (String dir : dirs) {
            if (path == null) {
                path = new Path(Path.SEPARATOR + dir);
            } else {
                path = new Path(path.toString() + Path.SEPARATOR + dir);
            }
            
            /*
            NFSv3FileSystemStore s = router.getStore(path);
            if(!s.equals(store)) {
                System.err.println("mkdirs(): path " + f + " parent " + path);
                System.err.println("mkdirs(): mismatched " + store + " and " + s);
                throw new IOException("Trying to create directories across junctions");
            }
            */
            
            FileHandle dirHandle = store.getFileHandle(parentDir, dir, getCredentials());
            if(dirHandle != null) {
                Nfs3FileAttributes attr = store.getFileAttributes(dirHandle, getCredentials());
                if(attr.getType() != NfsFileType.NFSDIR.toValue()) {
                    throw new FileAlreadyExistsException("Trying to make subdirectory inside a file");
                }
                parentDir = dirHandle;
            } 
            else {
                mkdir(store, parentDir, dir, permission);
                parentDir = store.getFileHandle(parentDir, dir, getCredentials());
            }
        }
        return true;
    }

    private void checkNFSStatus(FileHandle handle, Path path, int status, String NFSCall)
            throws IOException {
        /* If not OK or stale handle, then we are in trouble. */
        if (status != Nfs3Status.NFS3_OK && status != Nfs3Status.NFS3ERR_STALE) {
            String errorMsg = NFSCall + " error: " + status;
            if (path != null) {
                errorMsg += " for path " + path.toUri().getPath();
            }
            throw new IOException(errorMsg);
        }

        /* If handle is stale, remove it from handleCache. */
        if (status == Nfs3Status.NFS3ERR_STALE) {
            handleCache.removeByValue(handle);
            LOG.warn("NFS_GETATTR failed with status=" + status);
        }
    }

    private Boolean remove(Path f) throws IOException {
        NFSv3FileSystemStore store = router.getStore(f);
        
        // Parent and child must be on the same filesystem
        NFSv3FileSystemStore parentStore = router.getStore(f.getParent());
        if(!parentStore.equals(store)) {
            LOG.error("rmdir(): Parent " + f.getParent() + " and child " + f + " are on different junctions");
            throw new IOException("rmdir(): Parent " + f.getParent() + " and child " + f + " are on different junctions");
        }
        
        FileHandle dirHandle = getAndVerifyFileHandle(f.getParent());
        if (dirHandle == null) {
            throw new IOException("remove(): parent of path " + f + " does not exist");
        }

        String pathString = f.toUri().getPath();
        String name = f.getName();
        int status;
        REMOVE3Response remove3Response = store.remove(dirHandle, name, getCredentials());
        status = remove3Response.getStatus();
        if (status != Nfs3Status.NFS3_OK) {
            throw new IOException("remove(): failed for " + f + " with error status " + status);
        }

        // Remove the stale handle from the handle cache
        if (handleCache.get(pathString) != null) {
            handleCache.remove(pathString);
        }
        return true;
    }

    private Boolean rmdir(Path f) throws IOException {
        int status;
        NFSv3FileSystemStore store = router.getStore(f);

        if (f.isRoot() || isRoot(f)) {
            LOG.warn("rmdir(): cannot delete root directory");
            return true;
        }

        // Parent and child must be on the same filesystem
        NFSv3FileSystemStore parentStore = router.getStore(f.getParent());
        if(!parentStore.equals(store)) {
            LOG.error("rmdir(): Parent " + f.getParent() + " and child " + f + " are on different junctions");
            throw new IOException("rmdir(): Parent " + f.getParent() + " and child " + f + " are on different junctions");
        }
        
        String pathString = f.toUri().getPath();
        FileHandle parentDirHandle = getAndVerifyFileHandle(f.getParent());
        String name = f.getName();

        if (parentDirHandle == null) {
            throw new IOException("rmdir(): parent dir " + f.getParent() + " does not exist");
        }

        RMDIR3Response rmdir3Response = store.rmdir(parentDirHandle, name, getCredentials());
        status = rmdir3Response.getStatus();
        if (status != Nfs3Status.NFS3_OK) {
            throw new IOException("rmdir(): failed for " + f + " with error status " + status);
        }

        // Remove the stale handle from the handle cache
        if (handleCache.get(pathString) != null) {
            handleCache.remove(pathString);
        }

        return true;
    }

    private Set<Path> listSubPaths(Path f) throws IOException {
        f = makeAbsolute(f);
        NFSv3FileSystemStore store = router.getStore(f);
        Path fsPath = Path.getPathWithoutSchemeAndAuthority(f);
        FileStatus fileStatus = getFileStatus(f);

        // Return null if it is a file
        if (!fileStatus.isDirectory()) {
            return null;
        }

        FileHandle handle = getAndVerifyFileHandle(f);
        if (handle == null) {
            LOG.info("Directory to list does not exist: " + f);
            return null;
        }

        // Read in all entries in this directory
        Set<Path> paths = new TreeSet<>();
        long cookie = 0;
        long cookieVerf = 0;

        // Keep fetching directory entries until the list stops
        while (true) {
            Nfs3DirList dirList
                    = store.getDirectoryList(handle, cookie, cookieVerf, store.getDirListSize(),
                            getCredentials());
            if (dirList != null) {
                List<Nfs3DirEntry> entryList = dirList.getEntries();
                for (Nfs3DirEntry entry : entryList) {
                    cookie = entry.getCookie();
                    // Ignore dot and dot-dot entries
                    if (entry.getName().equals(UNIX_DOT_DIR) || entry.getName().equals(UNIX_DOT_DOT_DIR) || entry.getName().equals(".vsadmin")) {
                        continue;
                    }

                    Path newPath;
                    if (fsPath.isRoot()) {
                        newPath = new Path(Path.SEPARATOR + entry.getName());
                    } else {
                        newPath = new Path(fsPath.toString() + Path.SEPARATOR + entry.getName());
                    }
                    paths.add(newPath);

                }
            }

            // Check for more entries
            if (dirList == null || dirList.isEof()) {
                break;
            } else {
                cookieVerf = dirList.getCookieVerf();
            }
        }

        return paths;
    }

    private boolean mkdir(NFSv3FileSystemStore store, FileHandle dir, String name, FsPermission permission) throws IOException {
        int status;
        EnumSet<SetAttrField> updateFields = EnumSet.noneOf(SetAttrField.class);
        /*
         * Note we do not set a specific size for a directory. NFS server should be able to figure it
         * out when creating it. We also not set the mtime and ctime. Use the timestamp at the server
         * machine.
         */
        updateFields.add(SetAttr3.SetAttrField.UID);
        updateFields.add(SetAttr3.SetAttrField.GID);
        updateFields.add(SetAttr3.SetAttrField.MODE);
        Nfs3SetAttr objAttr
                = new Nfs3SetAttr(permission.toShort(), NFS_UID, NFS_GID, 0, null, null, updateFields);

        MKDIR3Response mkdir3Response = store.mkdir(dir, name, objAttr, getCredentials());
        status = mkdir3Response.getStatus();
        if (status != Nfs3Status.NFS3_OK) {
            if (status == Nfs3Status.NFS3ERR_EXIST) {
                LOG.error("mkdir(): Could not create directory with name " + name);
                throw new FileAlreadyExistsException();
            } else if (status == Nfs3Status.NFS3ERR_NOTDIR) {
                throw new ParentNotDirectoryException();
            } else {
                throw new IOException("mkdir(): returned error status " + status);
            }
        }
        return true;
    }

    private FileHandle create(NFSv3FileSystemStore store, FileHandle dir, String name, FsPermission permission)
            throws IOException {
        EnumSet<SetAttrField> updateFields = EnumSet.noneOf(SetAttrField.class);
        updateFields.add(SetAttr3.SetAttrField.UID);
        updateFields.add(SetAttr3.SetAttrField.GID);
        updateFields.add(SetAttr3.SetAttrField.MODE);
        Nfs3SetAttr objAttr
                = new Nfs3SetAttr(permission.toShort(), NFS_UID, NFS_GID, 0, null, null, updateFields);

        CREATE3Response create3Response
                = store.create(dir, name, Nfs3Constant.CREATE_UNCHECKED, objAttr, 0, getCredentials());
        int status = create3Response.getStatus();
        if (status != Nfs3Status.NFS3_OK) {
            throw new IOException("create(): returned error status " + status);
        }

        FileHandle handle = create3Response.getObjHandle();
        return handle;
    }

    private void truncate(NFSv3FileSystemStore store, FileHandle handle, long newSize) throws IOException {
        int status;
        EnumSet<SetAttrField> updateFields = EnumSet.noneOf(SetAttrField.class);
        updateFields.add(SetAttr3.SetAttrField.SIZE);
        Nfs3SetAttr objAttr = new Nfs3SetAttr();
        objAttr.setUpdateFields(updateFields);
        objAttr.setSize(newSize);

        SETATTR3Response setAttr3Response
                = store.setattr(handle, objAttr, false, null, getCredentials());
        status = setAttr3Response.getStatus();
        checkNFSStatus(handle, null, status, "NFS_SETATTR");
    }

    private FileHandle getAndVerifyFileHandle(Path path) throws IOException {
        int status;
        if (path == null) {
            return null;
        }

        path = makeAbsolute(path);
        NFSv3FileSystemStore store = router.getStore(path);
        Path fsPath = Path.getPathWithoutSchemeAndAuthority(path);
        FileHandle handle;

        // Root paths (top root or junctioned-root)
        if (fsPath.isRoot() || isRoot(path)) {
            handle = store.getRootFileHandle();
            GETATTR3Response getAttr3Response = store.getattr(handle, getCredentials());
            status = getAttr3Response.getStatus();
            if (status != Nfs3Status.NFS3_OK) {
                throw new IOException("getAndVerifyHandle(): Could not get attributes for path " + path);
            }
            return handle;
        }

        // Make sure parent and child are in the same junctioned filesystem
        NFSv3FileSystemStore parentStore = router.getStore(path.getParent());
        if(!parentStore.equals(store)) {
            throw new IOException("getAndVerifyHandle(): Parent " + path.getParent() + " and child " + path + " are not on the same filesystem!");
        }
        
        // If the handle is in the cache and valid, return it 
        handle = handleCache.get(fsPath.toString());
        if (handle != null) {
            GETATTR3Response getAttr3Response = store.getattr(handle, getCredentials());
            status = getAttr3Response.getStatus();
            if (status == Nfs3Status.NFS3_OK) {
                return handle;
            } else {
                // we have a stale handle in the handle cache, remove it
                assert (status == Nfs3Status.NFS3ERR_STALE);
                handleCache.remove(fsPath.toString());
            }
        }

        // else, get the valid parent handle and then lookup for the handle
        FileHandle parentHandle = getAndVerifyFileHandle(path.getParent());
        if (parentHandle == null) {
            LOG.info("getAndVerifyHandle(): Parent path " + path.getParent() + " could not be found");
            return null;
        }
        
        handle = store.getFileHandle(parentHandle, fsPath.getName(), getCredentials());
        if(handle != null) {
            handleCache.put(fsPath.toString(), handle);
        }
        return handle;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        f = makeAbsolute(f);
        NFSv3FileSystemStore store = router.getStore(f);

        FileHandle handle = getAndVerifyFileHandle(f);
        if (handle == null) {
            throw new FileNotFoundException("getFileStatus(): file " + f + " does not exist");
        }

        Nfs3FileAttributes fileAttr = store.getFileAttributes(handle, getCredentials());
        if (fileAttr == null) {
            throw new IOException("getFileStatus(): could not get attributes of file " + f);
        }

        Boolean isDir = false;
        if (fileAttr.getType() == NfsFileType.NFSDIR.toValue()) {
            isDir = true;
        }

        FileStatus fileStatus
                = new FileStatus(fileAttr.getSize(), isDir, 1, getSplitSize(), fileAttr.getMtime()
                        .getMilliSeconds(), fileAttr.getAtime().getMilliSeconds(), new FsPermission(
                                (short) fileAttr.getMode()), NFS_USER_NAME, NFS_GROUP_NAME, f.makeQualified(uri,
                                workingDir));
        return fileStatus;
    }

    protected boolean isRoot(Path path) throws IOException {
        if(space == null) {
            throw new IOException("isRoot(); Namespace is null");
        }
        if(path == null) {
            throw new IOException("isRoot(): Path is null");
        }
        
        NFSv3FileSystemStore store = router.getStore(path);
        Path fsPath = Path.getPathWithoutSchemeAndAuthority(path);
        return store.getEndpoint().getPath().equals(fsPath.toString());
    }
    
    protected Credentials getCredentials() throws IOException {
        if (space == null) {
            throw new IOException("No namespace defined!");
        }

        NamespaceOptions options = space.getConfiguration();
        String authScheme = (options.getNfsAuthScheme() == null) ? NamespaceOptions.getDefaultOptions().getNfsAuthScheme() : options.getNfsAuthScheme();
        if (authScheme.equalsIgnoreCase("AUTH_SYS") || authScheme.equalsIgnoreCase("AUTH_UNIX")) {
            CredentialsSys sys = new CredentialsSys();
            sys.setUID(options.getNfsUid());
            sys.setGID(options.getNfsGid());
            sys.setStamp(new Long(System.currentTimeMillis()).intValue());
            return sys;
        } else {
            return new CredentialsNone();
        }
    }

} // End class
