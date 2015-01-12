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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.nfs.NfsFileType;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3DirList;
import org.apache.hadoop.nfs.nfs3.Nfs3DirList.Nfs3DirEntry;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Info;
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
import org.apache.hadoop.portmap.PortmapMapping;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.nfs.portmap.PortmapClient;
import org.apache.hadoop.fs.nfs.stream.NFSBufferedInputStream;
import org.apache.hadoop.fs.nfs.stream.NFSBufferedOutputStream;

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
  private String scheme;
  private String authority;
  private String hostname;
  private int port;

  private NFSv3FileSystemStore store;

  private HandleCache handleCache;
  public static final int FILE_HANDLE_CACHE_SIZE = 256;

  // Number of connections
  public static final int DEFAULT_NUM_CONNECTIONS = 4;
  public static final int MAX_NUM_CONNECTIONS = 32;
  public static final int MIN_NUM_CONNECTIONS = 1;
  private int numConnections;
  
  // Read/write block size in bytes
  public static final int DEFAULT_READ_BLOCK_SIZE_BITS = 20;
  public static final int DEFAULT_WRITE_BLOCK_SIZE_BITS = 20;
  private int readBlockSizeBits;
  private int writeBlockSizeBits;
  
  // Default file split size
  public static final int DEFAULT_NFS_SPLIT_SIZE_BITS = 28;
  private int splitSizeBits;

  // Default UID and GID when creating a new file/directory
  public static final int NFS_UID_DEFAULT = 0;
  public static final int NFS_GID_DEFAULT = 0;
  public static final String NFS_USER_NAME_DEFAULT = "root";
  public static final String NFS_GROUP_NAME_DEFAULT = "root";
  public static final String NFS_URI_SCHEME = "nfs";
  
  // Actual UID, GID, user name and group name to use when creating a new file/directory. Values
  // from the configuration file will overwrite default values defined above.
  private int NFS_UID;
  private int NFS_GID;
  private String NFS_USER_NAME;
  private String NFS_GROUP_NAME;
  
  public final static Log LOG = LogFactory.getLog(NFSv3FileSystem.class);
  private Nfs3Info fsInfo;

  private final static String UNIX_DOT_DIR = ".";
  private final static String UNIX_DOT_DOT_DIR = "..";

  public long getNfsBlockSize() {
    return (1L << readBlockSizeBits);
  }

  public long getSplitSize() {
    return (1L << splitSizeBits);
  }
  
  public NFSv3FileSystem() {
    // set store in initialize()
  }

  @Override
  public String getScheme() {
    return NFS_URI_SCHEME;
  }

  @Override
  public URI getUri() {
    LOG.debug("START/END getUri(): uri=" + uri);
    return uri;
  }

  @Override
  public void close() throws IOException {
    store.shutdown();
  }
  
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {

    super.initialize(uri, conf);

    // Save configuration
    this.configuration = conf;
    
    // Get scheme
    scheme = uri.getScheme();
    if (scheme == null || scheme.equals(NFS_URI_SCHEME) == false) {
      LOG.error("NFS URI scheme is wrong! It was " + scheme
          + " but it should be " + NFS_URI_SCHEME);
      throw new IOException("Scheme in URI is wrong! It was " + scheme);
    }

    // Get authority
    authority = uri.getAuthority();
    if (authority == null) {
      LOG.error("NFS URI authority is wrong! It was " + authority);
      throw new IOException("NFS URI authority is wrong! It was " + authority);
    }

    // Get NFS server name from the URI or config file
    // if URI does not specify hostname, look for the "fs.nfs.server" property
    // in the configuration
    hostname = uri.getHost();
    if (hostname == null) {
      hostname = conf.get("fs.nfs.server", null);
    }
    if (hostname == null) {
      LOG.error("NFS URI hostname is wrong! It was " + hostname);
      throw new IOException("NFS URI hostname is wrong! It was " + hostname);
    }
    
    // Set the "fs.nfs.server" in the conf to be the final one
    conf.set("fs.nfs.server", hostname);
    this.uri = URI.create(scheme + "://" + authority);

    // Get NFS host port information from portmap protocol. 
    // This is the port we should use.
    int port = getNFSPortFromPortMap();
    if (port <= 0) {
      throw new IOException("NFS program is not registered!");
    }
    
    // Verify the port from the URI.
    int portInUri = uri.getPort();
    if (portInUri != -1 && portInUri != port) {
      LOG.warn("Port specified in URI is different from port gotten from portmap: "
          + uri.getPort() + "!=" + port);
    }
    if (port != Nfs3Constant.NFS3_SERVER_PORT_DEFAULT) {
      LOG.warn("Port gotten from portmap is different from default nfs port: "
          + port + "!=" + Nfs3Constant.NFS3_SERVER_PORT_DEFAULT);
    }
    conf.setInt(Nfs3Constant.NFS3_SERVER_PORT, port);

    setConf(conf);
    initializeInternal(this.uri, conf);

    this.handleCache = new HandleCache(FILE_HANDLE_CACHE_SIZE);
    workingDir = getHomeDirectory();
  }

  private void initializeInternal(URI uri, Configuration conf)
      throws IOException {

    // Get configuration parameters for read/write block size
    readBlockSizeBits =
        conf.getInt("fs." + uri.getScheme() + ".readblockbits",
            DEFAULT_READ_BLOCK_SIZE_BITS);
    writeBlockSizeBits =
        conf.getInt("fs." + uri.getScheme() + ".writeblockbits",
            DEFAULT_WRITE_BLOCK_SIZE_BITS);
    splitSizeBits = conf.getInt("fs." + uri.getScheme() + ".splitsizebits", DEFAULT_NFS_SPLIT_SIZE_BITS);
    
    // Get configuration values for UID, GID, user name and group name 
    NFS_UID = conf.getInt("fs." + uri.getScheme() + ".uid", NFS_UID_DEFAULT);
    NFS_GID = conf.getInt("fs." + uri.getScheme() + ".gid", NFS_GID_DEFAULT);
    NFS_USER_NAME = conf.get("fs." + uri.getScheme() + ".username", NFS_USER_NAME_DEFAULT);
    NFS_GROUP_NAME = conf.get("fs." + uri.getScheme() + ".groupname", NFS_GROUP_NAME_DEFAULT);
    LOG.info("Using credentials uid=" + NFS_UID + ", gid=" + NFS_GID + ", username=" + NFS_USER_NAME + ", groupname=" + NFS_GROUP_NAME);
    
    // Number of connections to NFS
    numConnections = Math.min(MAX_NUM_CONNECTIONS, Math.max(MIN_NUM_CONNECTIONS, conf.getInt("fs." + uri.getScheme() + ".numconnections", DEFAULT_NUM_CONNECTIONS)));
    
    // Initialize the filesystem store
    store = new NFSv3FileSystemStore(uri, conf, numConnections);
    store.initialize(uri, conf);

    // Get FileSystem Information. We need dtpref (the preferred size in bytes)
    // for READDIR request
    fsInfo = store.getFilesystemInfo(store.getRootfh(), store.getCredentials());
    if (fsInfo == null) {
      throw new IOException("NFS_FSINFO error: result is null");
    }

    // Check the values from fsinfo for read/write block size
    int Rtmax = fsInfo.getRtmax();
    if (Rtmax > 0 && ((1 << readBlockSizeBits) > Rtmax)) {
      LOG.warn("Overwrite readBlockSize with Rtmax gottern from NFS server: "
          + (1 << readBlockSizeBits) + "=>" + Rtmax);
      readBlockSizeBits = 31 - Integer.numberOfLeadingZeros(Rtmax);
    }
    
    int Wtmax = fsInfo.getWtmax();
    if (Wtmax > 0 && ((1 << writeBlockSizeBits) > Wtmax)) {
      LOG.warn("Overwrite writeBlockSize with Wtmax gottern from NFS server: "
          + (1 << writeBlockSizeBits) + "=>" + Wtmax);
      writeBlockSizeBits = 31 - Integer.numberOfLeadingZeros(Wtmax);
    }
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
      throws IOException {
    if (p == null) {
      LOG.debug("END getFileBlockLocations(): file is null");
      throw new NullPointerException();
    }
    
    FileStatus file = getFileStatus(p);
    BlockLocation[] locations = getFileBlockLocations(file, start, len);
    return locations;
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
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

    String[] name = { hostname + ":" + port };
    String[] host = { hostname };
    BlockLocation locations[] =
        new BlockLocation[] { new BlockLocation(name, host, 0, file.getLen()) };
    return locations;
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    workingDir = makeAbsolute(new_dir);
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute() && !path.isAbsoluteAndSchemeAuthorityNull()) {
      return path;
    }
    Path newPath = new Path(workingDir, path);
    return newPath;
  }

  @Override
  public Path getHomeDirectory() {
    Path homeDir = makeQualified(new Path("/"));
    return homeDir;
  }

  public NFSv3FileSystem(NFSv3FileSystemStore store) {
    this.store = store;
  }

  public NFSv3FileSystem(URI uri, Configuration conf) throws IOException {
    initialize(uri, conf);
  }

  protected int getWriteBlockSizeBits() {
    return writeBlockSizeBits;
  }

  public Path makeQualified(Path path) {
    checkPath(path);
    Path p = path.makeQualified(this.getUri(), this.getWorkingDirectory());
    return p;
  }

  private int getNFSPortFromPortMap() throws IOException {
    if (uri == null) {
      throw new IOException("URI is not defined yet!");
    }

    PortmapClient portmapClient =
        new PortmapClient(uri.getHost(), Nfs3Constant.SUN_RPCBIND);
    int nfsPort =
        portmapClient.getport(Nfs3Constant.PROGRAM, Nfs3Constant.VERSION,
            PortmapMapping.TRANSPORT_TCP);
    portmapClient.shutdown();
    
    return nfsPort;
  }

  public int getBlockSizeBits() {
    return readBlockSizeBits;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    f = makeAbsolute(f);
    FileHandle handle = getAndVerifyFileHandle(f);
    if (handle == null) {
      LOG.error("open: handle is undefined for file" + f.toUri().getPath());
      return null;
    }
    
    return new FSDataInputStream(new BufferedFSInputStream(new NFSBufferedInputStream(
        configuration, store, handle, f, this.getConf(), this.getBlockSizeBits(), this
            .getUri().getScheme(), this.getSplitSize(), statistics), bufferSize));

  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    f = makeAbsolute(f);
    FileHandle handle = getAndVerifyFileHandle(f);
    if (handle == null) {
      throw new IOException("File does not exist: " + f);
    }
    
    return new FSDataOutputStream(new NFSBufferedOutputStream(configuration, handle, f, store,
        this.getWriteBlockSizeBits(), true), statistics);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    f = makeAbsolute(f);
    FileHandle handle = getAndVerifyFileHandle(f);
    
    if (handle == null) {
      Path parent = f.getParent();
      FileHandle parentHandle=null;
      if (parent.isRoot()) {
        parentHandle = store.getRootfh();
      } else {
        if (mkdirs(parent)) {
          parentHandle = getAndVerifyFileHandle(parent);
        } else {
          throw new IOException("Failed to create parent dir: " + parent);
        }
      }

      // At this point, we should have a valid parent handle.
      if (parentHandle == null){
        throw new IOException("parenthandle is null for creating: " + f);
      }
      
      handle = create(parentHandle, f.getName(), permission);
    } 
    else {
      if (overwrite != true) {
        throw new IOException("File already exists: " + f);
      }
      truncate(handle, 0);
    }
    return new FSDataOutputStream(new NFSBufferedOutputStream(configuration, handle, f, store,
        this.getWriteBlockSizeBits(), false), statistics);
  }

  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    
    if(flags.contains(CreateFlag.APPEND)) {
      return append(f, bufferSize, progress);
    }
    else if(flags.contains(CreateFlag.OVERWRITE)) {
      return this.create(f, permission, true, bufferSize, replication, blockSize, progress);
    }
    else {
      return this.create(f, permission, false, bufferSize, replication, blockSize, progress);
    }
    
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    src = makeAbsolute(src);
    dst = makeAbsolute(dst);
    
    FileStatus srcStatus;
    FileStatus dstStatus;
    
    LOG.info("Rename from " + src.toUri() + " to " + dst.toUri());
    
    // Check status of src and dst paths
    try {
      srcStatus = getFileStatus(src);
    } catch(FileNotFoundException exception) {
      srcStatus = null;
    }
    
    try {
      dstStatus = getFileStatus(dst);
    } catch(FileNotFoundException exception) {
      dstStatus = null;
    }
    
    // Source path must exist
    if(srcStatus == null) {
      LOG.warn("Source path does not exist");
      return false;
    }
    
    FileHandle srcParentHandle = getAndVerifyFileHandle(src.getParent());
    FileHandle dstParentHandle = getAndVerifyFileHandle(dst.getParent());
    
    if (srcParentHandle == null) {
      LOG.warn("Source parent does not exist");
      return false;
    }
    
    if (dstParentHandle == null) {
      LOG.warn("Destination parent does not exist");
      return false;
    }
    
    int status = Nfs3Status.NFS3ERR_IO;

    // Destination is a file
    if(dstStatus != null && dstStatus.isFile()) {
        if(srcStatus.isDirectory()) {
          LOG.warn("Trying to rename file to an existing directory");
          return false;
        } else if(srcStatus.isFile()) {
          LOG.warn("Trying to rename over an existing file");
          return false;
        } else {
          throw new IOException("Source is neither a file nor a directory");
        }
    }
    // Destination is a directory, so move source into destination
    else if(dstStatus != null && dstStatus.isDirectory()) {
      FileHandle dstHandle = getAndVerifyFileHandle(dst);
      if(dstHandle ==null) {
        throw new IOException("Destination is a directory but its handle cannot be found");
      }
      RENAME3Response rename3Response =
          store.rename(srcParentHandle, src.getName(), dstHandle, src.getName(),
              store.getCredentials());
      status = rename3Response.getStatus();
    }
    // Destination does not exist
    else if(dstStatus == null) {
      RENAME3Response rename3Response =
          store.rename(srcParentHandle, src.getName(), dstParentHandle, dst.getName(),
              store.getCredentials());
      status = rename3Response.getStatus();
    }
    
    if (status != Nfs3Status.NFS3_OK) {
      throw new IOException("rename error status=" + status);
    }

    // Remove old handles
    handleCache.removeAll(Path.getPathWithoutSchemeAndAuthority(src).toString());
    
    return true;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    f = makeAbsolute(f);
    FileHandle handle = getAndVerifyFileHandle(f);
    if (handle == null) {
      LOG.info("file " + f.toUri().getPath() + " does not exist");
      return false;
    }

    Nfs3FileAttributes attributes = store.getFileAttributes(handle, store.getCredentials());
    if(attributes == null) {
      throw new IOException("Could not get file attributes for path=" + f);
    }
    
    int fileType = attributes.getType();
    if (fileType == NfsFileType.NFSREG.toValue()) {
      return remove(f);
    } else if (fileType == NfsFileType.NFSDIR.toValue()) {
      Set<Path> subPaths = listSubPaths(f);
      if(recursive == false && (subPaths != null && subPaths.isEmpty() == false)) {
        throw new IOException("Directory is not empty so it cannot be deleted");
      }
      
      /*
       * Return false if recursive is not set. The upper layer seems to handle
       * this case already. So, this should not happen.
       */
      /*
      if (recursive == false) {
        return false;
      }
       */
      FileStatus[] files = listStatus(f);
      for (FileStatus fileStatus : files) {
        if (delete(fileStatus.getPath(), recursive) == false) {
          LOG.error("delete failed for " + fileStatus);
          return false;
        }
      }
      return rmdir(f);
    } else {
      throw new IOException(
          "Delete error: file type not supported beyond regular file and dir");
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException,
      IOException {
    f = makeAbsolute(f);
    FileStatus fileStatus = getFileStatus(f);
    if (!fileStatus.isDirectory()) {
      //fileStatus.setPath(Path.getPathWithoutSchemeAndAuthority(f));
      return new FileStatus[] { fileStatus };
    }

    // Path f is a dir, return a list of all files/dir in this dir
    ArrayList<FileStatus> fileStatusList = new ArrayList<FileStatus>();
    for (Path path : listSubPaths(f)) {
      FileStatus s1 = getFileStatus(path);
      //s1.setPath(Path.getPathWithoutSchemeAndAuthority(path));
      fileStatusList.add(s1);
    }
    FileStatus[] array = fileStatusList.toArray(new FileStatus[0]);
    return array;
  }


  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    f = makeAbsolute(f);

    List<String> dirs = new LinkedList<String>();
    Path path = Path.getPathWithoutSchemeAndAuthority(f);
    do {
      dirs.add(0, path.getName());
      path = path.getParent();
    } while (path.isRoot() != true);

    path = null;
    FileHandle parentDir = store.getRootfh();
    for (String dir : dirs) {
      if (path == null) {
        path = new Path(Path.SEPARATOR + dir);
      } else {
        path = new Path(path.toString() + Path.SEPARATOR + dir);
      }
      if (handleCache.get(path.toString()) != null) {
        // Check for stale filehandle for parentDir
        parentDir = handleCache.get(path.toString());
        continue;
      }

      // create the subdir and then set parent dir to be subdir and continue.
      // The subdir could exist already. 
      mkdir(parentDir, dir, permission);

      parentDir = store.getFileHandle(parentDir, dir, store.getCredentials());
      if(parentDir != null) {
        handleCache.put(path.toString(), parentDir);
      } else {
        //FIXME Check for stale handle
      }

    }
    return true;
  }

  private void checkNFSStatus(FileHandle handle, Path path, int status,
      String NFSCall) throws IOException {

    /* If not OK or stale handle, then we are in trouble. */
    if (status != Nfs3Status.NFS3_OK && status != Nfs3Status.NFS3ERR_STALE) {
      String errorMsg =
          NFSCall + " error: " + status;
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

  private Boolean remove(Path file) throws IOException {
    String pathString = file.toUri().getPath();
    FileHandle dirHandle = getAndVerifyFileHandle(file.getParent());
    String name = file.getName();
    int status;

    if (dirHandle == null) {
      throw new IOException("remove error: parent dir does not exist");
    }

    REMOVE3Response remove3Response =
        store.remove(dirHandle, name, store.getCredentials());
    status = remove3Response.getStatus();
    if (status != Nfs3Status.NFS3_OK) {
      throw new IOException("NFS_REMOVE failed for " + file + " with error status=" + status);
    }

    // Remove the stale handle from the handle cache
    if (handleCache.get(pathString) != null) {
      handleCache.remove(pathString);
    }
    return true;
  }

  private Boolean rmdir(Path dir) throws IOException {
    int status;
    String pathString = dir.toUri().getPath();
    FileHandle parentDirHandle = getAndVerifyFileHandle(dir.getParent());
    String name = dir.getName();

    if (parentDirHandle == null) {
      throw new IOException("rmdir error: parent dir does not exist");
    }

    RMDIR3Response rmdir3Response =
        store.rmdir(parentDirHandle, name, store.getCredentials());
    status = rmdir3Response.getStatus();
    if (status != Nfs3Status.NFS3_OK) {
      throw new IOException("NFS_RMDIR failed for " + dir + " with error status=" + status);
    }

    // Remove the stale handle from the handle cache
    if (handleCache.get(pathString) != null) {
      LOG.debug("remove handle " + handleCache.get(pathString) + " for file "
          + pathString);
      handleCache.remove(pathString);
    }

    return true;
  }

  private Set<Path> listSubPaths(Path path) throws IOException {
    path = makeAbsolute(path);
    Path fsPath = Path.getPathWithoutSchemeAndAuthority(path);
    FileStatus fileStatus = getFileStatus(path);

    // Return null if it is a file
    if (!fileStatus.isDirectory()) {
      return null;
    }

    FileHandle handle = getAndVerifyFileHandle(path);
    if (handle == null) {
      LOG.info("Directory to list does not exist: " + path);
      return null;
    }

    // Read in all entries in this directory
    Set<Path> paths = new TreeSet<Path>();
    long cookie = 0;
    long cookieVerf = 0;

    // Keep fetching directory entries until the list stops
    while(true) {
      Nfs3DirList dirList = store.getDirectoryList(handle, cookie, cookieVerf, fsInfo.getDtpref(), store.getCredentials());
      if(dirList != null) {
        List<Nfs3DirEntry> entryList = dirList.getEntries();
        for(Nfs3DirEntry entry : entryList) {
          // Ignore dot and dot-dot entries
          if(entry.getName().equals(UNIX_DOT_DIR) || entry.getName().equals(UNIX_DOT_DOT_DIR)) {
            continue;
          }
          
          Path newPath;
          if(fsPath.isRoot()) {
            newPath = new Path(Path.SEPARATOR + entry.getName());
          } else {
            newPath = new Path(fsPath.toString() + Path.SEPARATOR + entry.getName());
          }
          paths.add(newPath);
          cookie = entry.getCookie();
        }
      }
      
      // Check for more entries
      if(dirList.isEof()) {
        break;
      } else {
        cookieVerf = dirList.getCookieVerf();
      }
    }

    return paths;
  }

  private void mkdir(FileHandle dir, String name, FsPermission permission)
      throws IOException {
    int status;

    EnumSet<SetAttrField> updateFields = EnumSet.noneOf(SetAttrField.class);
    /*
     * Note we do not set a specific size for a directory. NFS server should be
     * able to figure it out when creating it. We also not set the mtime and
     * ctime. Use the timestamp at the server machine.
     */
    updateFields.add(SetAttr3.SetAttrField.UID);
    updateFields.add(SetAttr3.SetAttrField.GID);
    updateFields.add(SetAttr3.SetAttrField.MODE);
    Nfs3SetAttr objAttr = new Nfs3SetAttr(permission.toShort(), NFS_UID, NFS_GID, 0,
            null, null, updateFields);

    MKDIR3Response mkdir3Response =
        store.mkdir(dir, name, objAttr, store.getCredentials());
    status = mkdir3Response.getStatus();
    if (status != Nfs3Status.NFS3_OK) {
      if (status == Nfs3Status.NFS3ERR_EXIST) {
        LOG.debug("NFSFileSystem mkdir: already exists for handle=" + dir
            + ", dir=" + name);
      } else {
        throw new IOException("NFSFileSystem mkdir error: status=" + status);
      }
    }

  }

  private FileHandle create(FileHandle dir, String name, FsPermission permission)
      throws IOException {
    FileHandle handle = null;

    EnumSet<SetAttrField> updateFields = EnumSet.noneOf(SetAttrField.class);
    updateFields.add(SetAttr3.SetAttrField.UID);
    updateFields.add(SetAttr3.SetAttrField.GID);
    updateFields.add(SetAttr3.SetAttrField.MODE);
    Nfs3SetAttr objAttr = new Nfs3SetAttr(permission.toShort(), NFS_UID, NFS_GID, 0,
            null, null, updateFields);

    CREATE3Response create3Response =
        store.create(dir, name, Nfs3Constant.CREATE_UNCHECKED, objAttr, 0,
            store.getCredentials());
    int status = create3Response.getStatus();
    if (status != Nfs3Status.NFS3_OK) {
      throw new IOException("NFSFileSystem create error: status=" + status);
    }
    
    handle = create3Response.getObjHandle();
    return handle;
  }

  private void truncate(FileHandle handle, long newSize) throws IOException {
    int status;
    EnumSet<SetAttrField> updateFields = EnumSet.noneOf(SetAttrField.class);
    updateFields.add(SetAttr3.SetAttrField.SIZE);
    Nfs3SetAttr objAttr = new Nfs3SetAttr();
    objAttr.setUpdateFields(updateFields);
    objAttr.setSize(newSize);

    SETATTR3Response setAttr3Response =
        store.setattr(handle, objAttr, false, null, store.getCredentials());
    status = setAttr3Response.getStatus();
    checkNFSStatus(handle, null, status, "NFS_SETATTR");
  }

  @SuppressWarnings("unused")
  @Deprecated
  private FileHandle getFileHandle(Path path) throws IOException {
    path = makeAbsolute(path);
    Path FsPath = Path.getPathWithoutSchemeAndAuthority(path);
    FileHandle handle = null;

    if (FsPath.isRoot()) {
      return store.getRootfh();
    }

    handle = handleCache.get(FsPath.toString());
    if (handle != null) {
      return handle;
    } else {
      FileHandle parentHandle = getFileHandle(path.getParent());
      if (parentHandle == null) {
        return null;
      }
      
      handle = store.getFileHandle(parentHandle, FsPath.getName(), store.getCredentials());
      // FIXME Check that handle is not stale and checkNFSStatus
      handleCache.put(FsPath.toString(), handle);
    }

    return handle;
  }

  private FileHandle getAndVerifyFileHandle(Path path) throws IOException {
    path = makeAbsolute(path);
    int status;
    Path FsPath = Path.getPathWithoutSchemeAndAuthority(path);
    FileHandle handle = null;

    if (FsPath.isRoot()) {
      handle = store.getRootfh();
      GETATTR3Response getAttr3Response = store.getattr(handle, store.getCredentials());
      status = getAttr3Response.getStatus(); 
      if (status != Nfs3Status.NFS3_OK) {
        throw new IOException("NFS_GETATTR failed for root handle with error: status=" + status);
      }
      return handle;
    }

    // If the handle is in the cache and valid, return it
    handle = handleCache.get(FsPath.toString());
    if (handle != null) {
      GETATTR3Response getAttr3Response = store.getattr(handle, store.getCredentials());
      status = getAttr3Response.getStatus(); 
      checkNFSStatus(handle, FsPath, status, "NFS_GETATTR");
      if (status == Nfs3Status.NFS3_OK) {
        return handle;
      } else {
        // we have a stale handle in the handle cache, remove it
        assert(status == Nfs3Status.NFS3ERR_STALE);
        handleCache.remove(FsPath.toString());
      }
    } 
    
    // else, get the valid parent handle and then lookup for the handle
    FileHandle parentHandle = getAndVerifyFileHandle(path.getParent());
    if (parentHandle == null) {
      return null;
    }
    handle = store.getFileHandle(parentHandle, FsPath.getName(), store.getCredentials());
    //FIXME Check that handle is not stale
    handleCache.put(FsPath.toString(), handle);
    return handle;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    f = makeAbsolute(f);
    FileStatus fileStatus = null;
    
    FileHandle handle = getAndVerifyFileHandle(f);
    if (handle == null) {
      throw new FileNotFoundException("File does not exist: " + f);
    }

    Nfs3FileAttributes fileAttr = store.getFileAttributes(handle, store.getCredentials());
    if(fileAttr == null) {
      throw new IOException("Could not get file attributes for path=" + f);
    }
    
    Boolean isDir = false;
    if (fileAttr.getType() == NfsFileType.NFSDIR.toValue()) {
      isDir = true;
    }

    fileStatus =
        new FileStatus(fileAttr.getSize(), isDir, 1, getSplitSize(),
            fileAttr.getMtime().getMilliSeconds(), fileAttr.getAtime()
                .getMilliSeconds(),
            new FsPermission((short) fileAttr.getMode()),
            NFS_USER_NAME, NFS_GROUP_NAME, f.makeQualified(uri,
                workingDir));
    return fileStatus;
  }
  
} // End class