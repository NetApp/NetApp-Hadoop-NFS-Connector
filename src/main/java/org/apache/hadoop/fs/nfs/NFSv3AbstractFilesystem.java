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
import java.net.URISyntaxException;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

public class NFSv3AbstractFilesystem extends AbstractFileSystem {

	private static final Log LOG = LogFactory.getLog(NFSv3AbstractFilesystem.class);
	private final FileSystem fsImpl;
	
	public NFSv3AbstractFilesystem(final URI uri, final Configuration conf) throws IOException, URISyntaxException {
		this(uri, new NFSv3FileSystem(), conf, uri.getScheme(), false);
		LOG.debug("called another constructor:");
	}
	
	public NFSv3AbstractFilesystem(URI uri, FileSystem fsImpl, Configuration conf, String supportedScheme, boolean authorityNeeded) throws IOException, URISyntaxException {
		super(uri, supportedScheme, authorityNeeded, FileSystem.getDefaultUri(conf).getPort());
		this.fsImpl = fsImpl;
		this.fsImpl.initialize(uri, conf);
	}

	@Override
	public Statistics getStatistics() {
		LOG.debug("getStatistics():");
		Statistics stats = super.getStatistics();
		LOG.debug("getStatistics(): super returned stats=" + stats);
		return stats;
	}
	
	/*
	@Override
	public boolean isValidName(String src) {
		LOG.debug("isValidName(): src=" + src);
		boolean valid = super.isValidName(src);
		LOG.debug("isValidName(): super returned valid=" + valid);
		return valid;
	}
	*/
	
	@Override
	public void checkScheme(URI uri, String supportedScheme) {
		LOG.debug("checkScheme(): uri=" + uri + " supportedScheme=" + supportedScheme);
		super.checkScheme(uri, supportedScheme);
		LOG.debug("checkScheme(): super returned successfully");
	}

	@Override
	public int getUriDefaultPort() {
		return Nfs3Constant.NFS3_SERVER_PORT_DEFAULT;
	}
	
	@Override
	public URI getUri() {
		LOG.debug("getUri():");
		URI uri = super.getUri();
		LOG.debug("getUri(): super returned uri=" + uri);
		return uri;
	}

	@Override
	public void checkPath(Path path) {
		LOG.debug("checkPath(): path=" + path);
		super.checkPath(path);
		LOG.debug("checkPath(): super successfully");
	}
	
	@Override
	public String getUriPath(final Path p) {
		LOG.debug("getUriPath(): path=" + p);
		String s = super.getUriPath(p);
		LOG.debug("getUriPath(): super returned string=" + s);
		return s;
	}
	
	@Override
	public Path makeQualified(Path path) {
		LOG.debug("makeQualified(): path=" + path);
		Path p = super.makeQualified(path);
		LOG.debug("makeQualified(): super returned p=" + p);
		return p;
	}
	
	@Override
	public Path getInitialWorkingDirectory() {
		LOG.debug("getInitialWorkingDirectory():");
		Path p = super.getInitialWorkingDirectory();
		LOG.debug("getInitialWorkingDirectory(): super returned p=" + p);
		return p;
	}
	
	@Override
	public Path getHomeDirectory() {
		LOG.debug("getHomeDirectory():");
		Path p = super.getHomeDirectory();
		LOG.debug("getHomeDirectory(): super returned p=" + p);
		return p;
	}
	
	@Override
	public FsServerDefaults getServerDefaults() throws IOException {
		LOG.debug("getServerDefaults():");
		@SuppressWarnings("deprecation")
    FsServerDefaults defaults = fsImpl.getServerDefaults();
		LOG.debug("getServerDefaults(): fsImpl returned defaults=" + defaults);
		return defaults;
	}

	@Override
	 public Path resolvePath(final Path p) throws FileNotFoundException, UnresolvedLinkException, AccessControlException, IOException {
		LOG.debug("resolvePath(): path=" + p);
		Path path = super.resolvePath(p);
		LOG.debug("resolvePath(): super returned path=" + path);
		return path;
	 }
	
	@Override
	public FSDataOutputStream createInternal(Path f, EnumSet<CreateFlag> flag,
			FsPermission absolutePermission, int bufferSize, short replication,
			long blockSize, Progressable progress, ChecksumOpt checksumOpt,
			boolean createParent) throws AccessControlException,
			FileAlreadyExistsException, FileNotFoundException,
			ParentNotDirectoryException, UnsupportedFileSystemException,
			UnresolvedLinkException, IOException {

		LOG.debug("createInternal(): path=" + f + " flag=" + flag + " permission=" + absolutePermission + " buffersize=" + bufferSize + "replication=" + replication + " blocksize=" + blockSize);
		checkPath(f);

		// Default impl assumes that permissions do not matter
		// calling the regular create is good enough.
		// FSs that implement permissions should override this.
		if (!createParent) { // parent must exist.
			// since this.create makes parent dirs automatically
			// we must throw exception if parent does not exist.
			final FileStatus stat = getFileStatus(f.getParent());
			if (stat == null) {
				throw new FileNotFoundException("Missing parent:" + f);
			}
			if (!stat.isDirectory()) {
				throw new ParentNotDirectoryException("parent is not a dir:" + f);
			}
			// parent does exist - go ahead with create of file.
		}
		
		FSDataOutputStream stream = fsImpl.create(f, absolutePermission, flag, bufferSize, replication, blockSize, progress, checksumOpt);
		LOG.debug("createInternal(): fsImpl returned status=" + stream);
		return stream;
	}

	@Override
	public void mkdir(Path dir, FsPermission permission, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, UnresolvedLinkException, IOException {
		LOG.debug("mkdir():");
		checkPath(dir);
		boolean status = fsImpl.mkdirs(dir, permission);
		LOG.debug("mkdir(): fsImpl returned status=" + status);
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		LOG.debug("delete(): path=" + f + " recursive=" + recursive);
		checkPath(f);
		boolean status = fsImpl.delete(f, recursive);
		LOG.debug("delete(): fsImpl returned status=" + status);
		return status;
	}

	@Override
	public FSDataInputStream open(final Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		LOG.debug("open(): path=" + f);
		checkPath(f);
		FSDataInputStream stream = super.open(f);
		LOG.debug("open(): super returned stream=" + stream);
		return stream;
	}
	
	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		LOG.debug("open(): path=" + f + " buffersize=" + bufferSize);
		checkPath(f);
		FSDataInputStream stream = fsImpl.open(f, bufferSize);
		LOG.debug("open(): fsImpl returned stream=" + stream);
		return stream;
	}

	@Override
	public boolean setReplication(Path f, short replication) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		LOG.debug("setReplication():");
		checkPath(f);
		boolean set = fsImpl.setReplication(f, replication);
		LOG.debug("setReplication(): fsImpl returned set=" + set);
		return set;
	}

	@Override
	public void renameInternal(Path src, Path dst) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnresolvedLinkException, IOException {
		LOG.debug("renameInternal():");
		checkPath(src);
		checkPath(dst);
		fsImpl.rename(src, dst);
		LOG.debug("renameInternal(): fsImpl returned successfully");
	}
	
	@Override
	 public void renameInternal(final Path src, final Path dst, boolean overwrite) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnresolvedLinkException, IOException {
		LOG.debug("renameInternal(): src=" + src + " dst=" + dst + " overwrite=" + overwrite);
		super.renameInternal(src, dst, overwrite);
		LOG.debug("renameInternal(): super returned successfully");
	}

	@Override
	public boolean supportsSymlinks() {
		LOG.debug("supportSymlinks():");
		boolean support = super.supportsSymlinks();
		LOG.debug("supportSymlinks(): supports=" + support);
		return support;
	}
	
	@Override
	public void setPermission(Path f, FsPermission permission) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		LOG.debug("setPermission():");
		checkPath(f);
		fsImpl.setPermission(f, permission);
		LOG.debug("setPermission(): fsImpl returned successfully");
	}

	@Override
	public void setOwner(Path f, String username, String groupname) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		LOG.debug("setOwner():");
		checkPath(f);
		fsImpl.setOwner(f, username, groupname);
		LOG.debug("setOwner(): fsImpl returned successfully");
	}

	@Override
	public void setTimes(Path f, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		LOG.debug("setTimes():");
		checkPath(f);
		fsImpl.setTimes(f, mtime, atime);
		LOG.debug("setTimes(): fsImpl returned successfully");
	}

	@Override
	public FileChecksum getFileChecksum(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		LOG.debug("getFileChecksum():");
		checkPath(f);
		FileChecksum checksum = fsImpl.getFileChecksum(f);
		LOG.debug("getFileChecksum(): fsImpl returned status=" + checksum);
		return checksum;
	}

	@Override
	public FileStatus getFileStatus(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		LOG.debug("getFileStatus():");
		checkPath(f);
		FileStatus status = fsImpl.getFileStatus(f);
		LOG.debug("getFileStatus(): fsImpl returned status=" + status);
		return status;
	}

	@Override
	public BlockLocation[] getFileBlockLocations(Path f, long start, long len) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		LOG.debug("getFileBlockLocations():");
		checkPath(f);
		BlockLocation[] locations = fsImpl.getFileBlockLocations(f, start, len);
		LOG.debug("getFileBlockLocations(): fsImpl returned locations=" + locations);
		return locations;
	}

	@Override
	public FsStatus getFsStatus() throws AccessControlException, FileNotFoundException, IOException {
		LOG.debug("getFsStatus():");
		FsStatus status = fsImpl.getStatus();
		LOG.debug("getFsStatus(): fsImpl returned status=" + status);
		return status;
	}

	@Override
	public FileStatus[] listStatus(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		LOG.debug("listStatus(): path=" + f);
		checkPath(f);
		FileStatus[] status = fsImpl.listStatus(f);
		LOG.debug("listStatus(): fsImpl returned status=" + status);
		return status;
	}

	@Override
	public void setVerifyChecksum(boolean verifyChecksum) throws AccessControlException, IOException {
		LOG.debug("setVerifyChecksum(): verifyChecksum=" + verifyChecksum);
		fsImpl.setVerifyChecksum(verifyChecksum);
		LOG.debug("setVerifyChecksum(): fsImpl returned successfully");
	}
	
	@Override
	public String getCanonicalServiceName() {
		LOG.debug("getCanonicalServiceName():");
		String name = super.getCanonicalServiceName();
		LOG.debug("getCanonicalServiceName(): super returned name=" + name);
		return name;
	}
	
	@Override
	public int hashCode() {
		LOG.debug("hashCode():");
		int code = super.hashCode();
		LOG.debug("hashCode(): super returned code=" + code);
		return code;
	}

	@Override
	public boolean equals(Object other) {
		LOG.debug("equals(): other=" + other);
		boolean eq = super.equals(other);
		LOG.debug("equals(): super returned equals=" + eq);
		return eq;
	}
	
}
