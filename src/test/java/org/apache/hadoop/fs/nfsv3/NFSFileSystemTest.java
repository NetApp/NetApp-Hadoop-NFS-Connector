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

package org.apache.hadoop.fs.nfsv3;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.nfs.NFSv3FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;


public class NFSFileSystemTest {
	
	static final String SERVER_HOSTNAME_DEFAULT = "nfs://atg-f-flamingo-d1.gdl.englab.netapp.com:2049/mambodata";
	static final String MOUNT_PATH_DEFAULT = "/mambodata";

	public final static Logger LOG = LoggerFactory.getLogger(NFSFileSystemTest.class);
	
	//Test paths
	public final static Path[] test_paths_dirs = {new Path("/unit_test/test0/subtest0"), new Path("/unit_test/test1/subtest1"), new Path("/unit_test/test2/subtest2") };
	public final static Path test_file_dir = new Path("/unit_test_files");
	public final static Path test_file_path_1 = new Path("/unit_test_files/testfile1");
	public final static Path test_file_path_2 = new Path("/unit_test_files/testfile_renamed");
	
	private NFSv3FileSystem fs;
	static String ServerHostname;
	static String MountPath;
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	     ServerHostname = System.getProperty("nfs_server");
	     if (ServerHostname == null) {
	    	 ServerHostname = SERVER_HOSTNAME_DEFAULT;
	     }
	     MountPath = System.getProperty("path");
	     if (MountPath == null) {
	    	 MountPath = MOUNT_PATH_DEFAULT;
	     }
	}
	@Before
	public void setUp() throws Exception {
		fs = new NFSv3FileSystem();
		
		URI uri = new URI(ServerHostname);
		Configuration conf = new Configuration();
		conf.setBoolean("mambo.test", false);
	    conf.set("fs.nfs.mountdir", MountPath);		
		
	    fs.initialize(uri, conf);
	    
	    for (Path dir : test_paths_dirs) {
	    	fs.delete(dir, true);
	    }
	    fs.delete(test_file_dir, true);
	    fs.delete(test_file_path_1, true);
	    fs.delete(test_file_path_2, true);
	}
	

	@After
	public void tearDown() throws Exception {
		
		for (Path dir : test_paths_dirs) {
	    	fs.delete(dir, true);
	    }
	    fs.delete(test_file_dir, true);
	    fs.delete(test_file_path_1, true);
	    fs.delete(test_file_path_2, true);
	    
		fs.close();
	}
	
	@Test
	public void testNFSInit() throws Exception{
		Boolean ex_caught = false;
		
		@SuppressWarnings("resource")
    NFSv3FileSystem temp_fs = new NFSv3FileSystem();
		Configuration conf = new Configuration();
		conf.setBoolean("mambo.test", false);
	    conf.set("fs.nfs.mountdir", MountPath);	
		
		//wrong scheme will be taken as nfs
		URI uri = new URI("http://atg-f-flamingo-d1.gdl.englab.netapp.com:2049/mambodata");
		try{
			temp_fs.initialize(uri, conf);
		} catch (Exception e) {
			ex_caught = true;
		}
		assertTrue(ex_caught);
		ex_caught = false;
		
		//null authority will not be taken
		temp_fs = new NFSv3FileSystem();
		uri = new URI(""); 
		try{
			temp_fs.initialize(uri, conf);
		} catch (Exception e) {
			ex_caught = true;
		}
		
		assertTrue(ex_caught);
		ex_caught = false;
		
		//null authority will not be taken
		temp_fs = new NFSv3FileSystem();
		uri = new URI("blahblah"); 
		try{
			temp_fs.initialize(uri, conf);
		} catch (Exception e) {
			ex_caught = true;
		}
				
		assertTrue(ex_caught);
		ex_caught = false;
		
	}
	
	@Test
	public void testFileSystemInfo() {
		  assertNotNull(fs);
		  Path p = fs.getWorkingDirectory();
		  assertTrue(p.isRoot());
		  
		  assertEquals(fs.getNfsBlockSize(), 1048576L); //1M block size
		  assertEquals(fs.getScheme(), NFSv3FileSystem.NFS_URI_SCHEME);
	}

	@Test
	public void testBlockLocation() throws Exception{
		Boolean ex_caught = false;
		
		BlockLocation[] locs = fs.getFileBlockLocations(new Path("/"), 0, 1024);
		assertEquals(locs.length, 1);
		
		try{
			Path temp = null;
			locs = fs.getFileBlockLocations(temp, 0, 1024);
		} catch (Exception e) {
			ex_caught = true;
		}
		
		assertTrue(ex_caught);
	}

	@SuppressWarnings("deprecation")
  @Test
	public void testDirs() throws Exception {
		// making /test0/subtest0, /test1/subtest1 /test2/subtest2"
		FsPermission permission = new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE);
		assertTrue(fs.mkdirs(test_paths_dirs[0], permission));
		
		assertTrue(fs.mkdirs(test_paths_dirs[1], permission));
		//The second time just returns true
		assertTrue(fs.mkdirs(test_paths_dirs[1], permission));
		
		assertTrue(fs.mkdirs(test_paths_dirs[2], permission));
		
		FileStatus[] file_status = fs.listStatus(new Path("/unit_test"));
		
		for (int i = 0; i<3; i++) {
			assertTrue(file_status[i].isDir());
			assertEquals(file_status[i].getPath(), new Path(fs.getWorkingDirectory(), test_paths_dirs[i].getParent()));

		}
		//set working directory to /test0/subtest0
		Path newWorkingDir = new Path(fs.getWorkingDirectory(), test_paths_dirs[0]);
		fs.setWorkingDirectory(test_paths_dirs[0]);
		assertEquals(fs.getWorkingDirectory(), newWorkingDir);
		Path subsub = new Path("subsubtest0");
		assertTrue(fs.mkdirs(subsub, permission));
		file_status = fs.listStatus(test_paths_dirs[0]);
		
		assertEquals(file_status[0].getPath(), new Path(fs.getWorkingDirectory(), new Path("/unit_test/test0/subtest0/subsubtest0")));
		
		//Remove dir
		assertFalse(fs.delete(test_paths_dirs[0].getParent(), false));
		assertTrue(fs.delete(test_paths_dirs[0].getParent(), true));
		
		assertTrue(fs.delete(test_paths_dirs[1].getParent(), true));
		
		assertTrue(fs.delete(test_paths_dirs[2].getParent(), true));
		
		FileStatus[] file_status_after = fs.listStatus(new Path("/unit_test"));
		assertEquals(file_status_after.length, 0);
		
	}
	
	@SuppressWarnings("deprecation")
  @Test
	public void testFiles() throws Exception {
		//create a file "/unit_test_files/testfile1"	
		FsPermission permission = new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE);
		assertTrue(fs.mkdirs(test_file_dir, permission));
		
		String test_content_1 = new String("0123456789");
		
		
		FSDataOutputStream file_o_stream = fs.create(test_file_path_1);
		assertNotNull(file_o_stream);
		FileStatus[] file_status = fs.listStatus(new Path("/unit_test_files"));
		assertEquals(file_status.length, 1);
		assertFalse(file_status[0].isDir());
		assertEquals(file_status[0].getPath(), new Path(fs.getWorkingDirectory(), test_file_path_1));
		
		
		file_o_stream.writeBytes(test_content_1);
		file_o_stream.flush();
		
		
		byte[] test1_buffer = new byte[test_content_1.length()];
		FSDataInputStream file_i_stream = fs.open(test_file_path_1);
		file_i_stream.read(test1_buffer, 0, test1_buffer.length);
		String buffer_str_1 = new String(test1_buffer);
		assertNotNull(buffer_str_1);
		assertNotNull(test_content_1);
		assertTrue(test_content_1.equals(buffer_str_1));
		
		file_i_stream.seek(0);
		for(int i=0; i<test_content_1.length(); i++) {
			assertEquals(file_i_stream.read(), (int)test_content_1.charAt(i));
		}
		assertEquals(file_i_stream.read(), -1);
		assertEquals(file_i_stream.getPos(), (long)test_content_1.length());
		
		
		
		String test_content_2 = new String("9876543210");
		byte[] test_content_byte_2 = new byte[test_content_2.length()];
		test_content_byte_2 = test_content_2.getBytes();
		
		file_o_stream.write(test_content_byte_2);
		file_o_stream.flush();
		file_o_stream.close();
		
		
		byte[] test2_buffer = new byte[test_content_2.length() + test_content_1.length()];
		file_i_stream = fs.open(test_file_path_1);
		file_i_stream.read(test2_buffer, 0, test_content_2.length() + test_content_1.length());
		String buffer_str_2 = new String(test2_buffer);
		assertTrue(buffer_str_2.equals(new String(test_content_1 + test_content_2)));
		
		file_o_stream = fs.append(test_file_path_1);
		assertNotNull(file_o_stream);
		file_o_stream.writeBytes(test_content_1);
		file_o_stream.flush();
		file_o_stream.close();
		
		byte[] test3_buffer = new byte[test_content_2.length() + 2 * test_content_1.length()];
		file_i_stream = fs.open(test_file_path_1);
		file_i_stream.read(test3_buffer, 0, test_content_2.length() + 2 * test_content_1.length());
		String buffer_str_3 = new String(test3_buffer);
		assertTrue(buffer_str_3.equals(new String(test_content_1 + test_content_2 + test_content_1)));
		file_i_stream.seek(test_content_1.length());
		
		Arrays.fill(test2_buffer, (byte)0);
		file_i_stream.read(test2_buffer, 0, test_content_2.length() + test_content_1.length());
		buffer_str_2 = new String(test2_buffer);
		assertTrue(buffer_str_2.equals(new String(test_content_2 + test_content_1)));
		
		
		file_i_stream.close();
		
		try{
			file_o_stream = fs.create(test_file_path_1, false);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//truncate a file
		file_o_stream = fs.create(test_file_path_1, true);
		assertNotNull(file_o_stream);

		file_status = fs.listStatus(new Path("/unit_test_files"));
		assertEquals(file_status.length, 1);
		assertEquals(file_status[0].getLen(), 0L);
		
		file_o_stream.write(65);//"a"
		file_o_stream.close();
		file_i_stream = fs.open(test_file_path_1);
		assertEquals(file_i_stream.read(), 65);
		
		
		//file_o_stream.close();
		
		
		//rename

		assertTrue(fs.rename(test_file_path_1, test_file_path_2));
		
		file_status = fs.listStatus(new Path("/unit_test_files"));
		assertEquals(file_status.length, 1);
		assertEquals(file_status[0].getLen(), 1L);
		assertEquals(file_status[0].getPath(), new Path(fs.getWorkingDirectory(), test_file_path_2));
			
		assertTrue(fs.delete(test_file_path_2, true));
		
		//Test write/read across multiple blocks

		file_o_stream = fs.create(test_file_path_2, true);
		assertNotNull(file_o_stream);

		byte[] test_4m_content = new byte[1024*1024*4];//4M
		for (int i=0; i<1024*1024*4; i++) {
			test_4m_content[i] = (byte)(i%10 + 65);
		}
		
		file_o_stream.write(test_4m_content);
		file_o_stream.flush();
		file_o_stream.close();
		
		file_i_stream = fs.open(test_file_path_2);
		byte[] test_4m_buffer = new byte[test_4m_content.length];
		
		file_i_stream.read(test_4m_buffer, 0, test_4m_content.length);
		String buffer_str_4m = new String(test_4m_buffer);
		for (int i=0; i<1024*1024*4; i++) {
			assertEquals(buffer_str_4m.charAt(i), (char)(i%10 + 65));
		}

		
		
		file_o_stream.close();
		
	}
}
