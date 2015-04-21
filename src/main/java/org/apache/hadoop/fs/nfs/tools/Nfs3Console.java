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

package org.apache.hadoop.fs.nfs.tools;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.nfs.NFSv3FileSystem;

public class Nfs3Console {

  static URI hostname;
  static List<String> endpoints = new ArrayList<String>();
  static String mountPath;
  static List<String> tests;
  
  static NFSv3FileSystem nfs;
  
  public final static Log LOG = LogFactory.getLog(Nfs3Console.class);
  
  public static void main(String args[]) throws Exception {
  
    args = new String[] {"-h", "10.63.150.50", "-p", "2049", "-m", "/hadoopvol"};
    parseCommandLine(args);
    
    // Setup a configuration
    Configuration config = new Configuration();
    config.setBoolean("mambo.test", false);
    config.set("fs.nfs.mountdir", mountPath);
    config.set("fs.nfs.auth.flavor", "AUTH_SYS");
    config.set("fs.nfs.extraendpoints", "" + endpoints.size());
    for(int i = 0; i < endpoints.size(); ++i) {
        config.set("fs.nfs.endpoint." + (i+1), endpoints.get(i).toString());
    }
    
    // Connect to NFS server
    nfs = new NFSv3FileSystem(hostname, config);
    nfs.initialize(hostname, config);
    nfs.listStatus(new Path("/"));
    
    // Run different tests
    checkIntegrity();
    //measureReadBandwidth();
    
    // Shutdown
    nfs.close();
    System.exit(0);
    
  }
  
  private static void parseCommandLine(String args[]) throws Exception {
    final Options options = new Options();
    
    // Get NFS server details
    Option opt;
    
    options.addOption("h", "hostname", true, "NFS server hostname, e.g., nfs://server.com:2049/mountpath");
    options.addOption("p", "port", true, "NFS server port (optional)");
    options.addOption("m", "mount-dir", true, "NFS mount directory, e.g., /mnt/nfs");
    opt = new Option("e", "endpoint", true, "NFS additional endpoints");
    opt.setArgs(64);
    options.addOption(opt);
    
    // Parse the command line
    try {
      final CommandLineParser parser = new GnuParser();
      CommandLine commandLine = parser.parse(options, args);
      
      if(commandLine.hasOption('h')) {
        hostname = new URI(commandLine.getOptionValue('h'));
      }
      if(commandLine.hasOption('m')) {
        mountPath = commandLine.getOptionValue('m', "/");
      }
      if(commandLine.hasOption('e')) {
        String u[] = commandLine.getOptionValues('e');
        if(u != null) {
            for(String s: u) {
                endpoints.add(s);
            }
        }
      }
    } catch(ParseException exception) {
      LOG.error("Could not parse command line options!");
      throw exception;
    }
  }
  
  private static void checkIntegrity() throws Exception {
    
    final int content = 1;
    final long fileSizes[] = {0, 1, 2, 10, 50, 100, 1024, 4095, 4096, 4097, 65535, 65536, 65537, 1048575, 1048576, 1048577};
    
    // Make the files
    for(int i = 0; i < fileSizes.length; ++i) {
      String filename = "checkfile-" + fileSizes[i] + ".dat";
      FSDataOutputStream output = nfs.create(new Path(filename), true);
      long startTime = System.currentTimeMillis(), stopTime;
      for(long j = 0L; j < fileSizes[i]; ++j) {
        output.write(content);
      }
      output.flush();
      output.close();
      stopTime = System.currentTimeMillis();
      LOG.info("[INTEGRITY] FILE WRITE of " + filename + " took " + (stopTime-startTime) + " ms");
    }
    
    // Check the files
    for(int i = 0; i < fileSizes.length; ++i) {
      String filename = "checkfile-" + fileSizes[i] + ".dat";
      FSDataInputStream input = nfs.open(new Path(filename));
      long startTime = System.currentTimeMillis(), stopTime;
      for(long j = 0L; j < fileSizes[i]; ++j) {
        int value = input.read();
        assert(value == content);
      }
      input.close();
      stopTime = System.currentTimeMillis();
      LOG.info("[INTEGRITY] FILE READ of " + filename + " took " + (stopTime-startTime) + " ms");
    }
    
  }
  
  private static void measureReadBandwidth() throws Exception {
    
    final int minBufferSize = 65536;
    final long fileSizes[] = {/*1024L * 1024L, 1024L * 1024L *1024L,*/ 4096L * 1024L *1024L/*, 16384L * 1024L *1024L*/};
    final byte buffer[] = new byte[minBufferSize];
    final Random random = new Random();
    
    // Initialize the buffer
    random.nextBytes(buffer);
    
    // Make the files
    for(int i = 0; i < fileSizes.length; ++i) {
      String filename = "testfile-" + fileSizes[i] + ".dat";
      FSDataOutputStream output = nfs.create(new Path(filename), true);
      long startTime = System.currentTimeMillis(), stopTime;
      
      // Copy byte-by-byte for small files
      if(fileSizes[i] < minBufferSize) {
        for(long j = 0; j < fileSizes[i]; ++j) {
          output.write(0);
        }
        output.close();
        stopTime = System.currentTimeMillis();
        LOG.info("FILE WRITE of " + filename + " took " + (stopTime-startTime) + " ms");
      }
      // Copy one buffer at a time for large file
      else {
        long totalBytesWritten = 0L;
        for(long j = 0; j < fileSizes[i]; j += buffer.length) {
          output.write(buffer);
          totalBytesWritten += buffer.length;
        }
        output.close();
        stopTime = System.currentTimeMillis();
        LOG.info("FILE WRITE of " + filename + " took " + (stopTime-startTime) + " ms");
        LOG.info("FILE WRITE of " + filename + " transferred at " + calculateTransferRateMB(totalBytesWritten, (stopTime-startTime)) + " MB/s");
      }
    }
    
    // Read the files and measure time
    for(int i = 0; i < fileSizes.length; ++i) {
      String filename = "testfile-" + fileSizes[i] + ".dat";
      FSDataInputStream input = nfs.open(new Path(filename));
      long startTime = System.currentTimeMillis(), stopTime;
      
      // Copy byte-by-byte for small files
      if(fileSizes[i] < minBufferSize) {
        byte b;
        while((b = (byte) input.read()) != -1) {
          assert(b == 0);
        }
        input.close();
        stopTime = System.currentTimeMillis();
        LOG.info("FILE READ of " + filename + " took " + (stopTime-startTime) + " ms");
      }
      // Copy one buffer at a time for large file
      else {
        byte inBuf[] = new byte[minBufferSize];
        long totalBytesRead = 0, bytesRead;
        while( (bytesRead = input.read(inBuf)) != -1) {
          for(int j = 0; j < buffer.length; ++j) {
            assert(inBuf[j] == buffer[j]);
          }
          totalBytesRead += bytesRead;
        }
        input.close();
        stopTime = System.currentTimeMillis();
        LOG.info("FILE READ of " + filename + " took " + (stopTime-startTime) + " ms");
        LOG.info("FILE READ of " + filename + " transferred at " + calculateTransferRateMB(totalBytesRead, (stopTime-startTime)) + " MB/s");
      }
    }
    
  }
  
  private static double calculateTransferRateMB(long bytesTransferred, long timeInMilliSeconds) throws ArithmeticException {
    if(timeInMilliSeconds == 0) {
      throw new ArithmeticException("Divide by zero");
    } else {
      return (bytesTransferred/(1024.0*1024.0))/(timeInMilliSeconds/1000.0);
    }
  }
  
}
