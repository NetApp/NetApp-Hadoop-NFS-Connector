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

package org.apache.hadoop.fs.nfsv3.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MockNfs3Server implements Runnable {

  final int serverPort;
  final ExecutorService threadPool;
  final MockNfs3Filesystem filesystem;
  final ServerSocket serverSocket;
  final boolean broken;
  
  public final static Logger LOG = LoggerFactory.getLogger(MockNfs3Server.class);
  
  public MockNfs3Server(boolean broken, int port) throws IOException {
    threadPool = Executors.newFixedThreadPool(128);
    serverPort = port;
    filesystem = new MockNfs3Filesystem();
    serverSocket = new ServerSocket(serverPort);
    this.broken = broken;
  }

  @Override
  public void run() {
    try {
      int threadId = 1;
      while(!serverSocket.isClosed()) {
        Socket clientSocket = serverSocket.accept();
        LOG.info("Launched NFS handler thread " + threadId);
        threadPool.execute(new MockNfs3ServerHandler(filesystem, clientSocket, broken));
        threadId++;
      }
      threadPool.shutdown();
    } catch(IOException exception) {
      exception.printStackTrace();
      LOG.error("Got an IOException in the accept loop");
    }
  }
  
}
