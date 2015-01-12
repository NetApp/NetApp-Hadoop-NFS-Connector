/**
 * Copyright 2014 NetApp Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.nfs.rpc;


import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.RpcMessage;
import org.apache.hadoop.oncrpc.RpcReply;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Credentials;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

public class RpcClient {

  final ClientBootstrap bootstrap;
  final Map<Integer, RpcNetworkTask> tasks;
  final AtomicBoolean errored;
  final AtomicBoolean shutdown;
  final AtomicInteger xid;

  ChannelFuture future;

  public static final int RECONNECT_DELAY_MS = 5000;
  public static final int MAX_RETRIES = 10;
  public static final int MAX_RPCWAIT_MS = 10000;

  public static final Log LOG = LogFactory.getLog(RpcClient.class);

  public RpcClient(String hostname, int port) {

    tasks = new ConcurrentHashMap<Integer, RpcNetworkTask>();
    xid = new AtomicInteger(new Random(System.currentTimeMillis()).nextInt(1024) * 1000000);
    errored = new AtomicBoolean(false);
    shutdown = new AtomicBoolean(false);

    final Timer timer = new HashedWheelTimer();
    ChannelFactory factory =
        new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(), 1, 1);

    final RpcClient client = this;
    ChannelPipelineFactory pipelineFactory = new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() {
        return Channels.pipeline(new RpcFrameDecoder(), new IdleStateHandler(timer, 0, 1, 0,
            TimeUnit.MICROSECONDS), new RpcClientHandler(client, bootstrap, timer));
      }
    };
    bootstrap = new ClientBootstrap(factory);
    bootstrap.setPipelineFactory(pipelineFactory);

    bootstrap.setOption("remoteAddress", new InetSocketAddress(hostname, port));
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", true);
    bootstrap.setOption("soLinger", 0);
    bootstrap.setOption("receiveBufferSize", 16 * 1024 * 1024);
    bootstrap.setOption("sendBufferSize", 16 * 1024 * 1024);

    future = bootstrap.connect();

  }

  public RpcMessage service(int program, int version, int procedure, XDR in, XDR out,
      Credentials credentials) throws RpcException {
    int callXid = xid.incrementAndGet();

    // Package call into a new task
    XDR request = new XDR();
    RpcCall call =
        RpcCall.getInstance(callXid, program, version, procedure, credentials, new VerifierNone());
    call.write(request);
    request.writeFixedOpaque(in.getBytes());
    ChannelBuffer buf = XDR.writeMessageTcp(request, true);
    RpcNetworkTask task = new RpcNetworkTask(callXid, buf);

    // Issue it and signal
    synchronized (tasks) {
      tasks.put(callXid, task);
    }
    task.signal();

    // Wait for task to complete
    for (int i = 0; i < MAX_RETRIES; ++i) {
      if (task.wait(MAX_RPCWAIT_MS)) {
        if (i > 0) {
          LOG.debug("RPC: Call xid=" + task.getXid() + " completed with " + i + " retries");
          // LOG.debug("exiting abruptly!!!!");
          // System.exit(-1);
        }
        break;
      }
    }

    // Process reply and return
    RpcReply reply = task.getReply();
    if (reply.getState() == RpcReply.ReplyState.MSG_DENIED) {
      LOG.error("RPC: xid=" + callXid + " RpcReply request denied: " + reply);
      throw new RpcException("RPC: xid=" + callXid + " RpcReply request denied: " + reply);
    }

    RpcAcceptedReply acceptedReply = (RpcAcceptedReply) reply;
    LOG.debug("RPC: xid=" + callXid + " completed successfully with acceptstate="
        + acceptedReply.getAcceptState());
    out.writeFixedOpaque(task.getReplyData().getBytes());

    return acceptedReply;

  }

  public void shutdown() {
    LOG.info("Shutting down");
    try {
      shutdown.set(true);
      future.getChannel().close();
      future.getChannel().getCloseFuture().awaitUninterruptibly();
      bootstrap.shutdown();
    } finally {
      bootstrap.releaseExternalResources();
    }
  }

  public boolean hasShutdown() {
    return shutdown.get();
  }

  protected synchronized void setChannel(ChannelFuture future) {
    this.future = future;
  }

  protected RpcNetworkTask getTask() {
    RpcNetworkTask task = null;
    synchronized (tasks) {
      for (RpcNetworkTask t : tasks.values()) {
        if ((System.currentTimeMillis() - t.getLastEnqueuedTime()) > 10000) {
          task = t;
          task.setEnqueueTime(System.currentTimeMillis());
          break;
        }
      }
    }
    return task;
  }

  protected void completeTask(int xid, RpcReply reply, XDR replyData) {
    synchronized (tasks) {
      if (tasks.containsKey(xid)) {
        RpcNetworkTask found = tasks.remove(xid);
        found.setReply(reply, replyData);
        found.signal();
        LOG.debug("RPC: Call finished for xid=" + xid);
      } else {
        LOG.error("RPC: Could not find original call for xid=" + xid);
        errored.set(true);
      }
    }
  }

}
