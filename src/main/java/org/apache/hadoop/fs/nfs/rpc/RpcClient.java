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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
  final Queue<RpcNetworkTask> pending;
  final AtomicBoolean errored;
  final AtomicBoolean shutdown;
  final AtomicInteger xid;
  final RpcClient client;

  ChannelFuture future;

  public static final int RECONNECT_DELAY_MS = 5;
  public static final int MAX_RETRIES = 10;
  public static final int MAX_RPCWAIT_MS = 10000;

  public static final Timer timer = new HashedWheelTimer();

  public static final Log LOG = LogFactory.getLog(RpcClient.class);

  public RpcClient(String hostname, int port) throws IOException {

    tasks = new ConcurrentHashMap<>();
    pending = new ConcurrentLinkedQueue<>();
    xid = new AtomicInteger(new Random(System.currentTimeMillis()).nextInt(1024) * 1000000);
    errored = new AtomicBoolean(false);
    shutdown = new AtomicBoolean(false);

    ChannelFactory factory =
        new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(), 1, 8);

    client = this;
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
    bootstrap.setOption("keepAlive", false);
    bootstrap.setOption("soLinger", 0);
    bootstrap.setOption("receiveBufferSize", 32 * 1024 * 1024);
    bootstrap.setOption("sendBufferSize", 32 * 1024 * 1024);

    future = bootstrap.connect();
    future.awaitUninterruptibly();
    if(future.isDone() && (future.isCancelled() || !future.isSuccess())) {
        throw new IOException("Could not connect to " + hostname + " on port " + port);
    }
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

    // Issue the task
    tasks.put(callXid, task);
    pending.add(task);
    sendToChannel();

    // Wait for task to complete
    boolean completed = false;
    for (int i = 0; i < MAX_RETRIES; ++i) {
      if (task.wait(MAX_RPCWAIT_MS)) {
        completed = true;
        break;
      } else {
        LOG.info("RPC: xid=" + callXid + " took too long, so retrying");
        task = new RpcNetworkTask(callXid, buf);
        tasks.put(callXid, task);
        pending.add(task);
        sendToChannel();
      }
    }

    if (!completed || task.getReply() == null) {
      LOG.error("RPC: xid=" + callXid + " timed out");
      throw new RpcException("RPC: xid=" + callXid + " timed out");
    }

    // Process reply and return
    RpcReply reply = task.getReply();
    if (reply.getState() == RpcReply.ReplyState.MSG_DENIED) {
      LOG.error("RPC: xid=" + callXid + " RpcReply request denied: " + reply);
      throw new RpcException("RPC: xid=" + callXid + " RpcReply request denied: " + reply);
    }

    // Call was accepted so process the correct reply
    RpcAcceptedReply acceptedReply = (RpcAcceptedReply) reply;
    LOG.debug("RPC: xid=" + callXid + " completed successfully with acceptstate="
        + acceptedReply.getAcceptState());
    out.writeFixedOpaque(task.getReplyData().getBytes());

    return acceptedReply;

  }

  public void shutdown() {
    long start = System.currentTimeMillis();
    try {
      shutdown.set(true);
      future.getChannel().close();
      future.getChannel().getCloseFuture().awaitUninterruptibly();
      bootstrap.shutdown();
    } finally {
      bootstrap.releaseExternalResources();
      LOG.debug("RpcClient shutdown took " + (System.currentTimeMillis() - start) + " ms");
    }
  }

  public boolean hasShutdown() {
    return shutdown.get();
  }

  protected synchronized void setChannel(ChannelFuture future) {
    this.future = future;
  }

  protected RpcNetworkTask getTask() {
    return pending.poll();
  }

  protected void completeTask(int xid, RpcReply reply, XDR replyData) {
    RpcNetworkTask found = tasks.remove(xid);
    if (found != null) {
      found.setReply(reply, replyData);
      found.signal();
    }
  }

  protected void sendToChannel() {
    try {
      RpcNetworkTask task = getTask();
      if (task != null) {
        future.getChannel().write(task.getCallData());
      }
    } catch (Exception ignore) {

    }
  }

}
