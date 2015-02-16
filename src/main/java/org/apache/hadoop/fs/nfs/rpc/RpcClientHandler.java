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
package org.apache.hadoop.fs.nfs.rpc;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.oncrpc.RpcReply;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.XDR.State;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

public class RpcClientHandler extends IdleStateAwareChannelHandler {

    final RpcClient client;
    final ClientBootstrap bootstrap;
    final Timer timer;

    public static final Log LOG = LogFactory.getLog(RpcClientHandler.class);

    public RpcClientHandler(RpcClient client, ClientBootstrap bootstrap, Timer timer) {
        this.client = client;
        this.bootstrap = bootstrap;
        this.timer = timer;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent event) {
        sendNextMessage(ctx, event);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
        ChannelBuffer buf = (ChannelBuffer) event.getMessage();
        XDR replyxdr = new XDR(buf.toByteBuffer().asReadOnlyBuffer(), State.READING);
        RpcReply rpcreply = RpcReply.read(replyxdr);
        client.completeTask(rpcreply.getXid(), rpcreply, replyxdr);
        sendNextMessage(ctx, event);
    }

    @Override
    public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent event) {
        sendNextMessage(ctx, event);
    }

    @Override
    public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent event) {
        sendNextMessage(ctx, event);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent event) {
        LOG.debug("RPC: channel was disconnected");
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent event) {
        timer.newTimeout(new TimerTask() {
            public void run(Timeout timeout) {
                if (!client.hasShutdown()) {
                    LOG.debug("RPC: channel was closed. Trying to reconnect");
                    client.setChannel(bootstrap.connect());
                }
            }
        }, RpcClient.RECONNECT_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) {
        LOG.error("RPC: Got an exception", event.getCause());
    }

    protected void sendNextMessage(ChannelHandlerContext ctx, ChannelEvent event) {
        RpcNetworkTask task = client.getTask();
        if (task != null) {
            if (event.getChannel().isConnected()) {
                LOG.debug("Send call with xid=" + task.xid);
                event.getChannel().write(task.getCallData());
            }
        }
    }

}
