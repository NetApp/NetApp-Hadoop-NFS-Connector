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

package org.apache.hadoop.fs.nfs.rpc;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.oncrpc.RpcReply;
import org.apache.hadoop.oncrpc.XDR;
import org.jboss.netty.buffer.ChannelBuffer;

class RpcNetworkTask {
  
  final int xid;
  final CountDownLatch countdown;
  
  ChannelBuffer callData;
  
  RpcReply reply;
  XDR replyData;
  
  public static final Log LOG = LogFactory.getLog(RpcNetworkTask.class);
  
  public RpcNetworkTask(Integer xid, ChannelBuffer callData) {
    this.xid = xid;
    this.callData = callData;
    countdown = new CountDownLatch(1);
  }
  
  public int getXid() {
    return xid;
  }
  
  public ChannelBuffer getCallData() {
    return callData;
  }
  
  public void setReply(RpcReply reply, XDR replyData) {
    this.reply = reply;
    this.replyData = replyData;
  }
  
  public RpcReply getReply() {
    return reply;
  }
  
  public XDR getReplyData() {
    return replyData;
  }
  
  public boolean wait(int millis) {
    while(true) {
      try {
        return countdown.await(millis, TimeUnit.MILLISECONDS);
      } catch(InterruptedException exception) {
        LOG.info("Thread got interrupted while waiting for task xid=" + xid + " to finish");
        continue;
      }
    }
  }
  
  public void signal() {
    countdown.countDown();
  }
  
}
