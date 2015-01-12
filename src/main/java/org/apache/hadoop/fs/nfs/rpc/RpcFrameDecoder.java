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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.oncrpc.XDR;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

/**
 * This code is copied from Hadoop NFS project.
 * @author gokuls
 *
 */
public class RpcFrameDecoder extends FrameDecoder {
  
  public static final Log LOG = LogFactory.getLog(RpcFrameDecoder.class);
  private ChannelBuffer currentFrame;

  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) {

    if (buf.readableBytes() < 4) {
      return null;
    }

    buf.markReaderIndex();

    byte[] fragmentHeader = new byte[4];
    buf.readBytes(fragmentHeader);
    int length = XDR.fragmentSize(fragmentHeader);
    boolean isLast = XDR.isLastFragment(fragmentHeader);

    if (buf.readableBytes() < length) {
      buf.resetReaderIndex();
      return null;
    }

    ChannelBuffer newFragment = buf.readSlice(length);
    if (currentFrame == null) {
      currentFrame = newFragment;
    } else {
      currentFrame = ChannelBuffers.wrappedBuffer(currentFrame, newFragment);
    }

    if (isLast) {
      ChannelBuffer completeFrame = currentFrame;
      currentFrame = null;
      return completeFrame;
    } else {
      return null;
    }
  }
}
