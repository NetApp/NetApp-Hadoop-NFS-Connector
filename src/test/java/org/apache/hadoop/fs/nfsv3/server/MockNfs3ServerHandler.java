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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.nfs.mount.MountClient;
import org.apache.hadoop.fs.nfs.mount.MountMNTResponse;
import org.apache.hadoop.mount.MountInterface;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.COMMIT3Request;
import org.apache.hadoop.nfs.nfs3.request.CREATE3Request;
import org.apache.hadoop.nfs.nfs3.request.FSINFO3Request;
import org.apache.hadoop.nfs.nfs3.request.GETATTR3Request;
import org.apache.hadoop.nfs.nfs3.request.LOOKUP3Request;
import org.apache.hadoop.nfs.nfs3.request.MKDIR3Request;
import org.apache.hadoop.nfs.nfs3.request.READ3Request;
import org.apache.hadoop.nfs.nfs3.request.READDIR3Request;
import org.apache.hadoop.nfs.nfs3.request.REMOVE3Request;
import org.apache.hadoop.nfs.nfs3.request.RENAME3Request;
import org.apache.hadoop.nfs.nfs3.request.RMDIR3Request;
import org.apache.hadoop.nfs.nfs3.request.SETATTR3Request;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
import org.apache.hadoop.nfs.nfs3.response.NFS3Response;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.RpcAcceptedReply.AcceptState;
import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.RpcDeniedReply;
import org.apache.hadoop.oncrpc.RpcDeniedReply.RejectState;
import org.apache.hadoop.oncrpc.RpcReply.ReplyState;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.oncrpc.security.Verifier;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.apache.hadoop.oncrpc.security.RpcAuthInfo.AuthFlavor;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockNfs3ServerHandler implements Runnable {

  final Socket socket;
  final MockNfs3Filesystem filesystem;
  final boolean broken;
  
  public final static Logger LOG = LoggerFactory.getLogger(MockNfs3ServerHandler.class);
  
  public MockNfs3ServerHandler(MockNfs3Filesystem filesystem, Socket socket, boolean broken) throws IOException {
    this.socket = socket;
    this.filesystem = filesystem;
    this.broken = broken;
  }

  @Override
  public void run() {
    try {
      
      LOG.info("Nfs3 server handler thread is running");
      
      BufferedOutputStream oos =
          new BufferedOutputStream(socket.getOutputStream());
      BufferedInputStream ois =
          new BufferedInputStream(socket.getInputStream());
      
      while(!socket.isClosed()) {

        byte[] requestBuffer = null;
        byte[] fragmentHeader = new byte[4];
        @SuppressWarnings("unused")
        long totalReadBytes = 0;
        
        // Read an entire RPC request message
        while(true) {
          
          LOG.info("Reading fragment header");
          // The first 4 bytes are the record header
          if (ois.read(fragmentHeader, 0, 4) != 4) {
            throw new IOException("Could not read fragment header");
          }
          LOG.info("Finished reading fragment header");
          
          // The first bit is the flag to tell whether this is the last fragment
          // The left 31 bits are the length of this segment
          int fragmentHeaderInt = ByteBuffer.wrap(fragmentHeader).getInt();
          int lastFragmentFlag = fragmentHeaderInt & (1 << 31);
          int length = fragmentHeaderInt & (~(1 << 31));
  
          // Do multiple reads since a fragment could be larger than the maximal
          // allowed TCP packet
          byte[] fragment = new byte[length];
          int readbytes = 0;
  
          while (readbytes < length) {
            int curReadBytes = ois.read(fragment, readbytes, length - readbytes);
            readbytes += curReadBytes;
            LOG.info("Read " + readbytes + " of a total size of " + length);
          }
  
          if (readbytes != length) {
            throw new IOException("Did not read expected number of bytes readbytes=" + readbytes + " length=" + length);
          }
          totalReadBytes += readbytes;
          
          // Concatenate fragments together
          if (requestBuffer == null) {
            requestBuffer = fragment.clone();
          } else {
            requestBuffer = ArrayUtils.addAll(requestBuffer, fragment);
          }
  
          // Stop if we have reached the last fragment
          if (lastFragmentFlag != 0) {
            break;
          }
        }
        
        // Make the RPC Call object
        XDR callXdr = new XDR(requestBuffer);
        RpcCall rpcCall = RpcCall.read(callXdr);
        LOG.info("Converted incoming bytes to a RPC call object");
        
        // Make XDR for reply
        XDR replyXdr = new XDR();
        
        // Check RPC version
        if(rpcCall.getRpcVersion() != RpcCall.RPC_VERSION) {
          RpcDeniedReply reply = new RpcDeniedReply(rpcCall.getXid(), ReplyState.MSG_DENIED, RejectState.RPC_MISMATCH, new VerifierNone());
          reply.write(replyXdr);
        }
        else if(!(rpcCall.getCredential() instanceof CredentialsNone)) {
          RpcDeniedReply reply = new RpcDeniedReply(rpcCall.getXid(), ReplyState.MSG_DENIED, RejectState.AUTH_ERROR, new VerifierNone());
          reply.write(replyXdr);
        }
        else if(rpcCall.getProgram() != Nfs3Constant.PROGRAM || rpcCall.getVersion() != Nfs3Constant.VERSION) {
          // FIXME Check for portmap
          // Check for MOUNT
          if(rpcCall.getProgram() == MountClient.MOUNTD_PROGRAM && rpcCall.getVersion() == MountClient.MOUNTD_VERSION) {
            String mountPath = callXdr.readString();
            RpcAcceptedReply reply = RpcAcceptedReply.getInstance(rpcCall.getXid(), AcceptState.SUCCESS, Verifier.VERIFIER_NONE);
            reply.write(replyXdr);
            // Mount response
            if(mountPath.equals("/mountpath")) {
              MountMNTResponse mountResponse = mount(rpcCall, callXdr);
              replyXdr.writeInt(mountResponse.getStatus());
              mountResponse.getFilehandle().serialize(replyXdr);
              replyXdr.writeInt(1);
              replyXdr.writeInt(AuthFlavor.AUTH_NONE.getValue());
            } else {
              MountMNTResponse mountResponse = mount(rpcCall, callXdr);
              replyXdr.writeInt(mountResponse.getStatus());
              mountResponse.getFilehandle().serialize(replyXdr);
              replyXdr.writeInt(0);
            }
          } else {
            RpcAcceptedReply reply = RpcAcceptedReply.getInstance(rpcCall.getXid(), AcceptState.PROG_MISMATCH, Verifier.VERIFIER_NONE);
            reply.write(replyXdr);
          }
        }
        else {
          if(!broken) {
            LOG.info("Basics of the RPC checks out, call the RPC program to process the call");
            NFS3Response nfsResponse = handleNFS(rpcCall, callXdr);
            nfsResponse.writeHeaderAndResponse(replyXdr, rpcCall.getXid(), Verifier.VERIFIER_NONE);
          } else {
            RpcAcceptedReply reply = RpcAcceptedReply.getInstance(rpcCall.getXid(), AcceptState.PROC_UNAVAIL, Verifier.VERIFIER_NONE);
            reply.write(replyXdr);
          }
        }
        
        // Send reply to socket
        ChannelBuffer buffer = XDR.writeMessageTcp(replyXdr, true);
        oos.write(buffer.toByteBuffer().array());
        oos.flush();
        LOG.info("Sent reply of " + buffer.toByteBuffer().array().length + " bytes");
      }
      
    } catch(IOException exception) {
      LOG.error("Could not handle request");
      //exception.printStackTrace();
    }
    
  }
  
  private MountMNTResponse mount(RpcCall call, XDR callXdr) throws IOException {
    assert(call.getProgram() == MountClient.MOUNTD_PROGRAM);
    assert(call.getVersion() == MountClient.MOUNTD_VERSION);
    
    if(call.getProcedure() == MountInterface.MNTPROC.MNT.getValue()) {
      //callXdr.readString();
      return filesystem.getRoot();
    } else {
      throw new IOException("Not a valid MOUNT3 procedure");
    }
    
  }
  
  private NFS3Response handleNFS(RpcCall call, XDR callXdr) throws IOException {
    assert(call.getProgram() == Nfs3Constant.PROGRAM);
    assert(call.getVersion() == Nfs3Constant.VERSION);
    
    if(call.getProcedure() == Nfs3Constant.NFSPROC3.CREATE.getValue()) {
      CREATE3Request request = new CREATE3Request(callXdr);
      return filesystem.CREATE(request);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.COMMIT.getValue()) {
      COMMIT3Request request = new COMMIT3Request(callXdr);
      return filesystem.COMMIT(request);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.NULL.getValue()) {
      return new NFS3Response(Nfs3Status.NFS3_OK);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.FSINFO.getValue()) {
      FSINFO3Request request = new FSINFO3Request(callXdr);
      return filesystem.FSINFO(request);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.MKDIR.getValue()) {
      MKDIR3Request request = new MKDIR3Request(callXdr);
      return filesystem.MKDIR(request);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.LOOKUP.getValue()) {
      LOOKUP3Request request = new LOOKUP3Request(callXdr);
      return filesystem.LOOKUP(request);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.GETATTR.getValue()) {
      GETATTR3Request request = new GETATTR3Request(callXdr);
      return filesystem.GETATTR(request);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.READ.getValue()) {
      READ3Request request = new READ3Request(callXdr);
      return filesystem.READ(request);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.READDIR.getValue()) {
      READDIR3Request request = new READDIR3Request(callXdr);
      return filesystem.READDIR(request);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.RENAME.getValue()) {
      RENAME3Request request = new RENAME3Request(callXdr);
      return filesystem.RENAME(request);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.REMOVE.getValue()) {
      REMOVE3Request request = new REMOVE3Request(callXdr);
      return filesystem.REMOVE(request);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.RMDIR.getValue()) {
      RMDIR3Request request = new RMDIR3Request(callXdr);
      return filesystem.RMDIR(request);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.SETATTR.getValue()) {
      SETATTR3Request request = new SETATTR3Request(callXdr);
      return filesystem.SETATTR(request);
    } else if(call.getProcedure() == Nfs3Constant.NFSPROC3.WRITE.getValue()) {
      WRITE3Request request = new WRITE3Request(callXdr);
      return filesystem.WRITE(request);
    } else {
      throw new IOException("Not a valid NFS3 procedure");
    }
    
  }
  
  
}
