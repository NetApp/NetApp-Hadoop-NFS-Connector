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

package org.apache.hadoop.fs.nfs.portmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.nfs.rpc.RpcClient;
import org.apache.hadoop.fs.nfs.rpc.RpcException;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.RpcAcceptedReply.AcceptState;
import org.apache.hadoop.oncrpc.RpcMessage;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.portmap.PortmapMapping;

public class PortmapClient extends RpcClient {
  static final int PROGRAM = 100000;
  static final int VERSION = 2;

  static final int PMAPPROC_NULL = 0;
  static final int PMAPPROC_GETPORT = 3;
  static final int PMAPPROC_DUMP = 4;

  public final static Log LOG = LogFactory.getLog(PortmapClient.class);

  public PortmapClient(String host, int port) throws IOException {
    super(host, port);
  }

  public void nullOp() throws IOException {
    
    try {
      XDR in = new XDR();
      XDR out = new XDR();
      RpcMessage reply;
      reply = service(PROGRAM, VERSION, PMAPPROC_NULL, in, out, new CredentialsNone());
      if(reply instanceof RpcAcceptedReply) {
        RpcAcceptedReply accepted = (RpcAcceptedReply) reply;
        if(accepted.getAcceptState() != AcceptState.SUCCESS) {
          LOG.error("Portmap NULL operation was not accepted acceptState=" + accepted.getAcceptState());
        }
      } else {
        LOG.error("Portmap NULL operation was not accepted");
      }
    } catch(RpcException exception) {
      LOG.error("Portmap NULL operation failed with RpcException " + exception.getMessage());
      throw new IOException(exception.getCause());
    }

  }

  public int getport(int program, int version, int protocol) throws IOException {
    
    try {
      XDR in = new XDR();
      XDR out = new XDR();
      int port = -1;
      RpcMessage reply;
      
      // Construct PORTMAP request
      {
        in.writeInt(program);
        in.writeInt(version);
        in.writeInt(protocol);
        in.writeInt(port);
      }
      
      reply = service(PROGRAM, VERSION, PMAPPROC_GETPORT, in, out, new CredentialsNone());
      if(reply instanceof RpcAcceptedReply) {
        RpcAcceptedReply accepted = (RpcAcceptedReply) reply;
        if(accepted.getAcceptState() != AcceptState.SUCCESS) {
          LOG.error("Portmap GETPORT operation was not accepted acceptState=" + accepted.getAcceptState());
          throw new IOException("Portmap GETPORT operation was not accepted acceptState=" + accepted.getAcceptState());
        } 
        // Parse the response
        else {
          out = out.asReadOnlyWrap();
          port = out.readInt();
          if(port <= 0) {
            LOG.error("PORTMAP says that program=" + program + ", version=" + version + ", protocol="
          + protocol + " is not registered!");
          }
          return port;
        }
      } else {
        LOG.error("Portmap GETPORT operation was not accepted");
        throw new IOException("Portmap GETPORT operation was not accepted");
      }
    } catch(RpcException exception) {
      LOG.error("Portmap GETPORT operation failed with RpcException " + exception.getMessage());
      throw new IOException(exception.getCause());
    }
  }

  public List<PortmapMapping> dump() throws IOException {
    
    try {
      XDR in = new XDR();
      XDR out = new XDR();
      RpcMessage reply;
      
      reply = service(PROGRAM, VERSION, PMAPPROC_DUMP, in, out, new CredentialsNone());
      if(reply instanceof RpcAcceptedReply) {
        RpcAcceptedReply accepted = (RpcAcceptedReply) reply;
        if(accepted.getAcceptState() != AcceptState.SUCCESS) {
          LOG.error("Portmap DUMP operation was not accepted acceptState=" + accepted.getAcceptState());
          throw new IOException("Portmap DUMP operation was not accepted acceptState=" + accepted.getAcceptState());
        } 
        // Parse the response
        else {
          List<PortmapMapping> pmapList = new ArrayList<PortmapMapping>();
          out = out.asReadOnlyWrap();
          while (out.readBoolean() == true) {
            int program = out.readInt();
            int version = out.readInt();
            int protocol = out.readInt();
            int port = out.readInt();
            PortmapMapping portmapmapping =
                new PortmapMapping(program, version, protocol, port);
            pmapList.add(portmapmapping);
          }
          return pmapList;
        }
      } else {
        LOG.error("Portmap GETPORT operation was not accepted");
        throw new IOException("Portmap GETPORT operation was not accepted");
      }
    } catch(RpcException exception) {
      LOG.error("Portmap GETPORT operation failed with RpcException " + exception.getMessage());
      throw new IOException(exception.getCause());
    }
  }
}
