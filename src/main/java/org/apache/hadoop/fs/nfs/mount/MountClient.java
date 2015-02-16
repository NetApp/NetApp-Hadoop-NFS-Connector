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

package org.apache.hadoop.fs.nfs.mount;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.nfs.rpc.RpcClient;
import org.apache.hadoop.fs.nfs.rpc.RpcException;
import org.apache.hadoop.fs.nfs.topology.Namespace;
import org.apache.hadoop.fs.nfs.topology.NamespaceOptions;
import org.apache.hadoop.mount.MountInterface;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.RpcAcceptedReply.AcceptState;
import org.apache.hadoop.oncrpc.RpcMessage;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Credentials;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.oncrpc.security.CredentialsSys;

public class MountClient extends RpcClient {

  public static final int MOUNTD_PROGRAM = 100005;
  public static final int MOUNTD_VERSION = 3;

  public final static Log LOG = LogFactory.getLog(MountClient.class);
  private final Credentials credentials;

  public MountClient(Namespace space, String host, int port) throws IOException {
    super(host, port);

    // Check namespace for authentication scheme
    if(space == null || space.getConfiguration() == null) {
        throw new IOException("No namespace given");
    }
    
    NamespaceOptions options = space.getConfiguration();
    String authScheme = options.getNfsAuthScheme();
    
    if(authScheme != null && (authScheme.equalsIgnoreCase("AUTH_SYS") || authScheme.equalsIgnoreCase("AUTH_UNIX"))) {
        CredentialsSys sys = new CredentialsSys();
        sys.setUID(options.getNfsUid());
        sys.setGID(options.getNfsGid());
        sys.setStamp(new Long(System.currentTimeMillis()).intValue());
        credentials = sys;
    }
    // Use AUTH_NONE by default
    else {
        credentials = new CredentialsNone();
    }  
  }

  public MountMNTResponse mnt(String path) throws IOException {
    try {
      XDR in = new XDR();
      XDR out = new XDR();
      RpcMessage reply;
      MountMNTResponse mountMNTResponse;

      // Construct MOUNT request
      in.writeString(path);
      
      // Call MOUNT
      reply =
          service(MOUNTD_PROGRAM, MOUNTD_VERSION,
              MountInterface.MNTPROC.MNT.getValue(), in, out,
              credentials);

      if (reply instanceof RpcAcceptedReply) {
        RpcAcceptedReply accepted = (RpcAcceptedReply) reply;
        if(accepted.getAcceptState().equals(AcceptState.SUCCESS)) {
            LOG.debug("Mount MNT operation acceptState=" + accepted.getAcceptState());
            mountMNTResponse = new MountMNTResponse(out.asReadOnlyWrap());
            return mountMNTResponse;
        } else {
            LOG.error("Could not mount filesystem");
            throw new IOException("Could not mount filesystem. Got status " + accepted.getAcceptState());
        }
      } else {
        LOG.error("Mount MNT operation was not accepted");
        throw new IOException("Mount MNT operation was not accepted");
      }
    } catch (RpcException exception) {
      LOG.error("Mount MNT operation failed with RpcException "
          + exception.getMessage());
      throw new IOException(exception.getCause());
    }
  }
}
