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
import org.apache.hadoop.mount.MountInterface;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
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

  public MountClient(String host, int port, Configuration conf) {
    super(host, port);

    // By default, we use AUTH_NONE for mount protocol, unless users specify
    // AUTH_SYS in the configuration file.
    if (conf != null) {
      String credentialsFlavor = conf.get("fs.nfs.mount.auth.flavor", null);

      if (credentialsFlavor != null
          && (credentialsFlavor.equalsIgnoreCase("AUTH_SYS") || credentialsFlavor
              .equalsIgnoreCase("AUTH_UNIX"))) {
        CredentialsSys sys = new CredentialsSys();
        sys.setUID(0);
        sys.setGID(0);
        sys.setStamp(new Long(System.currentTimeMillis()).intValue());
        credentials = sys;
      } else {
        credentials = new CredentialsNone();
      }
    } else {
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
        LOG.debug("Mount MNT operation acceptState=" + accepted.getAcceptState());
        mountMNTResponse = new MountMNTResponse(out.asReadOnlyWrap());
        return mountMNTResponse;
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
