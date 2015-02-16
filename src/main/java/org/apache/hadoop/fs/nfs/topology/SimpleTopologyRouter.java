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
package org.apache.hadoop.fs.nfs.topology;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.nfs.NFSv3FileSystem;
import org.apache.hadoop.fs.nfs.NFSv3FileSystemStore;


public class SimpleTopologyRouter extends TopologyRouter {
    
    NFSv3FileSystem fs;
    Namespace space;
    Map<Endpoint,NFSv3FileSystemStore> stores;
    public final static Log LOG = LogFactory.getLog(SimpleTopologyRouter.class);
    
    public SimpleTopologyRouter() {
        stores = new HashMap<>();
    }
    
    @Override
    public synchronized void initialize(NFSv3FileSystem fs, Namespace space, Configuration configuration) throws IOException {
        this.fs = fs;
        this.space = space;
        this.configuration.addResource(configuration);
        if(!verify(space)) {
            throw new IOException("Check namespace to verify configuration");
        }
    }

    private boolean verify(Namespace ns) {

        // Go through the endpoints - they should not be ambiguous
        // Two endpoints cannot cover the same path (unless they are identical)
        // Really stupid configs are possible and this code won't detect that!
        for(Endpoint epi : ns.getEndpoints()) {
            for(Endpoint epj : ns.getEndpoints()) {
                String pi = (epi.getPath() == null) ? "/" : epi.getPath();
                String pj = (epj.getPath() == null) ? "/" : epj.getPath();
                if(pi.startsWith(pj) && !pi.equals(pj)) {
                    LOG.warn("Two endpoints " + epi + " and " + epj + " are overlapping which could lead to confusion. Please fix.");
                    return false;
                }
            }
        }
        
        return true;
    }
    
    @Override
    public synchronized NFSv3FileSystemStore getStore(Path p) throws IOException {        

        if(space == null) {
            throw new IOException("No namespace defined!");
        }
        if(p == null) {
            throw new IOException("Path is null!");
        }
        
        // Choose an endpoint using the path
        Endpoint ep = chooseEndpoint(space, p);
        if(!stores.containsKey(ep)) {
            NFSv3FileSystemStore store = new NFSv3FileSystemStore(fs, space, ep);
            store.initialize();
            stores.put(ep, store);
        }
        return stores.get(ep);
    }
    
    @Override    
    public synchronized Endpoint chooseEndpoint(Namespace space, Path p) throws IOException {
        List<Endpoint> endpoints = space.getEndpoints();
        List<Endpoint> chosen = new LinkedList<>();
        
        if(p == null || !p.isAbsolute()) {
            throw new IOException("Need absolute path for choosing endpoint");
        }
        
        // Multiple endpoints are specified
        if(endpoints != null && endpoints.size() > 0) {
            for(Endpoint ep : endpoints) {
                String epp = ep.getPath();
                if(epp == null || epp.length() == 0 || Path.getPathWithoutSchemeAndAuthority(p).toString().startsWith(epp)) {
                    chosen.add(ep);
                }
            }
            
            // No endpoint matches, so resort to using default one
            if(chosen.isEmpty()) {
                return space.getDefaultEndpoint();
            }
            
            // Pick one of the matching endpoints
            String hostname = InetAddress.getLocalHost().getHostName();
            int id = Math.abs((hostname == null ? 0 : hostname.hashCode())) % chosen.size();
            LOG.debug("Choosing option " + id + " from " + chosen.size() + " options available");
            return chosen.get(id);
        }
        else {
            return null;
        }
    }

    @Override
    public synchronized List<NFSv3FileSystemStore> getAllStores() throws IOException {
        return new LinkedList<>(stores.values());
    }
    
}
