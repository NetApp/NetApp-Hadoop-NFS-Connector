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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.nfs.NFSv3FileSystem;
import org.apache.hadoop.fs.nfs.NFSv3FileSystemStore;


public abstract class TopologyRouter {
    
    final Configuration configuration;
    
    public TopologyRouter() {
        this.configuration = new Configuration();
    }
    
    public abstract void initialize(NFSv3FileSystem fs, Namespace space, Configuration configuration) throws IOException;
    
    public abstract Endpoint chooseEndpoint(Namespace space, Path p) throws IOException;
    
    public abstract NFSv3FileSystemStore getStore(Path p) throws IOException;

    public abstract List<NFSv3FileSystemStore> getAllStores() throws IOException;
    
}
