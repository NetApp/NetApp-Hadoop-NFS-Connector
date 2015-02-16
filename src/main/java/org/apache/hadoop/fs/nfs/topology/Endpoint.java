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

import com.google.gson.annotations.Expose;
import java.net.URI;
import java.util.Objects;


public class Endpoint {
    
    @Expose
    final URI host;
    @Expose
    final String exportPath;
    @Expose
    final String path;
    
    public Endpoint(URI host, String export, String path) {
        this.host = host;
        this.exportPath = export;
        this.path = path;
    }
    
    public URI getUri() {
        return host;
    }
    
    public String getExportPath() {
        return exportPath;
    }
    
    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "Endpoint: host=" + host + " export=" + exportPath + " path=" + path;
    }
    
    @Override
    public int hashCode() {
        int hash = 3;
        hash = 97 * hash + Objects.hashCode(this.host);
        hash = 97 * hash + Objects.hashCode(this.exportPath);
        hash = 97 * hash + Objects.hashCode(this.path);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Endpoint other = (Endpoint) obj;
        if (!Objects.equals(this.host, other.host)) {
            return false;
        }
        if (!Objects.equals(this.exportPath, other.exportPath)) {
            return false;
        }
        if (!Objects.equals(this.path, other.path)) {
            return false;
        }
        return true;
    }
    
}
