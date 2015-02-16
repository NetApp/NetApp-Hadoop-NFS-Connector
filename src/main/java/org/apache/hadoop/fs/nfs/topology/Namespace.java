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
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;


public class Namespace {
    
    @Expose
    final String name;
    @Expose
    final URI uri;
    @Expose
    final NamespaceOptions options;
    @Expose
    final List<Endpoint> endpoints;
    
    public Namespace(String name, URI uri) {
        this(name, uri, null);
    }
    
    public Namespace(String name, URI uri, NamespaceOptions options) {
        this.name = name;
        this.uri = uri;
        this.endpoints = new LinkedList<>();
        this.options = (options == null) ? new NamespaceOptions() : options;
    }
    
    public void setEndpoints(List<Endpoint> points) {
        endpoints.clear();
        endpoints.addAll(points);
    }
    
    public String getName() {
        return name;
    }
    
    public URI getUri() {
        return uri;
    }
    
    public NamespaceOptions getConfiguration() {
        return options;
    }
    
    public List<Endpoint> getEndpoints() {
        return endpoints;
    }
    
    public Endpoint getDefaultEndpoint() {
        return new Endpoint(uri, options.getNfsExportPath(), "/");
    }
    
    @Override
    public int hashCode() {
        int hash = 5;
        hash = 29 * hash + Objects.hashCode(this.name);
        hash = 29 * hash + Objects.hashCode(this.uri);
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
        final Namespace other = (Namespace) obj;
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.uri, other.uri)) {
            return false;
        }
        return true;
    }
    
}
