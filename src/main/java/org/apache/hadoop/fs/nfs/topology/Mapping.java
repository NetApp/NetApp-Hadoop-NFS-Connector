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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.Expose;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class Mapping {
    
    @Expose
    List<Namespace> spaces;
    
    final static AtomicInteger counter = new AtomicInteger(1);
    
    public Mapping() {
        spaces = new LinkedList<>();
    }
    
    public Mapping(List<Namespace> spaces) {
        this.spaces = spaces;
    }
    
    public Namespace getNamespace(URI uri) {
        // Find one from the config file
        if(spaces != null && spaces.size() > 0) {
            for(Namespace ns : spaces) {
                if(ns.getUri().getAuthority().equals(uri.getAuthority())) {
                    return ns;
                }
            }
        } 
        // Make a new namespace with defaults
        Namespace space = buildNamespace(uri);
        if(spaces == null) {
            spaces = new LinkedList<>();
        }
        spaces.add(space);
        return space;
    }
    
    public Namespace buildNamespace(URI uri) {
        Namespace space = new Namespace("ns-" + counter.getAndIncrement(), uri);
        List<Endpoint> endpoints = new LinkedList<>();
        endpoints.add(space.getDefaultEndpoint());
        space.setEndpoints(endpoints);
        return space;
    }
    
    public static Mapping loadFromString(String json) throws IOException {
        try {
            Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
            Mapping map = gson.fromJson(json, Mapping.class);
            return map;
        } catch(JsonSyntaxException exception) {
            throw new IOException("Could not parse config string", exception);
        }  
    }
    
    public static Mapping loadFromFile(String filename) throws IOException {
        try {
            Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
            Mapping map = gson.fromJson(new BufferedReader(new FileReader(filename)), Mapping.class);
            return map;
        } catch(JsonSyntaxException exception) {
            throw new IOException("Could not parse config file " + filename, exception);
        } catch(IOException exception) {
            throw new IOException("Could not open namespace config file " + filename, exception);
        }
    }
    
}
