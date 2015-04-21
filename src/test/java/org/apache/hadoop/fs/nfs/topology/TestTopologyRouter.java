/*
 * Copyright 2015 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.nfs.topology;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.nfs.NFSv3FileSystem;
import static org.junit.Assert.assertEquals;
import org.junit.Test;


public class TestTopologyRouter {
    
    private Mapping getMappingWithSingleNamespace() throws Exception {
        List<Namespace> spaces = new LinkedList<>();
        Namespace space = new Namespace("default", new URI("nfs://somehost:2049/"));
        List<Endpoint> points = new LinkedList<>();
        points.add(new Endpoint(new URI("nfs://host1"), "/vol/volA", "/data01"));
        points.add(new Endpoint(new URI("nfs://host2"), "/vol/volB", "/data02"));
        space.setEndpoints(points);
        spaces.add(space);
        return new Mapping(spaces);
    }
    
    private Mapping getMappingWithSeveralMounts() throws Exception {
        List<Namespace> spaces = new LinkedList<>();
        Namespace space = new Namespace("default", new URI("nfs://somehost:2049/"));
        List<Endpoint> points = new LinkedList<>();
        points.add(new Endpoint(new URI("nfs://host1"), "/vol/volA", "/data01"));
        points.add(new Endpoint(new URI("nfs://host2"), "/vol/volB", "/data02"));
        space.setEndpoints(points);
        spaces.add(space);
        return new Mapping(spaces);
    }
    
    @Test
    public void testDefaultNamespace() throws Exception {
        Mapping mapping = new Mapping();
        Namespace ns = mapping.getNamespace(new URI("nfs://localhost:2049/foo/bar"));
        assertEquals(1,ns.getEndpoints().size());
    }
    
    @Test
    public void testSimpleMatch() throws Exception {
        Mapping mapping = getMappingWithSingleNamespace();
        TopologyRouter router = new SimpleTopologyRouter();
        router.initialize(new NFSv3FileSystem(), mapping.getNamespace(new URI("nfs://somehost:2049/")), new Configuration());
        Endpoint chosen = router.chooseEndpoint(mapping.getNamespace(new URI("nfs://somehost:2049/")), new Path("/data01/X"));
        assertEquals(new URI("nfs://somehost:2049/"), mapping.getNamespace(new URI("nfs://somehost:2049/")).getUri());
        assertEquals(new URI("nfs://host1"),chosen.getUri());
    }
    
    @Test
    public void testSimpleMismatch() throws Exception {
        Mapping mapping = getMappingWithSingleNamespace();
        TopologyRouter router = new SimpleTopologyRouter();
        router.initialize(new NFSv3FileSystem(), mapping.getNamespace(new URI("nfs://somehost:2049/")), new Configuration());
        Namespace ns = mapping.getNamespace(new URI("nfs://blahblah:2049/"));
        assertEquals(new URI("nfs://blahblah:2049/"), ns.getUri());
    }
    
}
