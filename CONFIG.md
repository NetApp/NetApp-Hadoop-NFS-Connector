NetApp NFS Connector for Hadoop
===============================

Configuration
-------------

To configure, you will need to edit/create two files:
* core-site.xml - Hadoop's configuration file
* nfs-mapping.json - A JSON file describing the NFS server topology

Apache Hadoop
-------------

```
  <!-- If NFS should be the primary/default filesystem -->
  <property>
      <name>fs.defaultFS</name>
      <value>nfs://<nfsserver>:2049</value>
  </property>
  <property>
      <name>fs.nfs.configuration</name>
      <value><path-to-json-configuration-file>/nfs-mapping.json</value>
  </property>
  <property>
  <name>fs.nfs.impl</name>
      <value>org.apache.hadoop.fs.nfs.NFSv3FileSystem</value>
  </property>
      <property>
      <name>fs.AbstractFileSystem.nfs.impl</name>
      <value>org.apache.hadoop.fs.nfs.NFSv3AbstractFilesystem</value>
  </property>
```

NFS Configuration
------------------

The NFS Configuration is provided in a JSON file (nfs-mapping.json). In this file, we define each namespace (space for short) 
that describes the NFS namespace to used. A space contains its settings and a set of endpoints (IP addresses) where the 
NFS server can be accessed from. 

For example, let us assume we have clustered Data ONTAP with a SVM (vs0) hosted on two controllers (node01 and node02).
Each controller node has two IP addresses (node01-ip01 and node01-ip02, node02-ip01 and node02-ip02). 
Let us also assume node01 hosts one volume (vol01 exported as /vol01) and node02 hosts one volume (vol02 exported as /vol2). 
We can also assume that node01 hosts the root volume (exported as /).

In this example, the NFS space would be defined as (we explain the options below):
```
	{
	    "name": "example",
	    "uri": "nfs://node01-ip01:2049/",
	    "options": {
        		"nfsExportPath": "/",
        		"nfsReadSizeBits": 20,
        		"nfsWriteSizeBits": 20,
        		"nfsSplitSizeBits": 28,
        		"nfsAuthScheme": "AUTH_SYS",
        		"nfsUsername": "root",
        		"nfsGroupname": "root",
        		"nfsUid": 0,
        		"nfsGid": 0,
        		"nfsPort": 2049,
        		"nfsMountPort": -1,
        		"nfsRpcbindPort": 111
	    },
	    "endpoints": [
        		{
        		    "host": "nfs://node01-ip01:2049/",
        		    "exportPath":"/vol01",
        		    "path": "/vol01/"
        		},
        		{
        		    "host": "nfs://node01-ip02:2049/",
        		    "exportPath":"/vol01",
        		    "path": "/vol01/"
        		},        		
        		{
        		    "host": "nfs://node02-ip01:2049/",
        		    "exportPath":"/vol02",
        		    "path": "/vol02/"
        		},        		
        		{
        		    "host": "nfs://node02-ip02:2049/",
        		    "exportPath":"/vol02",
        		    "path": "/vol02/"
        		}                
	    ]
	}
```

This configuration maximizes data locality by issuing NFS requests to the controller that hosts the data. 
Clustered Data ONTAP allows data to be accessed by all controllers but going through a different node incurs 
an extra network hop. In the normal case, we can avoid this hop to improve the performance. However, in the case of
upgrade or failure, the IP address can be migrated to other nodes and Hadoop will continue to function without
disruption (albeit with some performance degradation).

The above JSON also defines options for NFS as well.
```
	    "options": {
        		"nfsExportPath": "/",
        		"nfsReadSizeBits": 20,
        		"nfsWriteSizeBits": 20,
        		"nfsSplitSizeBits": 28,
        		"nfsAuthScheme": "AUTH_SYS",
        		"nfsUsername": "root",
        		"nfsGroupname": "root",
        		"nfsUid": 0,
        		"nfsGid": 0,
        		"nfsPort": 2049,
        		"nfsMountPort": -1,
        		"nfsRpcbindPort": 111
	    },
```

You may just use these numbers in most of the cases but you can also tune them for your needs. Here's an overview of each option:

* ```nfsExportPath``` - This is the primary export path. It is assumed to be "/" unless overridden.
* ```nfsReadSizeBits``` - The size of NFS Read requests. It is preferred to be 20 (1048576 bytes) but the connector will negotiate with the NFS server to determine.

* ```nfsWriteSizeBits``` - The size of the NFS Write requests.
* ```nfsSplitSizeBits``` - The size of the input split to be used by Hadoop. In general choose, 28 for a 256MB split, 29 for 512MB, and 30 for 1G split.
* ```nfsAuthScheme``` - The authentication scheme to use: AUTH_NONE or AUTH_SYS
* ```nfsUsername``` - The Hadoop username to use to access NFS
* ```nfsGroupname``` - The Hadoop username to use to access NFS
* ```nfsUid``` - The NFS uid to access NFS
* ```nfsGid``` - The NFS gid to access NFS
* ```nfsPort``` - The port for NFS (default is 2049)
* ```nfsMountPort``` - The port for MOUNT (default is -1 which will then query Portmapper)
* ```nfsRpcbindPort``` - The port for RPCBIND/PORTMAP (which is 111 by default)

