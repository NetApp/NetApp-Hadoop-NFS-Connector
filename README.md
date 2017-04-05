NetApp-Hadoop-NFS-Connector
===========================

Overview
-------------------------------------

This page is obsolete and please use http://mysupport.netapp.com/tools/info/ECMLP2570720I.html?productID=62124 for the right software and documentation
============================================================================================================================


The Hadoop NFS Connector allows Apache Hadoop (2.2+) and Apache Spark (1.2+) to use a NFSv3 storage server as a storage endpoint. The NFS Connector supports two modes: (1) secondary filesystem - where Hadoop/Spark runs using HDFS as its primary storage and can use NFS as a second storage endpoint, and (2) primary filesystem - where Hadoop/Spark runs entirely on a NFSv3 storage server. 

The code is written in a way such that existing applications do not have to change. All one has to do is to copy the connector jar into the lib/ directory of Hadoop/Spark. Then, modify core-site.xml to provide the necessary details.

**NOTE: The code is in beta. We would love for you to try it out and give us feedback.**

This is the first release and it does the following:
* Connects to a NFSv3 storage server supporting AUTH_NONE or AUTH_SYS authentication method.
* Works with Apache Hadoop (vanilla) 2.2 or newer, Hortonworks HDP 2.2 or newer 
* Supports all operations defined by the Hadoop FileSystem interface.
* Pipelines the READ/WRITE requests to utilize the underlying network (works fine with 1GbE and 10GbE networks)

We are planning to add these in the near future:
* Ability to connect to multiple NFS endpoints (multiple IP addresses). This allows for even more bandwidth.
* Integrate with Hadoop user authentication

How to use
-------------------------------------

Once the NFS connector is configured, you can easily invoke it from the command-line using the Hadoop shell.
```
  console> bin/hadoop fs -ls nfs://<nfs-server-hostname>:2049/ (if using as secondary filesystem)
  console> bin/hadoop fs -ls / (if using as default/primary filesystem)
```

When new jobs are submitted, you can simply provide it as an input or output path or both:
```
  (assuming NFS is used as a secondary filesystem)
  console> bin/hadoop jar <path-to-examples> jar terasort nfs://<nfs-server-hostname>:2049/tera/in /tera/out
  console> bin/hadoop jar <path-to-examples> jar terasort /tera/in nfs://<nfs-server-hostname>:2049/tera/out
  console> bin/hadoop jar <path-to-examples> jar terasort nfs://<nfs-server-hostname>:2049/tera/in nfs://<nfs-server-hostname>:2049/tera/out
```
  

Configuration
-------------------------------------
<ol>
<li>Compile the project
```
console> mvn clean package
```
</li>
<li>Copy the jar file to the shared common library directory based on your Hadoop installation. For example, for hadoop-2.4.1:
```
console> cp target/hadoop-connector-nfsv3-1.0.jar $HADOOP_HOME/share/hadoop/common/lib/
```
</li>
<li>Add parameters of NFSv3 connector into core-site.xml located in Hadoop configuration directory (e.g., for hadoop-2.4.1: $HADOOP_HOME/conf)
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
</li>
<li>Start Hadoop. NFS can now be used inside Hadoop. </li>
<ol>
