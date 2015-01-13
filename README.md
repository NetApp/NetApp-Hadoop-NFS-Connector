NetApp-Hadoop-NFS-Connector
===========================
How to use this connector
-------------------------------------
<ol>
<li>Compile the project</li>
mvn clean package
<li>Copy the jar file to the shared common library directory based on your Hadoop installation. For example, for hadoop-2.4.1:</li>
cp target/hadoop-connector-nfsv3-1.0.jar $HADOOP_HOME/share/hadoop/common/lib/
<li>Add parameters of NFSv3 connector into core-site.xml located in Hadoop configuration directory(e.g. for hadoop-2.4.1: $HADOOPHOME/conf)</li>
```
  <property>
      <name>fs.defaultFS</name>
      <value>nfs://nfsserver:2049</value>
  </property>
  <property>
      <name>fs.nfs.mountdir</name>
      <value>/export/path</value>
  </property>
  <property>
  <name>fs.nfs.impl</name>
      <value>org.apache.hadoop.fs.nfs.NFSv3FileSystem</value>
  </property>
      <property>
      <name>fs.AbstractFileSystem.nfs.impl</name>
      <value>org.apache.hadoop.fs.nfs.NFSv3AbstractFilesystem</value>
  </property>
  <property>
      <name>fs.nfs.auth.flavor</name>
      <value>AUTH_SYS</value>
  </property>
      <property>
      <name>fs.nfs.username</name>
      <value>username</value>
  </property>
  <property>
      <name>fs.nfs.groupname</name>
      <value>usergroup</value>
  </property>
  <property>
      <name>fs.nfs.prefetch</name>
      <value>true</value>
  </property>
```
<li>Start Hadoop. Now the HDFS has been replaced by NFS.</li>
<ol>
