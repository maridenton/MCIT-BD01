name := "MD4Assignments"

version := "0.1"

scalaVersion := "2.11.8"

val hadoopVersion = "2.7.3"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"

resolvers += "Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"
