name := "MD3"

version := "0.1"

scalaVersion := "2.13.5"

organization := "ca.mcit.bigdata"

val hadoopVersion = "2.7.3"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
