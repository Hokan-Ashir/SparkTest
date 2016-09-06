name := "SparkTest"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
libraryDependencies +=  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "eu.bitwalker" % "UserAgentUtils" % "1.14"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.0" % "provided"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

test in assembly := {}