name := "sparketl"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "net.jcazevedo" %% "moultingyaml" % "0.4.0"
libraryDependencies += "com.databricks" % "spark-xml_2.11" % "0.4.1"

libraryDependencies += "com.waioeka.sbt" %% "cucumber-runner" % "0.0.8"

libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"
