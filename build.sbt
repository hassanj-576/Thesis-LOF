name := "spark_proj"

version := "1.0"

scalaVersion := "2.10.5"
resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0" 
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0"
libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.11.2" 
libraryDependencies += "com.github.karlhigley" %% "spark-neighbors" % "0.2.2"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.2.0"
libraryDependencies += "com.opencsv" % "opencsv" % "3.9"
mainClass in (Compile, run) := Some("main.scala.mainClass")  

