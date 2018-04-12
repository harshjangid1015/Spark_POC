name := "Spark_POC"

version := "0.1"

//scalaVersion := "2.12.5"


scalaVersion := "2.11.12"

val sparkVersion = "2.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  //"org.apache.spark" % "spark-streaming-twitter_2.11" % "1.5.2"
  "org.apache.bahir" % "spark-streaming-twitter_2.11" % "2.0.1"
)