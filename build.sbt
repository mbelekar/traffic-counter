name := "traffic-counter"

version := "0.1"

scalaVersion := "2.13.11"

val sparkVersion = "3.4.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.16" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0" % "test"
)

Test / run / fork := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M")

