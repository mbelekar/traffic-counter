name := "traffic-counter"

version := "0.1"

scalaVersion := "2.13.11"

val sparkVersion = "3.4.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.2.16" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0" % "test"
)

Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated

Test / run / fork := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M")

