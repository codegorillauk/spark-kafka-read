name := "sparkstreaming"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.1" // 2.4.5 vs 3.0.1

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion exclude("org.spark-project.spark", "unused"),
  "com.github.scopt" %% "scopt" % "4.0.0-RC2"
)



assemblyJarName in assembly := "example.jar"
mainClass in assembly := Some("SimpleApp")