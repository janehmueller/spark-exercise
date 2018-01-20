name := "spark-exercise"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.1.0",
    "org.apache.spark" %% "spark-sql" % "2.1.0",
    "org.rogach" %% "scallop" % "3.1.0"
)

// mute assembly merge warnings
logLevel in assembly := Level.Error

// set main class in manifest
mainClass in Compile := Option("SparkExercise")
mainClass in assembly := Option("SparkExercise")

// change assembly jar name
assemblyJarName in assembly := "StrelowEhmuellerSpark.jar"
