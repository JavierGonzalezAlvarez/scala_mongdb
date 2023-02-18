ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"

lazy val root = (project in file("."))
  .settings(
    name := "scala_mongodb"
  )

libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.1.0",
  "org.apache.spark" %% "spark-core" % "3.2.2",
  "org.apache.spark" %% "spark-sql" % "3.2.2",
  "org.apache.spark" %% "spark-mllib" % "3.2.2",
  "org.apache.spark" %% "spark-streaming" % "3.2.2",
  "org.twitter4j" % "twitter4j-core" % "4.1.2",
  "org.twitter4j" % "twitter4j-stream" % "4.0.7"
)