name := "SparkScalaCourse"

version := "0.1"

scalaVersion := "2.12.3"

lazy val commonSettings = Seq(
  organization := "com.example",
  version := "0.1.0-SNAPSHOT"
)

lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "fat-jar-test"
  ).
  enablePlugins(AssemblyPlugin)

resolvers in Global ++= Seq(
  "Sbt plugins"                   at "https://dl.bintray.com/sbt/sbt-plugin-releases",
  "Maven Central Server"          at "http://repo1.maven.org/maven2",
  "TypeSafe Repository Releases"  at "http://repo.typesafe.com/typesafe/releases/",
  "TypeSafe Repository Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"
)


libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0-preview2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0-preview2"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.12"





