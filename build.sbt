ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    organization:= "com.github.majyphi",
    name := "wareflow"
  )

libraryDependencies += "de.vandermeer" % "asciitable" % "0.3.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test
libraryDependencies += "org.duckdb"% "duckdb_jdbc" % "0.7.0" % Test
