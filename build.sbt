name := "toy-noria"

version := "0.1"

organization := "com.ankurdave"

scalaVersion := "2.12.7"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

javaOptions in Test ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m")

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfuture",
  "-Xlint:_",
  "-Ywarn-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Ywarn-unused-import"
)

scalacOptions in (Compile, console) := Seq.empty
