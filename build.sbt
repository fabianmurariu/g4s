ThisBuild / scalaVersion := "2.12.11"
ThisBuild / organization := "com.github.fabianmurariu"

val scalaTest = "org.scalatest" %% "scalatest" % "3.1.2"
val scalaTestScalaCheck = "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2"
val grbVersion = "0.1.4"

lazy val g4s = (project in file("."))
  .aggregate(g4sSparse)
  .dependsOn(g4sSparse)
  .settings(
    name := "g4s",
    libraryDependencies += scalaTest % Test
  )

lazy val g4sSparse = (project in file("g4s-sparse"))
  .settings(
    name := "g4s-sparse",

    libraryDependencies ++= Seq(
      scalaTest % Test,
      scalaTestScalaCheck % Test,
      "com.github.fabianmurariu" % "graphblas-java-native" % grbVersion,
      "com.github.fabianmurariu" % "graphblas-java" % grbVersion % Test classifier "tests",
      "com.github.mpilquist" %% "simulacrum" % "0.19.0"),

    addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full),
    resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",


  )

// g4sSparse / Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.ScalaLibrary
g4sSparse / Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
