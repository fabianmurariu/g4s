ThisBuild / scalaVersion := "2.12.11"
ThisBuild / organization := "com.github.fabianmurariu"

val grbVersion = "0.1.15"

lazy val commonSettings = Seq(
  scalacOptions += "-Ypartial-unification",
  testFrameworks += new TestFramework("munit.Framework"),
  coverageEnabled := true,
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % "2.2.0-RC3",
    "com.github.mpilquist" %% "simulacrum" % "0.19.0",
    "org.scalameta" %% "munit" % "0.7.11" % Test,
    "org.scalameta" %% "munit-scalacheck" % "0.7.11" % Test
  ),
  Test / parallelExecution := true,
  addCompilerPlugin(
    "org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full
  ),
  resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
)

lazy val g4s = (project in file("."))
  .aggregate(g4sSparse)
  .dependsOn(g4sSparse)
  .settings(
    name := "g4s"
  )

lazy val g4sSparse = (project in file("g4s-sparse"))
  .enablePlugins(MUnitReportPlugin)
  .settings(
    commonSettings,
    name := "g4s-sparse",
    libraryDependencies ++= Seq(
      "com.github.fabianmurariu" % "graphblas-java-native" % grbVersion,
      "com.github.fabianmurariu" % "graphblas-java" % grbVersion % Test classifier "tests"
    )
  )

// lazy val g4sGraph = (project in file("g4s-graph"))
//   .aggregate(g4sSparse)
//   .dependsOn(g4sSparse)
//   .settings(
//     commonSettings,
//     name := "g4s-graph",
//     libraryDependencies ++= Seq(
//       "org.typelevel" %% "cats-free" % "2.1.1",
//       "co.fs2" %% "fs2-core" % "2.2.1"
//     )
//   )

g4sSparse / Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
