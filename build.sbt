ThisBuild / scalaVersion := "2.12.11"
ThisBuild / organization := "com.github.fabianmurariu"

val scalaTest = "org.scalatest" %% "scalatest" % "3.1.2"
val scalaTestScalaCheck = "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2"
val grbVersion = "0.1.8"

lazy val commonSettings = Seq(
    scalacOptions += "-Ypartial-unification",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "2.1.3",
      scalaTest % Test,
      scalaTestScalaCheck % Test,
      "com.github.mpilquist" %% "simulacrum" % "0.19.0"),

    addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full),
    resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

lazy val g4s = (project in file("."))
  .aggregate(g4sGraph)
  .dependsOn(g4sGraph)
  .settings(
    name := "g4s",
    libraryDependencies += scalaTest % Test
  )

lazy val g4sSparse = (project in file("g4s-sparse"))
  .settings(
    commonSettings,

    name := "g4s-sparse",

    libraryDependencies ++= Seq(
      "com.github.fabianmurariu" % "graphblas-java-native" % grbVersion,
      "com.github.fabianmurariu" % "graphblas-java" % grbVersion % Test classifier "tests"
    )

  )

lazy val g4sGraph = (project in file("g4s-graph"))
  .aggregate(g4sSparse)
  .dependsOn(g4sSparse)
  .settings(
    commonSettings,

    name := "g4s-graph",

    libraryDependencies ++= Seq(
      scalaTest % Test,
      scalaTestScalaCheck % Test,
      "org.typelevel" %% "cats-free" % "2.1.1",
      "io.monix" %% "monix-reactive" % "3.2.1"

    )
  )

// lazy val g4sGremlin = (project in file("g4s-gremlin"))
//   .aggregate(g4sSparse)
//   .dependsOn(g4sSparse)
//   .settings(
//     commonSettings,

//     name := "g4s-gremlin",

//     libraryDependencies ++= Seq(
//       scalaTest % Test,
//       scalaTestScalaCheck % Test,
//       "org.typelevel" %% "cats-effect" % "2.1.3"
//     )
//   )
// g4sSparse / Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.ScalaLibrary
g4sSparse / Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
