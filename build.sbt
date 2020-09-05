ThisBuild / scalaVersion := "2.12.11"
ThisBuild / organization := "com.github.fabianmurariu"

val grbVersion = "0.1.15"

lazy val commonSettings = Seq(
  scalacOptions in (Compile, console) --= Seq(
    "-Ywarn-unused:imports",
    "-Xfatal-warnings"
  ),
  scalacOptions ++= Seq(
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-encoding",
    "utf-8", // Specify character encoding used by source files.
    "-explaintypes", // Explain type errors in more detail.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
    "-language:experimental.macros", // Allow macro definition (besides implementation and application)
    "-language:higherKinds", // Allow higher-kinded types
    "-language:implicitConversions", // Allow definition of implicit functions called views
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
    // "-Xfatal-warnings", // Fail the compilation if there are any warnings.
    "-Xfuture", // Turn on future language features.
    "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
    "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
    "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
    "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
    "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
    "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
    "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
    "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
    "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
    "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
    "-Xlint:option-implicit", // Option.apply used implicit view.
    "-Xlint:package-object-classes", // Class or object defined in package object.
    "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
    "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
    "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
    "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
    "-Xlint:unsound-match", // Pattern match may not be typesafe.
    "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
    "-Ypartial-unification", // Enable partial unification in type constructor inference
    "-Ywarn-dead-code", // Warn when dead code is identified.
    "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
    "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
    "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
    "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
    "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
    "-Ywarn-numeric-widen", // Warn when numerics are widened.
    "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
    "-Ywarn-unused:locals", // Warn if a local definition is unused.
    "-Ywarn-unused:params", // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
    "-Ywarn-unused:privates", // Warn if a private member is unused.
    "-Ywarn-value-discard" // Warn when non-Unit expression results are unused.
  ),
  semanticdbEnabled := true, // enable SemanticDB
  semanticdbVersion := scalafixSemanticdb.revision, // use Scalafix compatible version
  testFrameworks += new TestFramework("munit.Framework"),
  coverageEnabled := true,
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % "2.2.0-RC3",
    "com.github.mpilquist" %% "simulacrum" % "0.19.0",
    "co.fs2" %% "fs2-core" % "2.4.4",
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
lazy val docs = project       // new documentation project
  .in(file("g4s-docs")) // important: it must not be docs/
  .dependsOn(g4sSparse)
  .enablePlugins(MdocPlugin)
  .settings(
     mdocVariables := Map(
     "VERSION" -> version.value
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
