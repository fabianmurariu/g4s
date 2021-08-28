import org.apache.ivy.core.module.descriptor.ExcludeRule
ThisBuild / scalaVersion := "2.13.4"
ThisBuild / organization := "com.github.fabianmurariu"
ThisBuild / version      := "0.1.0-SNAPSHOT"

val grbVersion = s"0.1.22-${sys.props("os.name").toLowerCase()}"
lazy val munitVersion = "0.7.11" 

lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  scalacOptions in (Compile, console) --= Seq(
    "-Ywarn-unused:imports",
    "-Xfatal-warnings"
  ),
  scalacOptions ++= Seq(
      "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-explaintypes", // Explain type errors in more detail.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
  "-language:experimental.macros", // Allow macro definition (besides implementation and application)
  "-language:higherKinds", // Allow higher-kinded types
  "-language:implicitConversions", // Allow definition of implicit functions called views
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
  // "-Xfatal-warnings", // Fail the compilation if there are any warnings.
  "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
  "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
  "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
  "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
  "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
  "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
  "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
  "-Xlint:option-implicit", // Option.apply used implicit view.
  "-Xlint:package-object-classes", // Class or object defined in package object.
  "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
  "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
  "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
  "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
  "-Ywarn-dead-code", // Warn when dead code is identified.
  "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
  "-Ywarn-numeric-widen", // Warn when numerics are widened.
  "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
  "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
  "-Ywarn-unused:locals", // Warn if a local definition is unused.
  "-Ywarn-unused:params", // Warn if a value parameter is unused.
  "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
  "-Ywarn-unused:privates", // Warn if a private member is unused.
  "-Ywarn-value-discard", // Warn when non-Unit expression results are unused.
  "-Ybackend-parallelism", "8", // Enable paralellisation — change to desired number!
  "-Ymacro-annotations",
  "-Ycache-plugin-class-loader:last-modified", // Enables caching of classloaders for compiler plugins
  "-Ycache-macro-class-loader:last-modified", // and macro definitions. This can lead to performance improvements.
  ),
  semanticdbEnabled := true, // enable SemanticDB
  semanticdbVersion := scalafixSemanticdb.revision, // use Scalafix compatible version
  testFrameworks += new TestFramework("munit.Framework"),
  coverageEnabled := true,
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % "2.3.1",
    "org.typelevel" %% "alleycats-core" % "2.3.1", 
    "com.github.mpilquist" %% "simulacrum" % "0.19.0",
    "org.scalameta" %% "munit" % munitVersion % Test
  ),
  Test / parallelExecution := false,
  Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.AllLibraryJars,
  resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
)

lazy val g4s = (project in file("."))
  .aggregate(g4sSparse, g4sOptim, g4sMatrixGraph)
  .settings(
    name := "g4s"
  )

lazy val g4sSparse = (project in file("g4s-sparse"))
  .enablePlugins(MUnitReportPlugin)
  .settings(
    commonSettings,
    name := "g4s-sparse",
    libraryDependencies ++= Seq(
      "com.github.fabianmurariu" % "graphblas-package" % grbVersion,
      "com.github.fabianmurariu" % "graphblas-java" % grbVersion classifier "tests",
    )
  )
// lazy val docs = project // new documentation project
//   .in(file("g4s-docs")) // important: it must not be docs/
//   .dependsOn(g4sSparse)
//   .enablePlugins(MdocPlugin)
//   .settings(
//     moduleName := "g4s-docs",
//     // mdocOut := baseDirectory.value / ".." /"g4s-docs-site" / "docs" / "tutorial-basics",
//     mdocVariables := Map(
//       "VERSION" -> version.value
//     )
//   )

lazy val g4sOptim = (project in file("g4s-optimizer"))
  .enablePlugins(MUnitReportPlugin)
  .dependsOn(g4sSparse % "test->test;compile->compile")
  .settings(
    commonSettings,
    name := "g4s-optimizer",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1",
      "org.opencypher" % "front-end-9.0" % "9.0-SNAPSHOT" excludeAll(
        ExclusionRule(organization = "org.scala-lang"),
        ExclusionRule(organization = "org.scalacheck", name= "scalacheck_2.12")
        ),
      "com.lihaoyi" %% "pprint" % "0.6.6",
      "co.fs2" %% "fs2-core" % "2.4.4" // FIXME: break BlockingMatrix into some support package
    )
  )

lazy val g4sMatrixGraph = (project in file("g4s-matrix-graph"))
  .enablePlugins(MUnitReportPlugin)
  .dependsOn(g4sSparse % "test->test;compile->compile", g4sOptim)
  .settings(
    commonSettings,
    name := "g4s-matrix-graph",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1",
      "co.fs2" %% "fs2-core" % "2.4.4",
    )
  )

