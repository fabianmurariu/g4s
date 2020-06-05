package com.github.fabianmurariu.g4s.graph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import cats.effect.IO
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import cats.free.Free

class GraphDBSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  import GraphStep._
  val create = for {
    michael <- createNode("Person", "Manager")
    jennifer <- createNode("Person", "Employee")
    msft <- createNode("Microsoft")
    food <- createNode("Mexican")
    games <- createNode("Games")
    _ <- createEdge(jennifer, "is_friend", michael)
    _ <- createEdge(jennifer, "likes", food)
    _ <- createEdge(jennifer, "likes", games)
    _ <- createEdge(jennifer, "works_for", msft)
  } yield ()


  def evalQuery[A](gQuery: Free[GraphStep, A]) = {

    val gResource = GraphDB.default[Task]

    gResource.use { g =>

      val a = gQuery.foldMap(GraphStep.interpreter[GrBMatrix](g))

      a.headL
    }
  }

  "GraphDB" should "get all the vertices back with labels" in {
    val actual = evalQuery(
      for {
        _ <- create
        res <- query(vs)
      } yield res
    ).runSyncUnsafe()

    actual shouldBe VerticesRes(
      Vector(
        0L -> Set("Person", "Manager"),
        1L -> Set("Person", "Employee"),
        2L -> Set("Microsoft"),
        3L -> Set("Mexican"),
        4L -> Set("Games")
      )
    )
  }


  it should "get all edges back with types" in {
      val gQuery = for {
        _ <- create
        res <- query(vs.out())
      } yield res

    val actual = evalQuery(gQuery).runSyncUnsafe()

    actual shouldBe EdgesRes(
      Vector(
        (1, "is_friend", 0),
        (1, "works_for", 2),
        (1, "likes", 3),
        (1, "likes", 4)
      )
    )
  }

  it should "expand 1 hop to one edge" in {
      val gQuery = for {
        _ <- create
        res <- query(vs.out("likes"))
      } yield res

    val actual = evalQuery(gQuery).runSyncUnsafe()

    actual shouldBe EdgesRes(
      Vector(
        (1, "likes", 3),
        (1, "likes", 4)
      )
    )
  }

  it should "expand 1 hop to the next labeled vertex" in {
      val gQuery = for {
        _ <- create
        res <- query(vs.out("likes").v("Mexican"))
      } yield res

    val actual = evalQuery(gQuery).runSyncUnsafe()

    actual shouldBe VerticesRes(
      Vector(
        3L -> Set("Mexican")
      )
    )

  }

  it should "expand 1 hope to multiple edge types" in {
      val gQuery = for {
        _ <- create
        res <- query(vs.out("is_friend", "works_for").v()) // is_friend OR works_for
      } yield res

    val actual = evalQuery(gQuery).runSyncUnsafe()

    actual shouldBe VerticesRes(
      Vector(
        0L -> Set("Manager", "Person"),
        2L -> Set("Microsoft")
      )
    )
  }

  it should "expand 1 hop out to all Vertices" in {
      val gQuery = for {
        _ <- create
        res <- query(vs.out("likes").v())
      } yield res

    val actual = evalQuery(gQuery).runSyncUnsafe()

    actual shouldBe VerticesRes(
      Vector(
        3L -> Set("Mexican"),
        4L -> Set("Games")
      )
    )

  }
}
