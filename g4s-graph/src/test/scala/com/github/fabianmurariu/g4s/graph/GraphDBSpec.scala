package com.github.fabianmurariu.g4s.graph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import cats.free.Free
import zio._
import zio.interop.catz._
import com.github.fabianmurariu.g4s.graph.graph.DefaultVertex
import com.github.fabianmurariu.g4s.graph.graph.DefaultEdge

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

  def evalQuery[A](gQuery: Free[GraphStep, A]): A = {
    val gResource = GraphDB.default

    val io = gResource.use { g =>
      val a = gQuery.foldMap(GraphStep.interpreter[GrBMatrix, DefaultVertex, DefaultEdge](g))
        a.compile.toVector
    }.map(_.head)

    Runtime.default.unsafeRun(io)
  }

  "GraphDB" should "get all the vertices back with labels" in {
    val actual = evalQuery(
      for {
        _ <- create
        res <- query(vs)
      } yield res
    )

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

    val actual = evalQuery(gQuery)

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

    val actual = evalQuery(gQuery)

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

    val actual = evalQuery(gQuery)

    actual shouldBe VerticesRes(
      Vector(
        3L -> Set("Mexican")
      )
    )

  }

  it should "expand 1 hope to multiple edge types" in {
    val gQuery = for {
      _ <- create
      res <- query(
        vs.out("is_friend", "works_for").v()
      ) // is_friend OR works_for
    } yield res

    val actual = evalQuery(gQuery)

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

    val actual = evalQuery(gQuery)

    actual shouldBe VerticesRes(
      Vector(
        3L -> Set("Mexican"),
        4L -> Set("Games")
      )
    )

  }

  it should "create nodes,edges in bulk, filter on label" in {

    val gQuery = for {
      _ <- createNodes(Vector("one", "two"), Vector("more","labels"), Vector("more", "two"))
      _ <- createEdges((2, "friend", 0), (1, "friend", 0))
      res <- query(vs("two").out("friend").v("one"))
    } yield res

    val actual = evalQuery(gQuery)

    actual shouldBe VerticesRes(
      Vector(
        0L -> Set("one", "two")
      )
    )
  }
}
