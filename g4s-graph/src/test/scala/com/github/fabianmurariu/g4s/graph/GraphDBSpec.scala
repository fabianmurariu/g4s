package com.github.fabianmurariu.g4s.graph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grb.GrBVector
import cats.free.Free
import zio._
import zio.interop.catz._
import com.github.fabianmurariu.g4s.graph.graph.DefaultVertex
import com.github.fabianmurariu.g4s.graph.graph.DefaultEdge
import cats.data.State

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
    dancing <- createNode("Dancing")
    _ <- createEdge(michael, "works_for", msft)
    _ <- createEdge(michael, "likes", dancing)
    _ <- createEdge(michael, "likes", games)
    _ <- createEdge(jennifer, "is_friend", michael)
    _ <- createEdge(jennifer, "likes", food)
    _ <- createEdge(jennifer, "likes", games)
    _ <- createEdge(jennifer, "works_for", msft)
  } yield ()

  def evalQuery[A](gQuery: Free[GraphStep, A]): A = {
    val gResource = GraphDB.default

    val io = gResource
      .use { g =>
        val a = gQuery.foldMap(
          GraphStep.interpreter[GrBMatrix, GrBVector](g)
        )
        a.compile.toVector
      }
      .map(_.head)

    Runtime.default.unsafeRun(io)
  }

  "GraphDB" should "get all the vertices back with labels" in {
    val gQuery =      for {
        _ <- create
        res <- query(vs)
      } yield res


    val VerticesRes(actual) = evalQuery(gQuery)

    actual should contain theSameElementsAs (
      Vector(
        0L -> Set("Person", "Manager"),
        1L -> Set("Person", "Employee"),
        2L -> Set("Microsoft"),
        3L -> Set("Mexican"),
        4L -> Set("Games"),
        5L -> Set("Dancing")
      )
    )
  }

  it should "get all edges back with types" in {
    val gQuery = for {
      _ <- create
      res <- query(vs.out())
    } yield res

    val EdgesRes(actual) = evalQuery(gQuery)

    actual should contain theSameElementsAs(
      Vector(
        (0, "likes", 5),
        (0, "likes", 4),
        (1, "is_friend", 0),
        (1, "works_for", 2),
        (0, "works_for", 2),
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

    val EdgesRes(actual) = evalQuery(gQuery)

    actual should contain theSameElementsAs (
      Vector(
        (1, "likes", 3),
        (1, "likes", 4),
        (0, "likes", 5),
        (0, "likes", 4)
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

  it should "expand 1 hop to multiple edge types" in {
    val gQuery = for {
      _ <- create
      res <- query(
        vs.out("is_friend", "works_for").v()
      ) // is_friend OR works_for
    } yield res

    val actual = evalQuery(gQuery)

    // possible FIXME: test this in neo4j and check if 2 shows up twice
    actual shouldBe VerticesRes(
      Vector(
        2L -> Set("Microsoft"),
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

    val VerticesRes(actual) = evalQuery(gQuery)

    // possible FIXME: test this in neo4j and check if Games shows up twice
    actual should contain theSameElementsAs(
      Vector(
        3L -> Set("Mexican"),
        4L -> Set("Games"),
        4L -> Set("Games"),
        5L -> Set("Dancing")
      )
    )

  }

  it should "create nodes,edges in bulk, filter on label" in {

    val gQuery = for {
      _ <- createNodes(
        Vector("one", "two"),
        Vector("more", "labels"),
        Vector("more", "two")
      )
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


  it should "return the all the paths from a query" ignore {
    //FIXME: this requires a redesign
    val gQuery = for {
      _ <- create
      res <- path(vs.out("likes").v("Games"))
    } yield res

    val actual = evalQuery(gQuery)

    actual shouldBe PathRes(
      Vector(
        Path(1, (1, 4)),
        Path(0, (1, 3))
      )
    ) // aren't paths just vectors of edges?
  }

}
