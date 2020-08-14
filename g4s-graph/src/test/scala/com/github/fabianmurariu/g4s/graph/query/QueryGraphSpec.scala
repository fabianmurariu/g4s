package com.github.fabianmurariu.g4s.graph.query

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.github.fabianmurariu.g4s.graph.core.Graph._
import com.github.fabianmurariu.g4s.graph.NodesMat
import com.github.fabianmurariu.g4s.graph.EdgeMat
import com.github.fabianmurariu.g4s.graph.MatMul

class QueryGraphSpec extends AnyFlatSpec with Matchers {

  import QueryGraph.Dsl._

  "QueryGraph" should "have a nice DSL to allow for comprehensions (a)-[:friends] ->(b)" in {
    val q = for {
      x <- vs("a")
      y <- vs("b")
      e <- edge(x, y, "friends")
    } yield y

    val qg = q.runS(QueryGraph.empty).value

    val x = QueryNode(0, Set("a"))
    val y = QueryNode(1, Set("b"))

    qg shouldBe QueryGraph(
      Map(
        x -> Map(y -> QueryEdge(0, Some(1), Set("friends"))),
        y -> Map.empty
      ),
      2
    )

  }

  it should "represent a sparql graph, no predicate on vertices" in {

    val q = for {
      x <- vs()
      y <- vs()
      z <- vs()
      w <- vs()
      _ <- edge(x, y, "a")
      _ <- edge(x, w, "b")
      _ <- edge(y, z, "c")
    } yield y

    val qg = q.runS(QueryGraph.empty).value

    val x = QueryNode(0)
    val y = QueryNode(1)
    val z = QueryNode(2)
    val w = QueryNode(3)

    qg.graph.vertices shouldBe Set(x, y, z, w)
    qg.graph.edges.toSet shouldBe Set(
      QueryEdge(0, Some(1), Set("a")),
      QueryEdge(0, Some(3), Set("b")),
      QueryEdge(1, Some(2), Set("c"))
    )

    qg.id shouldBe 4

  }

  it should "interpret a single edge as right multiply" in {

    val q = for {
      x <- vs()
      y <- vs()
      _ <- edge(x, y, "a")
    } yield ()

    val qg = q.runS(QueryGraph.empty).value

    QueryGraph.evalQueryGraph(qg) shouldBe Seq(
      MatMul(
        NodesMat,
        EdgeMat("a")
      )
    )

  }

}
