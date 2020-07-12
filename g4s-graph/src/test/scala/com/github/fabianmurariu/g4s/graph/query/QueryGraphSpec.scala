package com.github.fabianmurariu.g4s.graph.query

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QueryGraphSpec extends AnyFlatSpec with Matchers{

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
        y -> Map(x -> QueryEdge(0, Some(1), Set("friends")))
        ),
      2
    )

  }

}
