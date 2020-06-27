package com.github.fabianmurariu.g4s.graph.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import cats.Id
class UndirectedGraphSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "Graph" should "insert any number of nodes resolve the correct Order and Size" in forAll{ ts:Vector[(Int, Int)] =>
    val g = UndirectedGraph.fromTuples[Id](ts)
    val G = UndirectedGraph[Id, UndirectedGraph.AdjacencyMap]
   
    val expectedOder = ts.flatMap{case (v1, v2) => List(v1, v2)}.distinct.size
    G.orderG(g) shouldBe expectedOder
  }

  it should "support a self edge (0, 0)" in {
    val g = UndirectedGraph.fromTuples[Id](Vector(0 -> 0))
    val G = UndirectedGraph[Id, UndirectedGraph.AdjacencyMap]

    G.sizeG(g) shouldBe 1
    G.neighbours(g)(0) shouldBe Set(0)
  }

  it should "find the 1 neighbour in a 2 node graph with one edge" in {
    val g = UndirectedGraph.fromTuples[Id](Vector(0 -> 1))
    val G = UndirectedGraph[Id, UndirectedGraph.AdjacencyMap]

    G.sizeG(g) shouldBe 1
    G.neighbours(g)(0) shouldBe Set(1)
  }

  it should "run dfs on a single edge graph" in {
    val g = UndirectedGraph.fromTuples[Id](Vector(0 -> 1))
    val G = UndirectedGraph[Id, UndirectedGraph.AdjacencyMap]

    val (_, seen) = G.dfs(g)(0)
    seen shouldBe Map(0 -> None, 1 -> Some(0))
  }

}
