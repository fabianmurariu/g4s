package com.github.fabianmurariu.g4s.graph.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import cats.Id
import Graph.decorateGraphOps
import org.scalacheck.Prop
import zio.Task

class SingleTestGraphSpec extends AnyFlatSpec with Matchers {
  "Graph" should "find connected componets in a disjoint graph of 2 nodes" in {
    val g = Graph[AdjacencyMap, Id].empty[Int, String]

    val actual: List[Map[Int, Option[Int]]] =
      g.insertVertex(0).insertVertex(1).connectedComponents

    actual should contain theSameElementsAs List(Map(0 -> None), Map(1 -> None))
  }

  it should "pass a simple graph with 2 nodes with self connected edges" in {

    val g = GrBSparseMatrixGraph.empty[Int, String]

    val task = g.use { fg =>
      Task {
        val actual = GraphProps[GrBSparseMatrixGraph, Task]
          .canFind2ConnectedComponentsInAGraph(Task(fg))(
            List((0, "even", 0)),
            List((1, "odd", 1))
          )

        actual shouldBe true
      }
    }

    zio.Runtime.default.unsafeRun(task)
  }
}
