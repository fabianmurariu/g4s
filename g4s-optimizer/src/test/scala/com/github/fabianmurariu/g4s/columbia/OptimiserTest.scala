package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.{Binding, QueryGraph, StatsStore}
import com.github.fabianmurariu.g4s.optim.logic.{
  Expand,
  Filter,
  GetEdges,
  GetNodes,
  LogicNode
}
import munit.FunSuite

class OptimiserTest extends FunSuite {

  test("optimise a one step expand (a:A)-[:Y]-> to physical plan") {
    val optimiser = new Optimiser

    val logical = Expand(
      from = GetNodes("A", "a"),
      to = GetEdges(List("X"))
    )

    val Right(actual) = optimiser.chooseBestPlan(logical, StatsStore())
    println(actual.show(null))
  }

  test("optimise a 2 step expand and filter (a:A)-[:X]->(b:B) to physical plan") {
    val optimiser = new Optimiser

    val logical = Filter(
      Expand(
        from = GetNodes("A", "a"),
        to = GetEdges(List("X"))
      ),
      GetNodes("B", "b")
    )

    val Right(actual) = optimiser.chooseBestPlan(logical, StatsStore())
    println(actual.show(null))
  }

  test(
    "optimise a 2 step expand and filter to physical plan with StatsStore hints"
  ) {
    val optimiser = new Optimiser

    val logical = Filter(
      Expand(
        from = GetNodes("A", "a"),
        to = GetEdges(List("X"))
      ),
      GetNodes("B", "b")
    )

    val ss = StatsStore()

    ss.addNode("B")

    (1 to 5).foreach { _ => ss.addNode("A") }

    (1 to 4).foreach { _ => ss.addEdge("X") }

    val Right(actual) = optimiser.chooseBestPlan(logical, ss)
    println(actual.show(null))
  }

  test(
    "optimise a 3 step expand and filter (a:`fix.A`)-[:`fix.X`]->(b:`fix.B`)<-[:`fix.Y`]-(c:`fix.C`) to physical plan"
  ) {
    val optimiser = new Optimiser

    val query =
      """match (a:`fix.A`)-[:`fix.X`]->(b:`fix.B`)<-[:`fix.Y`]-(c:`fix.C`) return c"""

    val Right(qg) = QueryGraph.fromCypherText(query)
    val Right(logical) = LogicNode.fromQueryGraph(qg)(Binding("c"))

    val Right(actual) = optimiser.chooseBestPlan(logical, StatsStore())
    println(actual.show(null))
  }
}
