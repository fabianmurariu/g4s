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

    val Right((actual, ctx, operator)) =
      optimiser.chooseBestPlan(logical, StatsStore())
    optimiser.renderExpression(actual, ctx.memo).map(println)
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

    val Right((actual, ctx, operator)) =
      optimiser.chooseBestPlan(logical, StatsStore())
    optimiser.renderExpression(actual, ctx.memo).map(println)
  }

  test(
    "optimise a 2 step expand and filter to physical plan with StatsStore hints resulting in A * (X * B)"
  ) {
    val optimiser = new Optimiser

    val query = """match (a:A)-[:X]->(b:B) return b"""

    val Right(qg) = QueryGraph.fromCypherText(query)
    val Right(logical) = LogicNode.fromQueryGraph(qg)(Binding("b"))

    val ss = StatsStore()

    ss.addNode("B")

    (1 to 5).foreach { _ => ss.addNode("A") }

    (1 to 5).foreach { _ => ss.addEdge("X") }
    (1 to 4).foreach { _ => ss.addNode("D") }

    val Right((actual, ctx, operator)) =
      optimiser.chooseBestPlan(logical, ss)
    optimiser.renderExpression(actual, ctx.memo).map(println)

//    ExpandMul[sel=1.0, size=20, sorted=a, output=[a,b], cost=28.8]:
//      l=Nodes[size=5, sorted=a, output=[a], cost=0.0]
//      r=FilterMul[sel=1.0, size=4, output=[*,b], cost=4.8]:
//        l=Edges[size=4, output=[*], cost=0.0, label=[X], transpose=false]
//        r=Nodes[size=1, sorted=b, output=[b], cost=0.0]

//    ExpandMul[sel=1.0, size=5, sorted=a, output=[a,b], cost=7.2]:
//      l=Nodes[size=5, sorted=a, output=[a], cost=0.0]
//      r=FilterMul[sel=0.1, size=1, output=[b], cost=1.2]:
//        l=Edges[size=5, output=[], cost=0.0, label=[fix.X], transpose=false]
//        r=Nodes[size=1, sorted=b, output=[b], cost=0.0]

//    ExpandMul[sel=1.0, size=1, sorted=a, output=[a,b], cost=2.4]:
//      l=Nodes[size=0, sorted=a, output=[a], cost=0.0]
//      r=FilterMul[sel=1.0, size=1, output=[*,b], cost=1.2]:
//        l=Edges[size=0, output=[*], cost=0.0, label=[fix.X], transpose=false]
//        r=Nodes[size=0, sorted=b, output=[b], cost=0.0]
  }

  test(
    "optimise a 3 step expand and filter (a:`fix.A`)-[:`fix.X`]->(b:`fix.B`)<-[:`fix.Y`]-(c:`fix.C`) to physical plan"
  ) {
    val optimiser = new Optimiser

    val query =
      """match (a:`fix.A`)-[:`fix.X`]->(b:`fix.B`)<-[:`fix.Y`]-(c:`fix.C`) return c"""

    val Right(qg) = QueryGraph.fromCypherText(query)
    val Right(logical) = LogicNode.fromQueryGraph(qg)(Binding("c"))

    val Right((actual, ctx, operator)) =
      optimiser.chooseBestPlan(logical, StatsStore())
    optimiser.renderExpression(actual, ctx.memo).map(println)
  }
}
