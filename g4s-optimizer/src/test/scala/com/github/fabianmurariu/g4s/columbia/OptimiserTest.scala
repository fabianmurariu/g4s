package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.StatsStore
import com.github.fabianmurariu.g4s.optim.logic.{
  Expand,
  Filter,
  GetEdges,
  GetNodes
}
import munit.FunSuite

class OptimiserTest extends FunSuite {

  test("optimise a one step expand (a:A)-[:Y]-> to physical plan") {
    val optimiser = new Optimiser

    val logical = Expand(
      from = GetNodes("A", "a"),
      to = GetEdges(List("X"))
    )

    val actual = optimiser.chooseBestPlan(logical, StatsStore())
    println(actual)
    assertEquals(actual.isRight, true)
  }

  test("optimise a 2 step expand and filter (a:A)-[:Y]->(b:B) to physical plan") {
    val optimiser = new Optimiser

    val logical = Filter(
      Expand(
        from = GetNodes("A", "a"),
        to = GetEdges(List("X"))
      ),
      GetNodes("B", "b")
    )

    val actual = optimiser.chooseBestPlan(logical, StatsStore())
    println(actual)
    assertEquals(actual.isRight, true)
  }

}
