package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.{Expand, Filter, GetEdges, GetNodes}
import munit.FunSuite

class ContextTest extends FunSuite {
  test("insert Expand into optimiser context") {
    val memo = Memo()
    val ctx = Context(memo)

    val filter = Filter(
      Expand(
        from = GetNodes("A", "a"),
        to = GetEdges(List("X"))
      ),
      GetNodes("B", "b")
    )
    val expand = Expand(
      GetNodes("A", "a"),
      Filter(
        GetEdges(List("X")),
        GetNodes("B", "b")
      )
    )
    ctx.recordOptimiserNodeIntoGroup(LogicOptN(expand))
    println(memo)
  }
  test("insert Filter into optimiser context") {
    val memo = Memo()
    val ctx = Context(memo)

    val filter = Filter(
      Expand(
        from = GetNodes("A", "a"),
        to = GetEdges(List("X"))
      ),
      GetNodes("B", "b")
    )
    ctx.recordOptimiserNodeIntoGroup(LogicOptN(filter))
    println(memo)
  }
}
