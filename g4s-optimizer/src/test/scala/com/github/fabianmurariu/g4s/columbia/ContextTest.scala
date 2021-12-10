package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.StatsStore
import com.github.fabianmurariu.g4s.optim.logic.{Expand, Filter, GetEdges, GetNodes, GroupRef}
import munit.FunSuite

class ContextTest extends FunSuite {
  test("insert Expand into optimiser context") {
    val memo = Memo()
    val ctx = Context(memo, StatsStore())

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
    val ctx = Context(memo, StatsStore())

    val filter = Filter(
      Expand(
        from = GetNodes("A", "a"),
        to = GetEdges(List("X"))
      ),
      GetNodes("B", "b")
    )
    val Some(groupExpr) = ctx.recordOptimiserNodeIntoGroup(LogicOptN(filter))
    println(memo)
  }

  test(
    "insert Filter into optimiser context, run GroupExpressionBindingIterator 1 level"
  ) {
    val memo = Memo()
    val ctx = Context(memo, StatsStore())

    val filter = Filter(
      Expand(
        from = GetNodes("A", "a"),
        to = GetEdges(List("X"))
      ),
      GetNodes("B", "b")
    )
    val Some(groupExpr) = ctx.recordOptimiserNodeIntoGroup(LogicOptN(filter))

    val pattern = FilterPat(AnyMatch, AnyMatch)
    val iter = GroupExpressionBindingIterator(memo, groupExpr, pattern)

    assertEquals(iter.hasNext, true)
    val item = iter.next()
    assertEquals(item, LogicOptN(Filter(GroupRef(2), GroupRef(3))))
    assertEquals(iter.hasNext, false)
  }
  test(
    "insert Filter into optimiser context, run GroupExpressionBindingIterator 2 level".only
  ) {
    val memo = Memo()
    val ctx = Context(memo, StatsStore())

    val filter = Filter(
      Expand(
        from = GetNodes("A", "a"),
        to = GetEdges(List("X"))
      ),
      GetNodes("B", "b")
    )
    val Some(groupExpr) = ctx.recordOptimiserNodeIntoGroup(LogicOptN(filter))

    val pattern = FilterPat(ExpandPat(AnyMatch, AnyMatch), AnyMatch)
    val iter = GroupExpressionBindingIterator(memo, groupExpr, pattern)

    assertEquals(iter.hasNext, true)
    val item = iter.next()
    assertEquals(
      item,
      LogicOptN(Filter(Expand(GroupRef(0), GroupRef(1)), GroupRef(3)))
    )
    assertEquals(iter.hasNext, false)
  }
}
