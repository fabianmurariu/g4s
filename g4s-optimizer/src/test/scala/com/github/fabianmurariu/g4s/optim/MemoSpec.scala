package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.optim.logic.{Expand, GetEdges, GetNodes, LogicMemoRefV2}

import scala.collection.immutable.Queue
import com.github.fabianmurariu.g4s.optim.rules.trans.FilterChainPermute

class MemoSpec extends munit.FunSuite with MemoFixtures {

  test("Enqueue Get Nodes") {

    val m = MemoV2()

    val nodes = GetNodes("A", "a")

    val (actualMemo, actualGroup) = MemoV2.doEnqueuePlan(m)(nodes)

    val expectedGroup =
      GroupV2(nodes, Vector(UnEvaluatedGroupMember(nodes)), None)
    val expectedMemo =
      MemoV2(
        Map.empty,
        Queue(nodes.signature),
        Map(nodes.signature -> expectedGroup)
      )

    assertEquals(actualGroup, expectedGroup)
    assertEquals(actualMemo, expectedMemo)

  }

  test("Enqueue Expand nodes") {

    val m = MemoV2()

    val nodes = GetNodes("A", "a")
    val edges = GetEdges(List("X"), false)
    val expand = Expand(nodes, edges)

    val (actualMemo, actualGroup) = MemoV2.doEnqueuePlan(m)(expand)

    val nodesGroup =
      GroupV2(nodes, Vector(UnEvaluatedGroupMember(nodes)), None)
    val edgesGroup =
      GroupV2(edges, Vector(UnEvaluatedGroupMember(edges)), None)

    val expandGroupLogic = Expand(LogicMemoRefV2(nodes), LogicMemoRefV2(edges))

    val expandGroup =
      GroupV2(
        expandGroupLogic,
        Vector(UnEvaluatedGroupMember(expandGroupLogic)),
        None
      )

    val expectedMemo =
      MemoV2(
        Map.empty,
        Queue(nodes.signature, edges.signature, expand.signature),
        Map(
          nodes.signature -> nodesGroup,
          edges.signature -> edgesGroup,
          expand.signature -> expandGroup
        )
      )

    assertEquals(actualGroup, expandGroup)
    assertEquals(actualMemo, expectedMemo)
  }

  test("Nested Filter") {

    val m = MemoV2()

    val (actualMemo, actualGroup) = MemoV2.doEnqueuePlan(m)(filter2)

    assertEquals(actualGroup, filter2Group)
    assertEquals(actualMemo.queue, expectedMemo.queue)
    assertEquals(actualMemo.rootPlans, expectedMemo.rootPlans)
    assertEquals(actualMemo.table.keySet, expectedMemo.table.keySet)
    actualMemo.table.foreach {
      case (sig, actual) =>
        val expected = expectedMemo.table(sig)
        assertEquals(actual, expected)
    }
    assertEquals(
      actualMemo.table.values.toVector,
      expectedMemo.table.values.toVector
    )

  }

  test("Nested Filter, run FilterChainPermute".only) {

    val m = MemoV2()

    val (actualMemo, actualGroup) = MemoV2.doEnqueuePlan(m)(filter2)

    val optimMemo = Optimizer.memoLoop(Vector(new FilterChainPermute))(actualMemo, StatsStore())

    val filter2Group = optimMemo.table(filter2.signature)
    filter2Group.equivalentExprs.foreach(println)
    println(filter2)
  }
}
