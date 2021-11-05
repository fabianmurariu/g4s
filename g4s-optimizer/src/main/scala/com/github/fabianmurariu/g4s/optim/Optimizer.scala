package com.github.fabianmurariu.g4s.optim

import scala.annotation.tailrec

object Optimizer {
  def optimizeV2(qg: QueryGraph, ss: StatsStore): MemoV2 = {

    val rootPlans: Map[Binding, LogicNode] = qg.returns
      .map(name => LogicNode.fromQueryGraph(qg)(name).map((name, _)))
      .collect {
        case Right((name: Binding, p)) => name -> p
      }
      .toMap

    val memo = rootPlans.values.foldLeft(MemoV2(rootPlans))((m, node) =>
      MemoV2.doEnqueuePlan(m)(node)._1
    )

    @tailrec
    def loop(rules: Vector[rules2.Rule])(m: MemoV2): MemoV2 =
      MemoV2.pop(m) match {
        case None => m
        case Some((grp, memo)) =>
          loop(rules)(MemoV2.exploreGroup(memo)(grp, rules, ss))
      }

    loop(transformationRules ++ implementationRules)(memo)
  }

  def transformationRules: Vector[rules2.Rule] = Vector(
    new rules2.FilterExpandComutative
  )

  def implementationRules = Vector(
    new rules2.LoadEdges,
    new rules2.LoadNodes,
    new rules2.Filter2MxM,
    new rules2.Expand2MxM,
    new rules2.TreeJoinDiagFilter
  )
}
