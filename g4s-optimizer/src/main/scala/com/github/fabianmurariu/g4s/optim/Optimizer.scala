package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.optim.logic.LogicNode

import scala.annotation.tailrec
import com.github.fabianmurariu.g4s.optim.rules.Rule
import com.github.fabianmurariu.g4s.optim.rules.trans.FilterExpandCommutative
import com.github.fabianmurariu.g4s.optim.rules.impl.LoadEdges
import com.github.fabianmurariu.g4s.optim.rules.impl.LoadNodes
import com.github.fabianmurariu.g4s.optim.rules.impl.Filter2MxM
import com.github.fabianmurariu.g4s.optim.rules.impl.Expand2MxM
import com.github.fabianmurariu.g4s.optim.rules.impl.Fork2DiagFilter

object Optimizer {

  @tailrec
  def memoLoop(rules: Vector[Rule])(m: MemoV2, ss: StatsStore): MemoV2 =
    MemoV2.pop(m) match {
      case None => m
      case Some((grp, memo)) =>
        memoLoop(rules)(MemoV2.exploreGroup(memo)(grp, rules, ss), ss)
    }

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

    memoLoop(transformationRules ++ implementationRules)(memo, ss)
  }

  def transformationRules: Vector[Rule] = Vector(
    new FilterExpandCommutative
  )

  def implementationRules: Vector[Rule] = Vector(
    new LoadEdges,
    new LoadNodes,
    new Filter2MxM,
    new Expand2MxM,
    new Fork2DiagFilter
  )
}
