package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import alleycats.std.iterable._
import cats.effect.IO
import scala.annotation.tailrec

class Optimizer(rules: Vector[Rule]) {

  private def initMemo(qg: QueryGraph): IO[Memo] = {

    val rootPlans: Map[Binding, LogicNode] = qg.returns
      .map(name => LogicNode.fromQueryGraph(qg)(name).map((name, _)))
      .collect {
        case Right((name: Binding, p)) => name -> p
      }
      .toMap

    for {
      memo <- Memo(rootPlans)
      m <- rootPlans.values.foldM(memo) { (m, logicalPlan) =>
        m.doEnqueuePlan(logicalPlan).map(_ => m)
      }
    } yield m
  }

  def optimize(qg: QueryGraph, graph: EvaluatorGraph): IO[Memo] = {
    val memo = initMemo(qg)

    def loop(m: Memo): IO[Memo] =
      m.pop.flatMap {
        case None => IO.delay(m)
        case Some(group) =>
          println(s"POP ${group.logic.signature}")
          group
            .exploreGroup(rules, graph)
            .flatMap(_ => loop(m))
      }

    memo.flatMap(loop)
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
    @tailrec
    def loop(m: MemoV2): MemoV2 =
      MemoV2.pop(m) match {
        case None => m
        case Some((grp, memo)) =>
          // println(s"POP ${grp.logic.signature}")
          loop(MemoV2.exploreGroup(memo)(grp, OptimizerV2.defaultRules, ss))
      }

    loop(memo)
  }
}

object Optimizer {
  def default: Optimizer =
    new Optimizer(
      Vector(
        new LoadEdges,
        new LoadNodes,
        new Filter2MxM,
        new Expand2MxM,
        new TreeJoinDiagFilter
      )
    )
}

object OptimizerV2 {
    def defaultRules = Vector(
        new rules2.FilterExpandComutative,
        new rules2.LoadEdges,
        new rules2.LoadNodes,
        new rules2.Filter2MxM,
        new rules2.Expand2MxM,
        new rules2.TreeJoinDiagFilter,
    )
}