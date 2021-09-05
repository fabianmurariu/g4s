package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import alleycats.std.iterable._
import cats.effect.IO

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
      m.pop.flatMap{
        case None => IO.delay(m)
        case Some(group) =>
          group
            .exploreGroup(rules, graph)
          .flatMap(_ => loop(m))
      }

    memo.flatMap(loop)
  }

}
object Optimizer {
  def default: Optimizer = new Optimizer(Vector(
    new LoadEdges,
    new LoadNodes,
    new Filter2MxM,
    new Expand2MxM,
    new TreeJoinDiagFilter
  ))
}
