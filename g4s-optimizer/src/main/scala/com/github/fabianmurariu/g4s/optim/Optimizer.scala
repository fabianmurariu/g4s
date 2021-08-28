package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import cats.effect.Sync
import alleycats.std.iterable._

class Optimizer[F[_]: Sync](rules: Vector[Rule[F]]) {

  private def initMemo(qg: QueryGraph): F[Memo[F]] = {

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

  def optimize(qg: QueryGraph, graph: EvaluatorGraph[F]): F[Memo[F]] = {
    val memo = initMemo(qg)

    def loop(m: Memo[F]): F[Memo[F]] =
      m.pop.flatMap{
        case None => Sync[F].delay(m)
        case Some(group) =>
          group
            .exploreGroup(rules, graph)
          .flatMap(_ => loop(m))
      }

    memo.flatMap(loop)
  }

}
object Optimizer {
  def apply[F[_]:Sync] = new Optimizer[F](Vector(
    new LoadEdges[F],
    new LoadNodes[F],
    new Filter2MxM[F],
    new Expand2MxM[F]
  ))
}
