package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.optim.impls.Operator
import cats.implicits._
import cats.effect.Sync

sealed abstract class GroupMember[F[_]: Sync] { self =>

  def exploreMember(
      rules: Vector[Rule[F]],
      graph: EvaluatorGraph[F]
  ): F[Vector[GroupMember[F]]] = {
    Sync[F].delay(println(s"Exploring member $this")) *>
    rules
      .map(rule => rule.eval(this, graph))
      .sequence
      .map(_.flatten)
  }

  def logic: LogicNode

}

case class UnEvaluatedGroupMember[F[_]: Sync](logic: LogicNode)
    extends GroupMember[F]
case class EvaluatedGroupMember[F[_]: Sync](logic: LogicNode, plan: Operator[F])
    extends GroupMember[F] {

  def cost: F[Long] = Sync[F].delay(plan.cardinality)
}
