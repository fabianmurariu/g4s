package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.optim.impls.Operator
import cats.implicits._
import cats.data.StateT
import cats.effect.Sync

class GroupMember[F[_]: Sync](
    val parent: Group[F],
    val logic: LogicNode,
    val physic: Option[Operator[F]] = None
) {

  def memo: Memo[F] = parent.memo

  def physical: F[Operator[F]] = Sync[F].delay(physic.get)

  def exploreMember(
      rules: Vector[Rule[F]]
  ): StateT[F, EvaluatorGraph[F], Vector[GroupMember[F]]] = {
    rules
      .map(rule => rule.eval(this))
      .sequence
      .map(_.flatten)
  }

  def cost: F[Long] = Sync[F].delay(-1L)

  // for (rule <- rules) {
  //   if (rule.isDefinedAt(this)) {
  //     val newMembers = rule(this)
  //     for (newMember <- newMembers) {
  //       val logic = newMember.logic
  //       // enqueue in memo
  //       memo.doEnqueuePlan(logic)
  //       // add to the groups expression list
  //       parent.appendMember(newMember)
  //     }
  //   }
  // }
  // }
}
