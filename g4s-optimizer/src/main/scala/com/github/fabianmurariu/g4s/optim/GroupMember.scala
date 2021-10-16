package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.optim.impls.Operator
import cats.implicits._
import cats.effect.IO

sealed abstract class GroupMember { self =>

  def exploreMember(
      rules: Vector[Rule],
      graph: EvaluatorGraph
  ): IO[Vector[GroupMember]] = {
      rules
        .map(rule => rule.eval(this, graph))
        .sequence
        .map(_.flatten)
  }

  def exploreMemberV2(
      rules: Vector[rules2.Rule],
      store: StatsStore
  ): Vector[GroupMember] = {
      rules
        .flatMap{rule => 
          rule.eval(self, store)
        }
  }

  def logic: LogicNode

}

case class UnEvaluatedGroupMember(logic: LogicNode) extends GroupMember
case class EvaluatedGroupMember(logic: LogicNode, plan: Operator)
    extends GroupMember {

  def cost: Long = plan.cardinality
}
