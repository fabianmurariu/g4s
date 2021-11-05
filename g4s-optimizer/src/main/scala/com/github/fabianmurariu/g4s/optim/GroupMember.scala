package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.optim.impls.Operator

sealed abstract class GroupMember { self =>

  def exploreMemberV2(
      rules: Vector[rules2.Rule],
      store: StatsStore
  ): Vector[GroupMember] = {
    rules
      .flatMap(_.eval(self, store))
  }

  def logic: LogicNode

}

sealed trait PhysicalPlanMember extends GroupMember {
  def plan: Operator
}
case class UnEvaluatedGroupMember(logic: LogicNode, explored: Boolean = false)
    extends GroupMember
case class EvaluatedGroupMember(logic: LogicNode, plan: Operator)
    extends PhysicalPlanMember

case class CostedGroupMember(
    logic: LogicNode,
    plan: Operator,
    cost: Double,
    cardinality: Long
) extends PhysicalPlanMember
