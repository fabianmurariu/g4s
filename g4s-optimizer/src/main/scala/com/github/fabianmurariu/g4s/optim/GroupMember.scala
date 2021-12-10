package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.optim.impls.Operator
import com.github.fabianmurariu.g4s.optim.logic.LogicNode
import com.github.fabianmurariu.g4s.optim.rules.Rule

sealed abstract class GroupMember { self =>

  def exploreMemberV2(
      rules: Vector[Rule],
      store: StatsStore
  ): Vector[GroupMember] = {
    rules
      .flatMap(_.eval(self, store))
  }

  def logic: LogicNode

  def appliedRules: Set[Class[_ <: Rule]] = Set.empty

  def updateRules(newRules: Set[Class[_ <: Rule]] ) = self match {
      case g:UnEvaluatedGroupMember => g.copy(appliedRules = newRules)
      case g:CostedGroupMember => g.copy(appliedRules = newRules)
      case g:EvaluatedGroupMember => g.copy(appliedRules = newRules)
  }
}

sealed trait PhysicalPlanMember extends GroupMember {
  def plan: Operator
}
case class UnEvaluatedGroupMember(logic: LogicNode, override val appliedRules: Set[Class[_ <: Rule]] = Set.empty)
    extends GroupMember
case class EvaluatedGroupMember(logic: LogicNode, plan: Operator, override val appliedRules: Set[Class[_ <: Rule]] = Set.empty)
    extends PhysicalPlanMember

case class CostedGroupMember(
    logic: LogicNode,
    plan: Operator,
    cost: Double,
    cardinality: Long,
    override val appliedRules: Set[Class[_ <: Rule]] = Set.empty
) extends PhysicalPlanMember
