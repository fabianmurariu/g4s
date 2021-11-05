package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.optim.impls.Operator

case class GroupV2(
    logic: LogicNode,
    equivalentExprs: Vector[GroupMember],
    optMember: Option[GroupMember] = None
)

object GroupV2 {
  def apply(logic: LogicNode): GroupV2 =
    GroupV2(logic, Vector(UnEvaluatedGroupMember(logic)))

  def appendMember(g: GroupV2)(member: GroupMember): GroupV2 =
    g.copy(equivalentExprs = member +: g.equivalentExprs)

  def optimGroup(g: GroupV2, m: MemoV2): (MemoV2, Option[CostedGroupMember]) = {
    val evaluatedGroupMembers = g.equivalentExprs
      .collect {
        case egm: EvaluatedGroupMember => egm
      }

    val aa = evaluatedGroupMembers.foldLeft(
      (m, Double.MaxValue, Option.empty[CostedGroupMember])
    ) {
      case (ctx @ (memo, bestCost, _), physical: PhysicalPlanMember) =>
        val (cost, card) = Operator.relativeCost(memo, physical.plan)
        if (cost < bestCost) {
          val costedPlan =
            CostedGroupMember(physical.logic, physical.plan, cost, card)
          val newMemo =
            MemoV2.updateGroup(memo)(g.copy(optMember = Some(costedPlan)))
          (newMemo, cost, Some(costedPlan))
        } else ctx
    }

    val (newMemo, _, bestGm) = aa

    val updatedMemo = MemoV2.updateGroup(newMemo)(g.copy(optMember = bestGm))
    updatedMemo -> bestGm
  }

}