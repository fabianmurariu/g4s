package com.github.fabianmurariu.g4s.optim.rules.impl

import com.github.fabianmurariu.g4s.optim.{impls => op}
import com.github.fabianmurariu.g4s.optim.rules.ImplementationRule
import com.github.fabianmurariu.g4s.optim.GroupMember
import com.github.fabianmurariu.g4s.optim.StatsStore
import com.github.fabianmurariu.g4s.optim.EvaluatedGroupMember
import com.github.fabianmurariu.g4s.optim.logic.GetEdges

class LoadEdges extends ImplementationRule {

  def apply(
      gm: GroupMember,
      stats: StatsStore
  ): List[GroupMember] = {
    gm.logic match {
      case GetEdges((tpe: String) :: _, transpose) =>
        val edgesCard = stats.edgesTotal(Some(tpe))
        val physical: op.Operator =
          op.GetEdgeMatrix(None, Some(tpe), transpose, edgesCard)
        List(
          EvaluatedGroupMember(gm.logic, physical)
        )

    }
  }

  override def isDefinedAt(gm: GroupMember): Boolean =
    gm.logic.isInstanceOf[GetEdges]

}
