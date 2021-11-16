package com.github.fabianmurariu.g4s.optim.rules.impl

import com.github.fabianmurariu.g4s.optim.{impls => op}
import com.github.fabianmurariu.g4s.optim.rules.ImplementationRule
import com.github.fabianmurariu.g4s.optim.GroupMember
import com.github.fabianmurariu.g4s.optim.StatsStore
import com.github.fabianmurariu.g4s.optim.LogicMemoRefV2
import com.github.fabianmurariu.g4s.optim.EvaluatedGroupMember
import com.github.fabianmurariu.g4s.optim.Expand
import com.github.fabianmurariu.g4s.optim.GetEdges

class Expand2MxM extends ImplementationRule {

  override def apply(
      gm: GroupMember,
      stats: StatsStore
  ): List[GroupMember] = {
    gm.logic match {
      case Expand(
          from: LogicMemoRefV2,
          to @ LogicMemoRefV2(e: GetEdges)
          ) =>
        val sel = stats.edgeSel(e.tpe.headOption)

        val physical: op.Operator =
          op.ExpandMul(op.RefOperator(from), op.RefOperator(to), sel)

        val newGM = EvaluatedGroupMember(gm.logic, physical)
        List(newGM)
      case Expand(from: LogicMemoRefV2, to: LogicMemoRefV2) =>
        val physical: op.Operator =
          op.ExpandMul(op.RefOperator(from), op.RefOperator(to))

        val newGM = EvaluatedGroupMember(gm.logic, physical)
        List(newGM)

    }
  }

  override def isDefinedAt(gm: GroupMember): Boolean =
    gm.logic.isInstanceOf[Expand]

}
