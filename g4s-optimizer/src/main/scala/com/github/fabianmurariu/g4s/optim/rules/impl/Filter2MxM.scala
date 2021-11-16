package com.github.fabianmurariu.g4s.optim.rules.impl

import com.github.fabianmurariu.g4s.optim.{impls => op}
import com.github.fabianmurariu.g4s.optim.rules.ImplementationRule
import com.github.fabianmurariu.g4s.optim.GroupMember
import com.github.fabianmurariu.g4s.optim.StatsStore
import com.github.fabianmurariu.g4s.optim.Filter
import com.github.fabianmurariu.g4s.optim.LogicMemoRefV2
import com.github.fabianmurariu.g4s.optim.GetNodes
import com.github.fabianmurariu.g4s.optim.EvaluatedGroupMember

class Filter2MxM extends ImplementationRule {

  override def apply(
      gm: GroupMember,
      stats: StatsStore
  ): List[GroupMember] = {
    gm.logic match {
      case Filter(
          frontier: LogicMemoRefV2,
          filter @ LogicMemoRefV2(n: GetNodes)
          ) =>
        val sel = stats.nodeSel(n.label.headOption)
        val physical: op.Operator =
          op.FilterMul(
            op.RefOperator(frontier),
            op.RefOperator(filter),
            sel
          )

        val newGM: GroupMember = EvaluatedGroupMember(gm.logic, physical)
        List(newGM)
      case Filter(frontier: LogicMemoRefV2, filter: LogicMemoRefV2) =>
        val physical: op.Operator =
          op.FilterMul(
            op.RefOperator(frontier),
            op.Diag(op.RefOperator(filter)),
            1.0d
          )

        val newGM: GroupMember = EvaluatedGroupMember(gm.logic, physical)
        List(newGM)
    }
  }

  override def isDefinedAt(gm: GroupMember): Boolean =
    gm.logic.isInstanceOf[Filter]

}
