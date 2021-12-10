package com.github.fabianmurariu.g4s.optim.rules.impl

import com.github.fabianmurariu.g4s.optim.{impls => op}
import com.github.fabianmurariu.g4s.optim.rules.ImplementationRule
import com.github.fabianmurariu.g4s.optim.GroupMember
import com.github.fabianmurariu.g4s.optim.StatsStore
import com.github.fabianmurariu.g4s.optim.EvaluatedGroupMember
import com.github.fabianmurariu.g4s.optim.UnNamed
import com.github.fabianmurariu.g4s.optim.logic.GetNodes

class LoadNodes extends ImplementationRule {

  def apply(
      gm: GroupMember,
      stats: StatsStore
  ): List[GroupMember] = {
    gm.logic match {
      case GetNodes(label, sorted) =>
        val card = stats.nodesTotal(label)
        val physical = op.GetNodeMatrix(
          sorted.getOrElse(new UnNamed),
          label,
          card
        )
        List(
          EvaluatedGroupMember(gm.logic, physical)
        )

    }
  }

  override def isDefinedAt(gm: GroupMember): Boolean =
    gm.logic.isInstanceOf[GetNodes]

}
