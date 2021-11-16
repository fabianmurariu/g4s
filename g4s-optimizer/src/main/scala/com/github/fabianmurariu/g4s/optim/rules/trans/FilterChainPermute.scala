package com.github.fabianmurariu.g4s.optim.rules.trans

import com.github.fabianmurariu.g4s.optim.{GroupMember, StatsStore}
import com.github.fabianmurariu.g4s.optim.rules.TransformationRule
import com.github.fabianmurariu.g4s.optim.Filter
import com.github.fabianmurariu.g4s.optim.LogicMemoRefV2
import com.github.fabianmurariu.g4s.optim.UnEvaluatedGroupMember

class FilterChainPermute extends TransformationRule { self =>

  override def apply(v1: GroupMember, v2: StatsStore): List[GroupMember] =
    v1.logic match {
      case Filter(branch1, LogicMemoRefV2(f2 @ Filter(branch2, _))) =>
        val appliedRules = v1.appliedRules + self.getClass()

        List(
          v1.updateRules(appliedRules),
          UnEvaluatedGroupMember(
            Filter(branch2, f2.copy(frontier = branch1)),
            appliedRules
          )
        )
    }

  override def isDefinedAt(gm: GroupMember): Boolean = gm.logic match {
    case Filter(_, LogicMemoRefV2(_: Filter)) => true
    case _                                    => false
  }

}
