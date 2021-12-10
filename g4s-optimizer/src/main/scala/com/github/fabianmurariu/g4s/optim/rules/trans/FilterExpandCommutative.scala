package com.github.fabianmurariu.g4s.optim.rules.trans

import com.github.fabianmurariu.g4s.optim.rules.TransformationRule
import com.github.fabianmurariu.g4s.optim.GroupMember
import com.github.fabianmurariu.g4s.optim.StatsStore
import com.github.fabianmurariu.g4s.optim.UnEvaluatedGroupMember
import com.github.fabianmurariu.g4s.optim.logic.{Expand, Filter, LogicMemoRefV2}

class FilterExpandCommutative extends TransformationRule { self =>

  override def apply(gm: GroupMember, v2: StatsStore): List[GroupMember] =
    gm.logic match {
      case Filter(
          LogicMemoRefV2(
            Expand(left: LogicMemoRefV2, right: LogicMemoRefV2)
          ),
          filter: LogicMemoRefV2
          ) =>
        List(
          UnEvaluatedGroupMember(
            Expand(left, Filter(right, filter)),
            gm.appliedRules + self.getClass()
          )
        )
    }

  override def isDefinedAt(gm: GroupMember): Boolean = gm.logic match {
    case Filter(
        LogicMemoRefV2(Expand(_: LogicMemoRefV2, _: LogicMemoRefV2)),
        _: LogicMemoRefV2
        ) =>
      gm.isInstanceOf[UnEvaluatedGroupMember]
    case _ => false
  }

}
