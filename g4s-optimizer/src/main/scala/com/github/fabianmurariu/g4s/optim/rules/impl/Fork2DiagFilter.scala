package com.github.fabianmurariu.g4s.optim.rules.impl

import com.github.fabianmurariu.g4s.optim.{impls => op}
import com.github.fabianmurariu.g4s.optim.rules.ImplementationRule
import com.github.fabianmurariu.g4s.optim.GroupMember
import com.github.fabianmurariu.g4s.optim.StatsStore
import com.github.fabianmurariu.g4s.optim.EvaluatedGroupMember
import com.github.fabianmurariu.g4s.optim.logic.{Filter, Join, LogicMemoRefV2}

/**
  *  .. (a)-[]->(b)-[]->(c) return b
  *
  * the tree breaks into 2 branches
  * (a)-[]->(b)
  * (c)<-[]-(b)
  *  *
  * depending on direction these need to be joined on b
  * and/or sorted
  *
  * */
class Fork2DiagFilter extends ImplementationRule {

  override def apply(
      gm: GroupMember,
      v2: StatsStore
  ): List[GroupMember] =
    gm.logic match {
      case Join(left: LogicMemoRefV2, right: LogicMemoRefV2, _) =>
        (left.plan, right.plan) match {
          case (
              Filter(front1: LogicMemoRefV2, _),
              Filter(front2: LogicMemoRefV2, _)
              ) =>
            List(
              EvaluatedGroupMember(
                gm.logic,
                op.FilterMul(
                  op.RefOperator(front1),
                  op.Diag(op.RefOperator(right)),
                  1.0d // we can probably do better than this
                )
              ),
              EvaluatedGroupMember(
                gm.logic,
                op.FilterMul(
                  op.RefOperator(front2),
                  op.Diag(op.RefOperator(left)),
                  1.0d // we can probably do better than this
                )
              )
            )
        }
    }

  override def isDefinedAt(gm: GroupMember): Boolean = gm.logic match {
    case Join(l: LogicMemoRefV2, r: LogicMemoRefV2, _) =>
      r.plan.isInstanceOf[Filter] && l.plan.isInstanceOf[Filter]
    case _ => false
  }

}
