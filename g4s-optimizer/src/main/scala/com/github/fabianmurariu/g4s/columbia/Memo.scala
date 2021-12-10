package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.logic.GroupRef

import scala.collection.mutable

case class Memo(
    groupExpressions: mutable.Set[GroupExpression] = mutable.Set.empty,
    groups: mutable.ArrayBuffer[Group] = mutable.ArrayBuffer.empty
) {

  def addNewGroup(gExpr: GroupExpression): Int = {
    val newGroupId = groups.size
    groups += Group(newGroupId)
    newGroupId
  }

  def getGroupById(groupId: Int): Group = {
    groups(groupId)
  }

  def insertExpression(
      gExpr: GroupExpression,
      enforced: Boolean = false,
      targetGroupId: Int = -1
  ): Option[GroupExpression] = {
    gExpr.node match {
      case LogicOptN(GroupRef(groupId)) =>
        gExpr.setGroupId(groupId)
        None
      case _ =>
        gExpr.setGroupId(targetGroupId)
        if (groupExpressions(gExpr)) {
          groupExpressions.find(_ == gExpr)
        } else {
          groupExpressions += gExpr
          val groupId: Int = targetGroupId match {
            case -1 => addNewGroup(gExpr)
            case _  => targetGroupId
          }

          val group: Group = getGroupById(groupId)
          group.addExpression(gExpr, enforced)
          Some(gExpr)
        }
    }
  }

}
