package com.github.fabianmurariu.g4s.columbia

import scala.collection.mutable.ArrayBuffer

case class Group(
    var groupId: Int,
    physicalExprs: ArrayBuffer[GroupExpression] = ArrayBuffer.empty,
    logicalExprs: ArrayBuffer[GroupExpression] = ArrayBuffer.empty,
    enforcedExprs: ArrayBuffer[GroupExpression] = ArrayBuffer.empty,
    var bestExpression: Option[GroupExpression] = None,
    private var explored: Boolean = false
) {
  def setExplored(): Unit = explored = true

  def isExplored: Boolean = explored

  def addExpression(gExpr: GroupExpression, enforced: Boolean): Unit = {
    gExpr.setGroupId(groupId)
    if (enforced) {
      enforcedExprs += gExpr
    } else {
      gExpr.node match {
        case _: LogicOptN =>
          logicalExprs += gExpr
        case _: PhysicalOptN =>
          physicalExprs += gExpr
      }
    }
  }

}

object Group {
  val unidentified: Int = -1
}
