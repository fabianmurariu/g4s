package com.github.fabianmurariu.g4s.columbia

import scala.collection.mutable.ArrayBuffer

case class Group(
    var groupId: Int,
    physicalExprs: ArrayBuffer[GroupExpression] = ArrayBuffer.empty,
    logicalExprs: ArrayBuffer[GroupExpression] = ArrayBuffer.empty,
    enforcedExprs: ArrayBuffer[GroupExpression] = ArrayBuffer.empty,
    var bestExpression: Option[(GroupExpression, Double)] = None,
    var costLowerBound: Double = -1d, // FIXME: this is useless for now
    private var explored: Boolean = false,
    var estNumRows: Option[Long] = None,
    var estSelectivity: Option[Double] = Some(1d)
) {

  def getCardinality: Option[Long] = estNumRows

  def setExpressionCost(expr: GroupExpression, cost: Double): Unit =
    bestExpression match {
      case None =>
        bestExpression = Some(expr -> cost)
      case Some((_, current)) if current > cost =>
        bestExpression = Some(expr -> cost)
      case _ => ()
    }

  def getCostLB: Double = costLowerBound

  def setCardinality(numNodes: Long): Unit =
    estNumRows = Some(numNodes)


  def setSelectivity(sel: Double): Unit =
    estSelectivity = Some(sel)

  def isEstimated: Boolean =
    estNumRows.isDefined && estSelectivity.isDefined

  def setExplored(): Unit =
    explored = true

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
