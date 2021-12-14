package com.github.fabianmurariu.g4s.columbia

import scala.collection.mutable

case class GroupExpression(
    node: OptimiserNode,
    childGroups: Vector[Int],
    var groupId: Int = -1,
    var bestCost: Option[Double] = None
)(rulesApplied: mutable.BitSet = mutable.BitSet.empty) {

  def updateBestCost(curTotalCost: Double): Unit = bestCost match {
    case None =>
      bestCost = Some(curTotalCost)
    case Some(current) if current > curTotalCost =>
      bestCost = Some(curTotalCost)
    case _ => ()
  }

  var statsDerived: Boolean = false

  def setStatsDerived(): Unit =
    statsDerived = true

  def setExplored(rule: Rule): Unit = {
    rulesApplied += rule.id
  }

  def isLogical: Boolean = node match {
    case _: LogicOptN => true
    case _            => false
  }

  def hasRuleExplored(rule: Rule): Boolean =
    rulesApplied(rule.id)

  def setGroupId(groupId: Int): Unit = {
    this.groupId = groupId
  }

}
