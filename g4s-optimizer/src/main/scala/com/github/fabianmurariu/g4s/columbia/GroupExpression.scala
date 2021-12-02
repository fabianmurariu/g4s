package com.github.fabianmurariu.g4s.columbia

import scala.collection.mutable

case class GroupExpression(
    node: OptimiserNode,
    childGroups: Vector[Int],
    var groupId: Int = -1
)(rulesApplied: mutable.BitSet = mutable.BitSet.empty) {
  def setGroupId(groupId: Int): Unit = {
    this.groupId = groupId
  }

}
