package com.github.fabianmurariu.g4s.optim

import scala.collection.mutable

class Memo(
    val rootPlans: Set[LogicNode],
    val table: mutable.LinkedHashMap[Set[Name], Group] = mutable.LinkedHashMap.empty,
    var stack: List[Group] = List.empty
) {
  def insertGroup(logic: LogicNode): Group = {
    table.getOrElseUpdate(
      logic.output, {
        val group = new Group(this, logic, table.size, logic)
        stack = group :: stack
        group
      }
    )
  }

  def doEnqueuePlan(logic: LogicNode): Group = {
    if (!logic.leaf) {
      logic.children.zipWithIndex.foreach {
        case (child, i) =>
          val group = doEnqueuePlan(child)
          logic(i) = LogicMemoRef(group)
      }
    }
    insertGroup(logic)
  }
}
