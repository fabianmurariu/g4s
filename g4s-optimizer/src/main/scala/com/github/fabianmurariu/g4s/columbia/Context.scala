package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.GroupRef

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class Context(
    memo: Memo,
    stack: mutable.ArrayDeque[OptimizerTask] = mutable.ArrayDeque.empty,
    rule: Vector[Rule] = Vector.empty
) {
  def push(task: OptimizerTask): Context = {
    stack.append(task)
    this
  }

  def pop(): Option[OptimizerTask] = {
    stack.removeLastOption()
  }

  def isEmpty: Boolean =
    stack.isEmpty

  def recordOptimiserNodeIntoGroup(
      node: OptimiserNode,
      groupId: Int = Group.unidentified
  ): Option[GroupExpression] = {
    val newExpr = makeGroupExpression(node)
    memo.insertExpression(newExpr, enforced = false, groupId)
  }

  def makeGroupExpression(node: OptimiserNode): GroupExpression = {
    val childGroups = ArrayBuffer.empty[Int]
    for (child <- node.getChildren) {
      child match {
        case LogicOptN(GroupRef(childGroup)) =>
          childGroups += childGroup
        case optimNode =>
          val gExpr = makeGroupExpression(optimNode)
          val outcome: Option[GroupExpression] = memo.insertExpression(gExpr)
          outcome match {
            case None =>
              childGroups += gExpr.groupId
            case Some(mExpr) =>
              childGroups += mExpr.groupId
          }
      }
    }
    GroupExpression(node, childGroups.toVector)()
  }
}
