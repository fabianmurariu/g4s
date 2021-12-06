package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.GroupRef

import scala.util.control.Breaks

sealed abstract class BindingIterator(memo: Memo)
    extends Iterator[OptimiserNode] {}

case class GroupBindingIterator(memo: Memo, groupId: Int, pattern: Pattern)
    extends BindingIterator(memo) {
  val targetGroup: Group = memo.getGroupById(groupId)
  var currentItemIndex = 0;
  val numGroupItems: Int = targetGroup.logicalExprs.size

  var currentIterator: Option[BindingIterator] = None

  override def hasNext: Boolean = {
    if (pattern.isInstanceOf[AnyMatch]) {
      return currentItemIndex == 0
    }

    // currentIterator is now empty
    currentIterator match {
      case Some(iter) if !iter.hasNext =>
        // reset
        currentItemIndex += 1
        currentIterator = None
      case _ => ()
    }

    // direct translation from c++
    if (currentIterator.isEmpty) {
      val breaks = new Breaks
      import breaks.{break, breakable}
      breakable {
        while (currentItemIndex < numGroupItems) {
          val gExpr = targetGroup.logicalExprs(currentItemIndex)
          val gExprIt = GroupExpressionBindingIterator(memo, gExpr, pattern)
          currentIterator = Some(gExprIt)
          if (currentIterator.exists(_.hasNext)) {
            break
          }

          currentIterator = None
          currentItemIndex += 1
        }
      }
    }

    currentIterator.isDefined

  }

  override def next(): OptimiserNode = {
    if (pattern.isInstanceOf[AnyMatch]) {
      currentItemIndex = numGroupItems
      LogicOptN(GroupRef(groupId))
    } else {
      currentIterator.map(_.next()).get
    }
  }
}
case class GroupExpressionBindingIterator(
    memo: Memo,
    expr: GroupExpression,
    pattern: Pattern
) extends BindingIterator(memo) {
  override def hasNext: Boolean = ???

  override def next(): OptimiserNode = ???
}
