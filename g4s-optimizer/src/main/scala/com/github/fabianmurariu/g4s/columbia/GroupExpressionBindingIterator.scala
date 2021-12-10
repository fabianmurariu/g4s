package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.impls.ForkOperator
import com.github.fabianmurariu.g4s.optim.logic.GroupRef
import com.github.fabianmurariu.g4s.optim.logic.{ForkNode, GroupRef}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks


/**
  *
  *  !!!! this is a direct translation from C++ code, it's awful !!!!
  */

sealed abstract class BindingIterator(memo: Memo)
    extends Iterator[OptimiserNode] {}

case class GroupBindingIterator(memo: Memo, groupId: Int, pattern: Pattern)
    extends BindingIterator(memo) {
  val targetGroup: Group = memo.getGroupById(groupId)
  var currentItemIndex = 0;
  val numGroupItems: Int = targetGroup.logicalExprs.size

  var currentIterator: Option[BindingIterator] = None

  override def hasNext: Boolean = {
    if (pattern == AnyMatch) {
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
    if (pattern == AnyMatch) {
      currentItemIndex = numGroupItems
      LogicOptN(GroupRef(groupId))
    } else {
      currentIterator.map(_.next()).get
    }
  }
}
case class GroupExpressionBindingIterator(
    memo: Memo,
    gExpr: GroupExpression,
    pattern: Pattern
) extends BindingIterator(memo) {

  var first: Boolean = true
  var has_next = false
  var currentBinding: Option[OptimiserNode] = None

  val childGroups: Vector[Int] = gExpr.childGroups
  val childPatterns: Vector[Pattern] = pattern.children

  assert(childGroups.size == childPatterns.size)

  var childrenBindings: ArrayBuffer[ArrayBuffer[OptimiserNode]] =
    ArrayBuffer.fill(childGroups.size)(ArrayBuffer.empty)

  var childrenBindingPos: ArrayBuffer[Int] =
    ArrayBuffer.fill(childGroups.size)(0)

  val children: ArrayBuffer[OptimiserNode] = ArrayBuffer.empty

  for (i <- childGroups.indices) {
    val childBindings = childrenBindings(i)
    val iterator = GroupBindingIterator(memo, childGroups(i), childPatterns(i))
    while (iterator.hasNext) {
      childBindings += iterator.next()
    }

    assert(childBindings.nonEmpty)

    children += childBindings(0)
  }

  has_next = true
  currentBinding = rewireBindings(gExpr.node, children)

  def rewireBindings(node:OptimiserNode, children:Iterable[OptimiserNode]): Option[OptimiserNode] = {
    node match {
      case LogicOptN(logic: ForkNode) =>
        Some(
          LogicOptN(
            logic.rewireV2(
              children
                .map(_.content)
                .collect { case Left(logic) => logic }
                .toVector
            )
          )
        )
      case PhysicalOptN(operator: ForkOperator) =>
        Some(
          PhysicalOptN(
            operator.rewrite(
              children.map(_.content).collect { case Right(op) => op }.toVector
            )
          )
        )
    }
  }

  override def hasNext: Boolean = {
    if (has_next && first) {
      first = false
      return true
    }

    if (has_next) {
      var firstModifiedIdx = childrenBindingPos.size - 1
      val breaks = new Breaks
      import breaks.{breakable, break}
      breakable{
        while (firstModifiedIdx >= 0) {
          val childBinding = childrenBindings(firstModifiedIdx)
          var newPos = {
            childrenBindingPos(firstModifiedIdx)+=1
            childrenBindingPos(firstModifiedIdx)
          }

          if (newPos >= childBinding.size){
            childrenBindingPos(firstModifiedIdx) = 0;
          } else {
            break
          }
          firstModifiedIdx -= 1
        }
      }

      if (firstModifiedIdx < 0 ){
        has_next = false
      } else {
        val children = ArrayBuffer.empty[OptimiserNode]
        for (i <- childrenBindingPos.indices){
          val childBinding = childrenBindings(i)
          children += childBinding(childrenBindingPos(i))
          currentBinding = rewireBindings(gExpr.node, children)
        }
      }
    }

    has_next
  }

  override def next(): OptimiserNode = currentBinding.get
}
