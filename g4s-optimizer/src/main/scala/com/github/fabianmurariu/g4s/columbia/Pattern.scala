package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.logic.{Expand, Filter, GetEdges, GetNodes}

sealed trait Pattern {
  def matchLevel(gExpr: GroupExpression, i: Int): Boolean

  def isMatch(node: OptimiserNode): Boolean

  def children: Vector[Pattern]
}

case object AnyMatch extends Pattern {
  def isMatch(node: OptimiserNode): Boolean = true

  def children: Vector[Pattern] = Vector.empty

  def matchLevel(gExpr: GroupExpression, i: Int): Boolean = true
}

case class ExpandPat(left: Pattern, right: Pattern) extends Pattern {
  override def matchLevel(gExpr: GroupExpression, i: Int): Boolean = ???

  override def isMatch(node: OptimiserNode): Boolean = node.content match {
    case Left(Expand(_, _)) => true
    case _                  => false
  }

  override def children: Vector[Pattern] = Vector(left, right)
}

case class FilterPat(left: Pattern, right: Pattern) extends Pattern {
  override def matchLevel(gExpr: GroupExpression, i: Int): Boolean = ???

  override def isMatch(node: OptimiserNode): Boolean = node.content match {
    case Left(Filter(_, _)) => true
    case _                  => false
  }

  override def children: Vector[Pattern] = Vector(left, right)
}

case object GetEdgesPat extends Pattern {
  override def matchLevel(gExpr: GroupExpression, i: Int): Boolean = ???

  override def isMatch(node: OptimiserNode): Boolean = node.content match {
    case Left(_: GetEdges) => true
    case _                 => false
  }

  override def children: Vector[Pattern] = Vector.empty
}

case object GetNodesPat extends Pattern {
  override def matchLevel(gExpr: GroupExpression, i: Int): Boolean = ???

  override def isMatch(node: OptimiserNode): Boolean = node.content match {
    case Left(_: GetNodes) => true
    case _                 => false
  }

  override def children: Vector[Pattern] = Vector.empty
}
