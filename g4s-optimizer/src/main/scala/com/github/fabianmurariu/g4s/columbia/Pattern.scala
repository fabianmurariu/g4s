package com.github.fabianmurariu.g4s.columbia

sealed trait Pattern {
  def matchLevel(gExpr: GroupExpression, i: Int): Boolean

  def isMatch(node: OptimiserNode): Boolean

  def children: Vector[Pattern]
}

case class AnyMatch() extends Pattern {
  def isMatch(node: OptimiserNode): Boolean = true

  def children: Vector[Pattern] = Vector.empty

  def matchLevel(gExpr: GroupExpression, i: Int): Boolean = true
}
