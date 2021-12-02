package com.github.fabianmurariu.g4s.columbia

sealed trait Pattern {
  def isMatch(node: OptimiserNode): Boolean
}

case class AnyMatch() extends Pattern {
  def isMatch(node: OptimiserNode): Boolean = true
}
