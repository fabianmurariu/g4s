package com.github.fabianmurariu.g4s.columbia

trait Rule extends (OptimiserNode => Vector[OptimiserNode]) {

  def pattern: Pattern

  def id:Int
}
