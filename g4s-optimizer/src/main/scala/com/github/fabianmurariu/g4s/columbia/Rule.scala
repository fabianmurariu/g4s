package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.StatsStore

trait Rule extends ((OptimiserNode, StatsStore) => Vector[OptimiserNode]) {

  def pattern: Pattern

  def id: Int
}

trait TransRule extends Rule
trait ImplRule extends Rule
