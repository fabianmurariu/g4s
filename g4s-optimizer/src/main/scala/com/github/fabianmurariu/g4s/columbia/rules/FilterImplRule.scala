package com.github.fabianmurariu.g4s.columbia.rules

import com.github.fabianmurariu.g4s.columbia._
import com.github.fabianmurariu.g4s.optim.StatsStore
import com.github.fabianmurariu.g4s.optim.impls.{FilterMul, PhysicalGroupRef}
import com.github.fabianmurariu.g4s.optim.logic.{Filter, LogicGroupRef}

class FilterImplRule extends ImplRule {
  override def pattern: Pattern = FilterPat(AnyMatch, AnyMatch)

  override def id: Int = this.hashCode()

  override def apply(
      node: OptimiserNode,
      ss: StatsStore
  ): Vector[OptimiserNode] = node match {
    case LogicOptN(Filter(LogicGroupRef(left), LogicGroupRef(right))) =>
      Vector(
        PhysicalOptN(
          FilterMul(PhysicalGroupRef(left), PhysicalGroupRef(right), 1.0)
        )
      )
  }
}
