package com.github.fabianmurariu.g4s.columbia.rules

import com.github.fabianmurariu.g4s.columbia.{
  AnyMatch,
  ExpandPat,
  LogicOptN,
  OptimiserNode,
  Pattern,
  PhysicalOptN,
  Rule
}
import com.github.fabianmurariu.g4s.optim.StatsStore
import com.github.fabianmurariu.g4s.optim.impls.{
  ExpandMul,
  PhysicalGroupRef
}
import com.github.fabianmurariu.g4s.optim.logic.{Expand, LogicGroupRef}

class ExpandImplRule extends Rule {
  override def pattern: Pattern = ExpandPat(AnyMatch, AnyMatch)

  override def id: Int = this.hashCode()

  override def apply(
      node: OptimiserNode,
      ss: StatsStore
  ): Vector[OptimiserNode] = node match {
    case LogicOptN(Expand(LogicGroupRef(frontier), LogicGroupRef(edges))) =>
      Vector(
        PhysicalOptN(
          ExpandMul(PhysicalGroupRef(frontier), PhysicalGroupRef(edges))
        )
      )
  }
}
