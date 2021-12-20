package com.github.fabianmurariu.g4s.columbia.rules

import com.github.fabianmurariu.g4s.columbia._
import com.github.fabianmurariu.g4s.optim.StatsStore
import com.github.fabianmurariu.g4s.optim.logic.{Expand, Filter}

class FilterExpandEquivRule extends TransRule {
  override def pattern: Pattern =
    FilterPat(ExpandPat(AnyMatch, AnyMatch), AnyMatch)

  override def id: Int = this.hashCode()

  override def apply(
      node: OptimiserNode,
      ss: StatsStore
  ): Vector[OptimiserNode] = node match {
    case LogicOptN(Filter(Expand(front, edges), filter)) =>
      Vector(
        LogicOptN(
          Expand(front, Filter(edges, filter))
        )
      )
  }
}
