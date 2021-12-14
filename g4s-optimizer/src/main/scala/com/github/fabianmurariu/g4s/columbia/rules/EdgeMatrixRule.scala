package com.github.fabianmurariu.g4s.columbia.rules

import com.github.fabianmurariu.g4s.columbia.{
  GetEdgesPat,
  LogicOptN,
  OptimiserNode,
  Pattern,
  PhysicalOptN,
  Rule
}
import com.github.fabianmurariu.g4s.optim.{StatsStore, UnNamed}
import com.github.fabianmurariu.g4s.optim.impls.{GetEdgeMatrix, GetNodeMatrix}
import com.github.fabianmurariu.g4s.optim.logic.{GetEdges, GetNodes}

class EdgeMatrixRule extends Rule {
  override def pattern: Pattern = GetEdgesPat

  override def id: Int = this.hashCode()

  override def apply(
      node: OptimiserNode,
      ss: StatsStore
  ): Vector[OptimiserNode] = node match {
    case LogicOptN(GetEdges(tpe, transpose)) =>
      val card = ss.edgesTotal(tpe.headOption)
      Vector(
        PhysicalOptN(
          GetEdgeMatrix(Some(new UnNamed), tpe.headOption, transpose, card)
        )
      )
  }
}
