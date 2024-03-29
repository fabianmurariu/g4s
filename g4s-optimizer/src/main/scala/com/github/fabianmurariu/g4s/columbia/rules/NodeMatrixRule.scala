package com.github.fabianmurariu.g4s.columbia.rules

import com.github.fabianmurariu.g4s.columbia._
import com.github.fabianmurariu.g4s.optim.impls.GetNodeMatrix
import com.github.fabianmurariu.g4s.optim.logic.GetNodes
import com.github.fabianmurariu.g4s.optim.{StatsStore, UnNamed}

class NodeMatrixRule extends ImplRule {
  override def pattern: Pattern = GetNodesPat

  override def id: Int = this.hashCode()

  override def apply(
      node: OptimiserNode,
      ss: StatsStore
  ): Vector[OptimiserNode] = node match {
    case LogicOptN(GetNodes(label, sorted)) =>
      val card = ss.nodesTotal(label)
      Vector(
        PhysicalOptN(GetNodeMatrix(sorted.getOrElse(new UnNamed), label, card))
      )
  }
}
