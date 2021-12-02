package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.LogicNode
import com.github.fabianmurariu.g4s.optim.impls.Operator

sealed trait OptimiserNode { self =>
  def getChildren: Vector[OptimiserNode] = self match {
    case LogicOptN(node)    => node.children.map(LogicOptN).toVector
    case PhysicalOptN(node) => node.children.map(PhysicalOptN)
  }
}

case class LogicOptN(logic: LogicNode) extends OptimiserNode
case class PhysicalOptN(physical: Operator) extends OptimiserNode
