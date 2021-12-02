package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.{LogicNode, QueryGraph}
import com.github.fabianmurariu.g4s.optim.impls.Operator

case class Optimiser (context: Context) {

  def chooseBestPlan(rootPlan: LogicNode, qg: QueryGraph): Operator = {
    val optimNode = LogicOptN(rootPlan)
    val memo = new Memo()
    val context = new Context(memo)
    ???
  }

}
