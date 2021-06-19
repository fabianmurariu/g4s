package com.github.fabianmurariu.g4s.optim

object Optimizer {

  private def initMemo(qg: QueryGraph): Memo = {
    val rootPlans = qg.returns.map(LogicNode.fromQueryGraph(qg)).collect{case Right(p) => p}
    val memo = new Memo(rootPlans)
    rootPlans.foldLeft(memo) {
      case (memo, logicPlan) =>
        memo.doEnqueuePlan(logicPlan)
        memo
    }

    memo
  }

  // def optimize(qg: QueryGraph, rules: Vector[Rule]): Memo = {
  //     val memo = initMemo(qg)
  //     val root = memo.rootPlans.head
  //     def expandGroup()
  //     for (rule <- rules) {

  //         if (rule.isDefinedAt()) {}
  //     }
  //     memo
  // }

}

sealed trait Rule extends PartialFunction[GroupMember, Seq[GroupMember]] {

  override def apply(v1: GroupMember): Seq[GroupMember] = Seq.empty

  override def isDefinedAt(x: GroupMember): Boolean = false

}

trait ImplementationRule extends Rule
trait TransformationRule extends Rule
