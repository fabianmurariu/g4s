package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.{LogicNode, QueryGraph}
import com.github.fabianmurariu.g4s.optim.impls.Operator

class Optimiser {

  def bestPlanToOperator(
      ctx: Context,
      rootGroupId: Int
  ): Either[OptimiserError, Operator] = {
    val rootGroup = ctx.memo.getGroupById(rootGroupId)
    rootGroup.bestExpression
      .collect {
        case GroupExpression(PhysicalOptN(operator), _, _) =>
          Right(operator)
      }
      .getOrElse(Left(FailedToOptimisePlan))
  }

  def optimiserLoop(
      ctx: Context,
      rootGroupId: Int
  ): Either[OptimiserError, Operator] = {
    while (!ctx.isEmpty) {
      ctx.pop().foreach(_.apply(ctx))
    }
    bestPlanToOperator(ctx, rootGroupId)
  }

  def chooseBestPlan(
      rootPlan: LogicNode,
      qg: QueryGraph
  ): Either[OptimiserError, Operator] = {
    val optimNode = LogicOptN(rootPlan)
    val memo = new Memo()
    val context = new Context(memo)
    val rootExpr = context.recordOptimiserNodeIntoGroup(optimNode)

    rootExpr match {
      case None => Left(FailedToRecordOptimiserNode)
      case Some(expr) =>
        context.push(new OptimiseGroup(memo.getGroupById(expr.groupId)))
        optimiserLoop(context, expr.groupId)
    }
  }

}

sealed abstract class OptimiserError(msg: String) extends RuntimeException(msg)

case object FailedToRecordOptimiserNode
    extends OptimiserError(
      "failed to record logic node into optimiser, time to panic!"
    )
case object FailedToOptimisePlan
    extends OptimiserError("Failed to optimise logic node")
