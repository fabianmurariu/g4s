package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.columbia.rules.{
  EdgeMatrixRule,
  ExpandImplRule,
  FilterExpandEquivRule,
  FilterImplRule,
  NodeMatrixRule
}
import com.github.fabianmurariu.g4s.optim.{QueryGraph, StatsStore}
import com.github.fabianmurariu.g4s.optim.impls.{ForkOperator, Operator}
import com.github.fabianmurariu.g4s.optim.logic.LogicNode

class Optimiser {

  def groupExpressionToOperator(ctx: Context, ge: GroupExpression): Operator =
    ge match {
      case GroupExpression(
          PhysicalOptN(operator: ForkOperator),
          childGroups,
          _,
          _,
          _
          ) =>
        val children = childGroups
          .map(ctx.memo.getGroupById)
          .flatMap(_.bestExpression)
          .map { case (expr, _) => groupExpressionToOperator(ctx, expr) }
        operator.rewrite(children)
      case GroupExpression(PhysicalOptN(operator), _, _, _, _) =>
        operator
    }

  def bestPlanToOperator(
      ctx: Context,
      rootGroupId: Int
  ): Either[OptimiserError, Operator] = {
    val rootGroup = ctx.memo.getGroupById(rootGroupId)

    rootGroup.bestExpression
      .map { case (expr, _) => Right(groupExpressionToOperator(ctx, expr)) }
      .getOrElse(Left(FailedToOptimisePlan))
  }

  def renderExpression(
      ge: GroupExpression,
      memo: Memo,
      prefix: String = "",
      level: Int = 0,
  ): Either[OptimiserError, String] = {

    ge.node match {
      case PhysicalOptN(op) =>
        val name = op.getClass.getSimpleName
        val group = memo.getGroupById(ge.groupId)
        val card = group.estNumRows.getOrElse(-1L)
        val sel = group.estSelectivity.getOrElse(1.0d)
        val text = {
          val prepend = new String(Array.fill(level)(' '))
          s"$prepend$prefix$name cost=${ge.bestCost.getOrElse(-1d)} card=${card} sel=${sel}"
        }

        val children = ge.childGroups
          .map(memo.getGroupById)
          .flatMap(_.bestExpression)
          .map(_._1)

        val outText = children.zipWithIndex
          .map {
            case (childExpr, i) =>
              renderExpression(childExpr, memo, s"$i:", level + 1)
          }
          .map(_.map(Vector(_)))
          .reduceOption((a, b) =>
            for {
              l <- a
              r <- b
            } yield l ++ r
          )
          .getOrElse(Right(Vector.empty)) // no children
          .map((cText: Vector[String]) => (text +: cText).mkString("\n"))

        outText
      case _ => Left(RenderPlan)
    }

  }

  def optimiserLoop(
      ctx: Context,
      rootGroupId: Int
  ): Either[OptimiserError, GroupExpression] = {
    while (!ctx.isEmpty) {
      ctx.pop().foreach { task => task.perform() }
    }

    val rootGroup = ctx.memo.getGroupById(rootGroupId)

    rootGroup.bestExpression match {
      case Some((expr, _)) => Right(expr)
      case _               => Left(FailedToOptimisePlan)
    }
  }

  def chooseBestPlan(
      rootPlan: LogicNode,
      ss: StatsStore
  ): Either[OptimiserError, (GroupExpression, Context, Operator)] = {
    val optimNode = LogicOptN(rootPlan)
    val memo = Memo()
    val context = Context(
      memo,
      ss,
      new CostModel {},
      implementationRules = Vector(
        new NodeMatrixRule,
        new EdgeMatrixRule,
        new ExpandImplRule,
        new FilterImplRule
      ),
      transformationRules = Vector(
        new FilterExpandEquivRule
      )
    )
    val rootExpr = context.recordOptimiserNodeIntoGroup(optimNode)

    val out: Either[OptimiserError, GroupExpression] = rootExpr match {
      case None => Left(FailedToRecordOptimiserNode)
      case Some(expr) =>
        val context1 = new OptimisationContext(context)
        context.push(
          new OptimiseGroup(
            memo.getGroupById(expr.groupId),
            context1
          )
        )
        context.push(new DeriveStats(expr, context1))
        optimiserLoop(context, expr.groupId)
    }

    out.map(groupExpr =>
      (groupExpr, context, groupExpressionToOperator(context, groupExpr))
    )

  }

}

sealed abstract class OptimiserError(msg: String) extends RuntimeException(msg)

case object FailedToRecordOptimiserNode
    extends OptimiserError(
      "failed to record logic node into optimiser, time to panic!"
    )

case object FailedToOptimisePlan
    extends OptimiserError("Failed to optimise logic node")

case object DeriveStatsError
    extends OptimiserError("Unable to derive stats for physical plan")

case object RenderPlan extends OptimiserError("Unable to render plan")
