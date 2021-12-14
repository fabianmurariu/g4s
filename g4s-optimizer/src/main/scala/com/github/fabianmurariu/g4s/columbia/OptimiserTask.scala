package com.github.fabianmurariu.g4s.columbia

sealed trait OptimiserTask extends (() => Unit) {

  def optContext: OptimisationContext

  def exploreChildrenGroups(
      expr: GroupExpression,
      ctx: Context,
      rule: Rule
  ): Int = {
    rule.pattern.children.foldLeft(0) { (i, child) =>
      if (child.children.nonEmpty) {
        val group = ctx.memo.getGroupById(expr.childGroups(i))
        ctx.push(new ExploreGroup(group, optContext))
      }
      i + 1
    }
  }
  def constructValidRules(
      gExpr: GroupExpression,
      rules: Vector[Rule]
  ): Vector[Rule] = {

    rules
      .foldLeft(Vector.newBuilder[Rule]) { (b, rule) =>
        val missMatch: Boolean = !rule.pattern.matchLevel(gExpr, 2)
        val alreadyExplored: Boolean = gExpr.hasRuleExplored(rule)

        if (missMatch || alreadyExplored)
          b
        else
          b += rule
      }
      .result()
  }
}

class OptimiseGroup(group: Group, val optContext: OptimisationContext)
    extends OptimiserTask {
  override def apply(): Unit = {
    if (group.getCostLB > optContext.getCostUpperBound) return

    if (group.bestExpression.isEmpty /* TODO: add check for lower bound */ ) {
      if (!group.isExplored) {
        group.logicalExprs.foldLeft(optContext.getContext) { (context, expr) =>
          context.push(new OptimiseExpression(expr, optContext))
        }

        group.physicalExprs.foldLeft(optContext.getContext) { (context, expr) =>
          context.push(new OptimiseExpressionCost(expr, optContext))
        }
        group.setExplored()
      }
    }
  }
}

class OptimiseExpression(
    expr: GroupExpression,
    val optContext: OptimisationContext
) extends OptimiserTask {
  override def apply(): Unit = {
    val ctx = optContext.getContext
    val transform = constructValidRules(expr, ctx.transformationRules)
    val implement = constructValidRules(expr, ctx.implementationRules)

    val all = transform ++ implement

    all.foreach { rule =>
      ctx.push(new ApplyRule(rule, expr, optContext))
      exploreChildrenGroups(expr, ctx, rule)
    }
  }

}

class ExploreGroup(group: Group, val optContext: OptimisationContext)
    extends OptimiserTask {
  override def apply(): Unit = {
    val ctx = optContext.getContext
    if (!group.isExplored) {
      group.logicalExprs.foreach { expr =>
        ctx.push(new ExploreExpression(expr, optContext))
      }
      group.setExplored()
    }
  }
}

class ExploreExpression(
    expr: GroupExpression,
    val optContext: OptimisationContext
) extends OptimiserTask {
  override def apply(): Unit = {
    val ctx = optContext.getContext
    val transform = constructValidRules(expr, ctx.transformationRules)
    transform.foreach { rule =>
      ctx.push(new ApplyRule(rule, expr, optContext, true))
      exploreChildrenGroups(expr, ctx, rule)
    }
  }
}

class OptimiseExpressionCost(
    expr: GroupExpression,
    val optContext: OptimisationContext,
    var curChildIndex: Int = -1,
    var prevChildIndex: Int = -1,
    var curTotalCost: Double = 0d
) extends OptimiserTask {
  import scala.util.control.Breaks

  override def apply(): Unit = {
    val ctx = optContext.getContext
    if (curChildIndex == -1) {
      curTotalCost = 0d
      if (curTotalCost > optContext.getCostUpperBound) return
      else {
        curChildIndex = 0

        curTotalCost += ctx.costModel.calculateCost(expr, ctx)

        val breaks = new Breaks
        import breaks.{breakable, break}
        breakable {
          while (curChildIndex < expr.childGroups.size) {
            val childGroup =
              ctx.memo.getGroupById(expr.childGroups(curChildIndex))

            childGroup.bestExpression match {
              case Some((_, cost)) =>
                curTotalCost += cost
                if (curTotalCost > optContext.getCostUpperBound) break
              case None if prevChildIndex != curChildIndex =>
                prevChildIndex = curChildIndex
                ctx.push(this)
                val costHigh = optContext.getCostUpperBound - curTotalCost
                val optCtx = new OptimisationContext(ctx, costHigh)
                ctx.push(new OptimiseGroup(childGroup, optCtx))
              case None =>
                break
            }
            curChildIndex += 1
          }
        }
        if (curChildIndex == expr.childGroups.size) {
          expr.updateBestCost(curTotalCost)
          val curGroup = ctx.memo.getGroupById(expr.groupId)
          curGroup.setExpressionCost(expr, curTotalCost)
        }

        if (curTotalCost  <= optContext.getCostUpperBound) {
          optContext.setCostUpperBound(optContext.getCostUpperBound - curTotalCost)
          val curGroup = ctx.memo.getGroupById(expr.groupId)
          curGroup.setExpressionCost(expr, curTotalCost)
        }

        prevChildIndex = -1
        curChildIndex = 0
        curTotalCost = 0d
      }
    }
  }
}

class ApplyRule(
    rule: Rule,
    groupExpr: GroupExpression,
    val optContext: OptimisationContext,
    exploreOnly: Boolean = false
) extends OptimiserTask {
  override def apply(): Unit = {
    val ctx = optContext.getContext
    if (!groupExpr.hasRuleExplored(rule)) {
      val iterator =
        GroupExpressionBindingIterator(ctx.memo, groupExpr, rule.pattern)

      while (iterator.hasNext) {
        val gId = groupExpr.groupId
        val node = iterator.next()
        // we just assume the rule applies
        val newExpressions = rule(node, ctx.stats)
        for (newExpr <- newExpressions) {

          ctx.recordOptimiserNodeIntoGroup(newExpr, gId).foreach {
            newGroupExpr =>
              ctx.push(new DeriveStats(newGroupExpr, optContext))
              if (newGroupExpr.isLogical) {
                if (exploreOnly)
                  ctx.push(new ExploreExpression(newGroupExpr, optContext))
                else
                  ctx.push(new OptimiseExpression(newGroupExpr, optContext))
              } else {
                ctx.push(new OptimiseExpressionCost(newGroupExpr, optContext))
              }

          }
        }
      }

      groupExpr.setExplored(rule)
    }
  }
}

class DeriveStats(
    gExpr: GroupExpression,
    val optContext: OptimisationContext,
    var childrenDerived: Boolean = false
) extends OptimiserTask {

  override def apply(): Unit = {
    val ctx = optContext.getContext
    if (!childrenDerived) {
      childrenDerived = true
      ctx.push(this)
      for (childGroupId <- gExpr.childGroups) {
        // TODO we currently pick the first expr, we should pick the one with the highest confidence
        val childGroupExpr =
          ctx.memo.getGroupById(childGroupId).logicalExprs.head
        ctx.push(new DeriveStats(childGroupExpr, optContext))
      }
    } else {
      StatsCalculator.calculateStats(gExpr, ctx)
      gExpr.setStatsDerived()
    }
  }
}
