package com.github.fabianmurariu.g4s.columbia

sealed trait OptimiserTask extends (Context => Unit) {
  def exploreChildrenGroups(
      expr: GroupExpression,
      ctx: Context,
      rule: Rule
  ): Int = {
    rule.pattern.children.foldLeft(0) { (i, child) =>
      if (child.children.nonEmpty) {
        val group = ctx.memo.getGroupById(expr.childGroups(i))
        ctx.push(new ExploreGroup(group))
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

class OptimiseGroup(group: Group) extends OptimiserTask {
  override def apply(ctx: Context): Unit = {
    if (group.bestExpression.isEmpty /* TODO: add check for lower bound */ ) {
      if (!group.isExplored) {
        group.logicalExprs.foldLeft(ctx) { (context, expr) =>
          context.push(new OptimiseExpression(expr))
        }

        group.physicalExprs.foldLeft(ctx) { (context, expr) =>
          context.push(new OptimiseExpressionCost(expr))
        }
        group.setExplored()
      }
    }
  }
}

class OptimiseExpression(expr: GroupExpression) extends OptimiserTask {
  override def apply(ctx: Context): Unit = {
    val transform = constructValidRules(expr, ctx.transformationRules)
    val implement = constructValidRules(expr, ctx.implementationRules)

    val all = transform ++ implement

    all.foreach { rule =>
      ctx.push(new ApplyRule(rule, expr))
      exploreChildrenGroups(expr, ctx, rule)
    }
  }

}

class ExploreGroup(group: Group) extends OptimiserTask {
  override def apply(ctx: Context): Unit = {
    if (!group.isExplored) {
      group.logicalExprs.foreach { expr =>
        ctx.push(new ExploreExpression(expr))
      }
      group.setExplored()
    }
  }
}

class ExploreExpression(expr: GroupExpression) extends OptimiserTask {
  override def apply(ctx: Context): Unit = {
    val transform = constructValidRules(expr, ctx.transformationRules)
    transform.foreach { rule =>
      ctx.push(new ApplyRule(rule, expr, true))
      exploreChildrenGroups(expr, ctx, rule)
    }
  }
}

class OptimiseExpressionCost(expr: GroupExpression) extends OptimiserTask {
  override def apply(ctx: Context): Unit = ???
}

class ApplyRule(
    rule: Rule,
    groupExpr: GroupExpression,
    exploreOnly: Boolean = false
) extends OptimiserTask {
  override def apply(ctx: Context): Unit = {
    if (!groupExpr.hasRuleExplored(rule)) {
      val iterator =
        GroupExpressionBindingIterator(ctx.memo, groupExpr, rule.pattern)

      while (iterator.hasNext) {
        val gId = groupExpr.groupId
        val node = iterator.next()
        // we just assume the rule applies
        val newExpressions = rule(node)
        for (newExpr <- newExpressions) {

          ctx.recordOptimiserNodeIntoGroup(newExpr, gId).foreach {
            newGroupExpr =>
              ctx.push(new DeriveStats(newGroupExpr))
              if (newGroupExpr.isLogical) {
                if (exploreOnly)
                  ctx.push(new ExploreExpression(newGroupExpr))
                else
                  ctx.push(new OptimiseExpression(newGroupExpr))
              } else {
                ctx.push(new OptimiseExpressionCost(newGroupExpr))
              }

          }
        }
      }

      groupExpr.setExplored(rule)
    }
  }
}

class DeriveStats(gExpr: GroupExpression) extends OptimiserTask {
  private var childrenDerived:Boolean = false

  override def apply(ctx: Context): Unit = {
    if (!childrenDerived) {
      childrenDerived = true
      for (childGroupId <- gExpr.childGroups) {
        val childGroupExpr = ctx.memo.getGroupById(childGroupId).logicalExprs.head
        ctx.push(new DeriveStats(childGroupExpr))
      }
    } else {
      StatsCalculator.calculateStats(gExpr, ctx)
      gExpr.setStatsDerived()
    }
  }
}
