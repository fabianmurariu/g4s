package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.logic.{Expand, Filter, GetEdges, GetNodes, Join}

object StatsCalculator {

  def calculateForGetEdges(
      logic: GetEdges,
      group: Group,
      ctx: Context
  ): Unit = {
    val label = logic.tpe.headOption
    val numEdges = ctx.stats.edgesTotal(label)
    val edgesSel = ctx.stats.edgeSel(label)
    group.setCardinality(numEdges)
    group.setSelectivity(edgesSel)
  }

  def calculateForExpand(
      gExpr: GroupExpression,
      logic: Expand,
      group: Group,
      ctx: Context
  ): Unit = {
    val leftGroup = ctx.memo.getGroupById(gExpr.childGroups(0))
    val rightGroup = ctx.memo.getGroupById(gExpr.childGroups(1))

    val card = for {
      leftCard <- leftGroup.estNumRows
      rightCard <- rightGroup.estNumRows
      sel <- rightGroup.estSelectivity
    } yield Math.max(leftCard * rightCard * sel, 1.0d).toLong

    group.setCardinality(card.get)
  }

  def calculateForFilter(
      gExpr: GroupExpression,
      logic: Filter,
      group: Group,
      ctx: Context
  ): Unit = {
    val leftGroup = ctx.memo.getGroupById(gExpr.childGroups(0))
    val rightGroup = ctx.memo.getGroupById(gExpr.childGroups(1))

    val card = for {
      leftCard <- leftGroup.estNumRows
      rightCard <- rightGroup.estNumRows
      sel <- rightGroup.estSelectivity
      // filter cannot increase the cardinality of the output
      // it can only remove columns
      // so it can be max(l_card * r_card * sel, l_card)
    } yield Math
      .max(Math.max(leftCard * rightCard * sel, leftCard), 1.0d)
      .toLong

    group.setCardinality(card.get)
  }

  def calculateForJoin(gExpr: GroupExpression, logic: Join, group: Group, ctx: Context): Unit = {

    val leftGroup = ctx.memo.getGroupById(gExpr.childGroups(0))
    val rightGroup = ctx.memo.getGroupById(gExpr.childGroups(1))

    val card = for {
      leftCard <- leftGroup.estNumRows
      rightCard <- rightGroup.estNumRows.map(x => Math.sqrt(x.toDouble))
      sel <- rightGroup.estSelectivity
    } yield {
      // this is implemented as a Filter(left, Diag(right))
      Math
        .max(Math.max(leftCard * rightCard * sel, leftCard), 1.0d)
        .toLong
    }

    group.setCardinality(card.get)
  }

  def calculateForNode(
      gExpr: GroupExpression,
      group: Group,
      ctx: Context
  ): Unit =
    gExpr.node match {
      case LogicOptN(logic: GetNodes) =>
        calculateForGetNodes(logic, group, ctx)
      case LogicOptN(logic: GetEdges) =>
        calculateForGetEdges(logic, group, ctx)
      case LogicOptN(logic: Expand) =>
        calculateForExpand(gExpr, logic, group, ctx)
      case LogicOptN(logic: Filter) =>
        calculateForFilter(gExpr, logic, group, ctx)
      case LogicOptN(logic: Join) =>
        calculateForJoin(gExpr, logic, group, ctx)
      case _ => throw DeriveStatsError
    }

  def calculateForGetNodes(
      logic: GetNodes,
      group: Group,
      ctx: Context
  ): Unit = {
    val numNodes = ctx.stats.nodesTotal(logic.label)
    val nodesSel = ctx.stats.nodeSel(logic.label)
    group.setCardinality(numNodes)
    group.setSelectivity(nodesSel)
  }

  def calculateStats(groupExpression: GroupExpression, ctx: Context): Unit = {
    val rootGroup = ctx.memo.getGroupById(groupExpression.groupId)

    if (!rootGroup.isEstimated) {
      calculateForNode(groupExpression, rootGroup, ctx)
    }
  }

}
