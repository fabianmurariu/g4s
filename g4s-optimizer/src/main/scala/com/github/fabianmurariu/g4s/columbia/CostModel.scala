package com.github.fabianmurariu.g4s.columbia

trait CostModel {

  def calculateCost(groupExpression: GroupExpression, ctx: Context): Double = {
    val group = ctx.memo.getGroupById(groupExpression.groupId)
    if (groupExpression.childGroups.nonEmpty ) {
      group.getCardinality.map(_ * 1.2d).get
    } else {
      0.0d // for base tables we have a 0 cost, we're loading them anyways
    }
  }

}

