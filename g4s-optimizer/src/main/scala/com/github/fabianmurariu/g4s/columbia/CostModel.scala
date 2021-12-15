package com.github.fabianmurariu.g4s.columbia

trait CostModel {

  def calculateCost(groupExpression: GroupExpression, ctx: Context): Double = {
    val group = ctx.memo.getGroupById(groupExpression.groupId)
    group.getCardinality.map(_ * 1.2d).get
  }

}
