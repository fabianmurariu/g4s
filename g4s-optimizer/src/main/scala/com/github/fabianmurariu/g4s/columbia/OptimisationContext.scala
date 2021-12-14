package com.github.fabianmurariu.g4s.columbia

class OptimisationContext(
    ctx: Context,
    private var costUpperBound: Double = Double.MaxValue
) {
  def getCostUpperBound: Double =
    costUpperBound

  def getContext: Context =
    ctx

  def setCostUpperBound(cost: Double): Unit =
    costUpperBound = cost
}
