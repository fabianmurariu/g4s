package com.github.fabianmurariu.g4s.traverser

import scala.collection.mutable

class LookupTable(table: mutable.Map[(NodeRef, Set[EdgeRef]), PlanStep] = mutable.Map.empty) extends (((NodeRef, Set[EdgeRef])) => PlanStep) {
  def saveBinding(key: (NodeRef, Set[EdgeRef]), plan: PlanStep): PlanStep = {
    table.update(key, plan)
    plan
  }

  def lookup(key: (NodeRef, Set[EdgeRef])): Option[PlanStep] =
    table.get(key)
    
  def lookupOrBuildPlan(key: (NodeRef, Set[EdgeRef]), f: => PlanStep):PlanStep =
    table.getOrElse(key, f)

  override def apply(v1: (NodeRef, Set[EdgeRef])): PlanStep =
    table(v1)
}
