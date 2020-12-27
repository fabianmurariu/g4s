package com.github.fabianmurariu.g4s.traverser

import scala.collection.mutable
import com.github.fabianmurariu.g4s.traverser.Traverser.QGEdges

sealed abstract class PlanStep { self =>

  def show: String = self match {
    case LoadNodes(ref) => ref.name
    case Expand(from, name, to, true) =>
      s"(${from.show})<-[:$name]-(${to.show})"
    case Expand(from, name, to, false) =>
      s"(${from.show})-[:$name]->(${to.show})"
    case b @ Binding2(_) => b.lookUp.show
  }
}

case class LoadNodes(ref: NodeRef) extends PlanStep
case class Binding2(key: (NodeRef, Option[EdgeRef]))(
    cache: mutable.Map[(NodeRef, Option[EdgeRef]), PlanStep]
) extends PlanStep {
  def lookUp: PlanStep = cache(key)

}
case class Expand(
    from: PlanStep,
    edgeType: String,
    to: PlanStep,
    transpose: Boolean
) extends PlanStep

class LogicalPlan(
    val planTable: mutable.Map[Map[EdgeRef, Boolean], PlanStep]
) {

  def lookupBinding(key: Map[EdgeRef, Boolean]): PlanStep =
    planTable(key)

}

object LogicalPlan {

  type Key = (NodeRef, Option[EdgeRef])
  type Bindings = mutable.Map[Key, PlanStep]

  def emptyBindings: Bindings = mutable.Map.empty

  def compilePlan(
      qg: mutable.Map[NodeRef, QGEdges],
      cache: Bindings = mutable.Map.empty,
      exclude: Option[EdgeRef] = None
  )(sel: NodeRef): PlanStep = {

    // get all the subtrees
    val subTrees: Iterable[Expand] = qg
      .neighbours(sel)
      .filterNot(e => exclude.contains(e))
      .map {
        case e @ EdgeRef(name, src, dst) if (dst == sel) =>
          val key = (src, Option(e))
          // attempt to get it from cache or recursivelly compile
          cache
            .get(key) match {
              case None    =>
                val p = compilePlan(qg, cache, Option(e))(src)
                // println(s"Computed ${p.show} for $sel on $key")
            case Some(p) =>
                // println(s"Found ${p.show} for $sel on $key")
                p
          }

          Expand(Binding2(key)(cache), name, LoadNodes(sel), false)
        case e @ EdgeRef(name, src, dst) if (src == sel) =>
          val key = (dst, Option(e))
          cache
            .get(key) match {
              case None    =>
                val p = compilePlan(qg, cache, Option(e))(dst)
                // println(s"Computed ${p.show} for $sel on $key")
              case Some(p) =>
                // println(s"Found ${p.show} for $sel on $key")
                p
          }

          Expand(Binding2(key)(cache), name, LoadNodes(sel), true)
      }

    // compute the plan
    val plan: PlanStep = if (subTrees.isEmpty) {
      // one lonley node
      LoadNodes(sel)
    } else {
      // combine the subtrees, this step could use costs to estimate the best order
      subTrees.reduce { (e1, e2) => e1.copy(to = e2) }
    }

    // save to cache
    cache += ((sel, exclude) -> plan)

    plan
  }

  def compilePlans(
      qg: mutable.Map[NodeRef, QGEdges],
      cache: mutable.Map[(NodeRef, Option[EdgeRef]), PlanStep] =
        mutable.Map.empty,
      exclude: Option[EdgeRef] = None
  )(sel: Set[NodeRef]): mutable.Map[(NodeRef, Option[EdgeRef]), PlanStep] = {
    sel.foldLeft(cache) { (c, out) =>
      compilePlan(qg, c, exclude)(out)
      c
    }
  }

}
