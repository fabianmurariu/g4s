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
case class Binding2(key: (NodeRef, Set[EdgeRef]))(
    cache: mutable.Map[(NodeRef, Set[EdgeRef]), PlanStep]
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

  type Key = (NodeRef, Set[EdgeRef])
  type Bindings = mutable.Map[Key, PlanStep]

  def emptyBindings: Bindings = mutable.Map.empty

  def compilePlan(
      qg: mutable.Map[NodeRef, QGEdges],
      cache: Bindings = mutable.Map.empty,
      exclude: Set[EdgeRef] = Set.empty
  )(sel: NodeRef): PlanStep = {

    val neighbours = qg.neighbours(sel).toSet
    // get all the subtrees
    val subTrees: Set[((NodeRef, Set[EdgeRef]), Expand)] = neighbours
      .filterNot(exclude)
      .map { edge =>
        val edgePlanKey = (sel, (exclude ++ ((neighbours -- exclude) - edge)))

        val plan = cache
          .get(edgePlanKey)
        
          plan.collect { case e: Expand => edgePlanKey -> e }
          .getOrElse {

            val edgePlan = edge match {
              case e @ EdgeRef(name, src, dst) if dst == sel =>
                val subPlanKey = (src, Set(e))
                // attempt to get it from cache or recursivelly compile
                cache.getOrElse(subPlanKey, compilePlan(qg, cache, Set(e))(src))

                // the key for this subplan is (neighbours - (exclude + e))
                Expand(Binding2(subPlanKey)(cache), name, LoadNodes(sel), transpose = false)

              case e @ EdgeRef(name, src, dst) if src == sel =>
                val subPlanKey = (dst, Set(e))
                cache.getOrElse(subPlanKey, compilePlan(qg, cache, Set(e))(dst))

                Expand(Binding2(subPlanKey)(cache), name, LoadNodes(sel), transpose = true)
            }

            cache += (edgePlanKey -> edgePlan)
            edgePlanKey -> edgePlan
          }

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

//  def attachP1RightOfP2(parentKey: (NodeRef, Set[EdgeRef]), bindings: Bindings)
//                       (p1: ((NodeRef, Set[EdgeRef]), PlanStep), p2: ((NodeRef, Set[EdgeRef]), PlanStep)) = p2 match {
//    case (_, e@Expand(_, _, to:LoadNodes, _)) => (parentKey, e.copy(to = Binding2(p1._1)(bindings)))
//    case (_, e@Expand(_, _, to: Binding2, _)) => (parentKey, )
//  }
  
  def compilePlans(
      qg: mutable.Map[NodeRef, QGEdges],
      cache: Bindings = mutable.Map.empty,
      exclude: Set[EdgeRef] = Set.empty
  )(sel: Set[NodeRef]): Bindings = {
    sel.foldLeft(cache) { (c, out) =>
      compilePlan(qg, c, exclude)(out)
      c
    }
  }

}
