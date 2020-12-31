package com.github.fabianmurariu.g4s.traverser

import com.github.fabianmurariu.g4s.traverser.Traverser.QGEdges

import scala.collection.mutable

sealed abstract class PlanStep { self =>

  def show: String = self match {
    case LoadNodes(ref) => ref.name
    case Expand(from, name, to, true) =>
      s"(${from.show})<-[:$name]-(${to.show})"
    case Expand(from, name, to, false) =>
      s"(${from.show})-[:$name]->(${to.show})"
    case b @ Bind(_) => b.lookUp.show
    case Union(plans) =>
      plans.map(_.lookUp).map(_.show).mkString("U[", " + ", "]")
  }


  def materialise: PlanStep = self match {
    case b:Bind => b.lookUp
    case Union(plans) =>
      plans
        .iterator
        .map(_.lookUp)
        .map { case e: Expand => e }
        .toVector.reverse
        .reduce { (e1, e2) =>
          e2.copy(to = e1) // simplified version
        }
    case p => p
  }
}

case class LoadNodes(ref: NodeRef) extends PlanStep
case class Bind(key: (NodeRef, Set[EdgeRef]))(table: LookupTable)
    extends PlanStep {
  def lookUp: PlanStep = table(key)
}
case class Expand(
    from: PlanStep,
    edgeType: String,
    to: PlanStep,
    transpose: Boolean
) extends PlanStep

case class Union(plans: Iterable[Bind]) extends PlanStep {
  // this needs testing
}

object LogicalPlan {

  type Key = (NodeRef, Set[EdgeRef])

  def emptyBindings: LookupTable = new LookupTable()

  def compilePlan(
      qg: mutable.Map[NodeRef, QGEdges],
      bindings: LookupTable = new LookupTable(),
      exclude: Set[EdgeRef] = Set.empty
  )(sel: NodeRef): PlanStep = {

    val neighbours = qg.neighbours(sel).toSet
    // get all the subtrees
    val subTrees: Set[((NodeRef, Set[EdgeRef]), Expand)] = neighbours
      .filterNot(exclude)
      .map { edge =>
        val edgePlanKey = (sel, (exclude ++ ((neighbours -- exclude) - edge)))

        val plan = bindings.lookup(edgePlanKey)

        plan
          .collect { case e: Expand => edgePlanKey -> e }
          .getOrElse {

            val edgePlan = edge match {
              case e @ EdgeRef(name, src, dst) if dst == sel =>
                val subPlanKey = (src, Set(e))
                // attempt to get it from cache or recursively compile
                bindings.lookupOrBuildPlan(
                  subPlanKey,
                  compilePlan(qg, bindings, Set(e))(src)
                )

                Expand(
                  Bind(subPlanKey)(bindings),
                  name,
                  LoadNodes(sel),
                  transpose = false
                )

              case e @ EdgeRef(name, src, dst) if src == sel =>
                val subPlanKey = (dst, Set(e))
                bindings.lookupOrBuildPlan(
                  subPlanKey,
                  compilePlan(qg, bindings, Set(e))(dst)
                )

                Expand(
                  Bind(subPlanKey)(bindings),
                  name,
                  LoadNodes(sel),
                  transpose = true
                )
            }

            bindings.saveBinding(edgePlanKey, edgePlan)
            edgePlanKey -> edgePlan
          }

      }

    // compute the plan
    val plan: PlanStep = if (subTrees.isEmpty) {
      LoadNodes(sel)
    } else if (subTrees.size == 1) {
      subTrees.head._2
    } else {
      // combine the subtrees, this step could use costs to estimate the best order
      // save to cache
      // we know the sub plans have been written to the look-up table
      Union(subTrees.map { case (binding, _) => Bind(binding)(bindings) })
    }

    bindings.saveBinding(sel -> exclude, plan)
  }

  def compilePlans(
      qg: mutable.Map[NodeRef, QGEdges],
      table: LookupTable = new LookupTable(),
      exclude: Set[EdgeRef] = Set.empty
  )(sel: Set[NodeRef]): LookupTable = {
    sel.foldLeft(table) { (c, out) =>
      compilePlan(qg, c, exclude)(out)
      c
    }
  }

}
