package com.github.fabianmurariu.g4s.traverser

import com.github.fabianmurariu.g4s.traverser.Traverser.QGEdges

import scala.collection.mutable
import cats.Id

sealed abstract class PlanStep { self =>

  def show: String = self match {
    case LoadNodes(ref) => ref.name
    case Expand(from, name, to, true) =>
      s"(${from.lookup.show})<-[:$name]-(${to.show})"
    case Expand(from, name, to, false) =>
      s"(${from.lookup.show})-[:$name]->(${to.show})"
    case Union(plans) =>
      plans.map(_.lookup).map(_.show).mkString("U[", " + ", "]")
  }

  def materialise: PlanStep = self match {
    case Union(plans) =>
      plans.iterator
        .map(_.lookup)
        .map { case e: Expand => e }
        .toVector
        .reverse
        .reduce { (e1, e2) =>
          e2.copy(to = e1) // simplified version
        }
    case p => p
  }
}

case class LoadNodes(ref: NodeRef) extends PlanStep


case class Expand(
    from: Bind[Id, PlanStep],
    edgeType: String,
    to: PlanStep,
    transpose: Boolean
) extends PlanStep

case class Union(plans: Iterable[Bind[Id, PlanStep]]) extends PlanStep

object LogicalPlan {

  def emptyBindings: LookupTable[Id, PlanStep] = new LookupTable()

  def attachSubtreeDirect(e: EdgeRef, sel:NodeRef)(
      qg: mutable.Map[NodeRef, QGEdges],
      bindings: LookupTable[Id, PlanStep]
  ): Expand = e match {
    case e @ EdgeRef(name, src, _) =>
      val subPlanKey = LKey(src, Set(e))
      // attempt to get it from cache or recursively compile
      bindings.lookupOrBuildPlan(subPlanKey)(
        compilePlan(qg, bindings, Set(e))(src)
      )

      Expand(
        bindings.bind(subPlanKey),
        name,
        LoadNodes(sel),
        transpose = false
      )
  }

  def attachSubtreeTranspose(e: EdgeRef, sel:NodeRef)(
      qg: mutable.Map[NodeRef, QGEdges],
      bindings: LookupTable[Id, PlanStep]
  ): Expand = e match {

    case e @ EdgeRef(name, _, dst) =>
      val subPlanKey = LKey(dst, Set(e))
      bindings.lookupOrBuildPlan(subPlanKey)(
        compilePlan(qg, bindings, Set(e))(dst)
      )

      Expand(
        bindings.bind(subPlanKey),
        name,
        LoadNodes(sel),
        transpose = true
      )
  }

  def compilePlan(
      qg: mutable.Map[NodeRef, QGEdges],
      bindings: LookupTable[Id, PlanStep] = new LookupTable(),
      exclude: Set[EdgeRef] = Set.empty
  )(sel: NodeRef): PlanStep = {

    val neighbours = qg.neighbours(sel).toSet
    // get all the subtrees
    val subTrees: Set[(LKey, Expand)] = neighbours
      .filterNot(exclude)
      .map { edge =>
        val edgePlanKey =
          LKey(sel, (exclude ++ ((neighbours -- exclude) - edge)))

        val plan = bindings.lookupOrBuildPlan(edgePlanKey) {
          edge match {
            case e if e.dst == sel =>
              attachSubtreeDirect(e, sel)(qg, bindings)
            case e if e.src == sel =>
              attachSubtreeTranspose(e, sel)(qg, bindings)
          }

        }
        edgePlanKey -> plan

      }

    bindings.lookupOrBuildPlan(LKey(sel, exclude)) {
      if (subTrees.isEmpty) {
        LoadNodes(sel)
      } else if (subTrees.size == 1) {
        subTrees.head._2
      } else {
        // combine the subtrees, this step could use costs to estimate the best order
        // save to cache
        // we know the sub plans have been written to the look-up table
        Union(subTrees.map { case (binding, _) => bindings.bind(binding) })
      }
    }
  }

  def compilePlans(
      qg: mutable.Map[NodeRef, QGEdges],
      table: LookupTable[Id, PlanStep] = new LookupTable(),
      exclude: Set[EdgeRef] = Set.empty
  )(sel: Set[NodeRef]): LookupTable[Id, PlanStep] = {
    sel.foldLeft(table) { (c, out) =>
      compilePlan(qg, c, exclude)(out)
      c
    }
  }

}
