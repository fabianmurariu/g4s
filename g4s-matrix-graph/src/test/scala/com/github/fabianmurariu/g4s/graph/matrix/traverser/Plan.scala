package com.github.fabianmurariu.g4s.graph.matrix.traverser

import com.github.fabianmurariu.g4s.graph.matrix.traverser.Traverser.QGEdges

import scala.annotation.tailrec
import scala.collection.mutable

sealed trait Plan
sealed trait BasicPlan extends Plan
case class NodeLoad(label: String) extends BasicPlan
case class EdgeLoad(tpe: String) extends BasicPlan
case class Binding(n: NodeRef) extends BasicPlan
case class Expand(
    from: BasicPlan,
    to: BasicPlan,
    name: String,
    transpose: Boolean
) extends BasicPlan
case class MasterPlan(plans: Map[NodeRef, BasicPlan]) extends Plan

object Plan {

  private def planAsGraphMatrixOp(mp: MasterPlan)(p: BasicPlan): GraphMatrixOp =
    p match {
      case NodeLoad(label) => Nodes(label)
      case EdgeLoad(tpe)   => Edges(tpe)
      case Binding(ref)    => planAsGraphMatrixOp(mp)(mp.plans(ref))
      case Expand(from, to, name, true) =>
        MatMul(
          left = MatMul(
            left = planAsGraphMatrixOp(mp)(from),
            right = Transpose(
              Edges(name)
            )
          ),
          right = planAsGraphMatrixOp(mp)(to)
        )
      case Expand(from, to, name, false) =>
        MatMul(
          left = MatMul(
            left = planAsGraphMatrixOp(mp)(from),
            right = Edges(name)
          ),
          right = planAsGraphMatrixOp(mp)(to)
        )
    }

  def eval(mp: MasterPlan): Map[NodeRef, GraphMatrixOp] = {
    mp.plans.mapValues(planAsGraphMatrixOp(mp))
  }

  def firstPlanFromEdge(out: Set[NodeRef])(e: EdgeRef): Set[Plan] = e match {
    case EdgeRef(_, src, dst) if out(src) && out(dst) =>
      val mp = (firstPlanFromEdge(out - src)(e) ++
        firstPlanFromEdge(out - dst)(e))
        .collect {
          case m: MasterPlan => m
        }
        .reduce { (mp1, mp2) => MasterPlan(mp1.plans ++ mp2.plans) }
      Set(mp)
    case EdgeRef(name, src, dst) if out(src) =>
      Set(
        MasterPlan(
          Map(
            src -> Expand(
              from = NodeLoad(dst.name),
              to = NodeLoad(src.name),
              name = name,
              transpose = true
            )
          )
        )
      )
    case EdgeRef(name, src, dst) if out(dst) =>
      Set(
        MasterPlan(
          Map(
            dst -> Expand(
              from = NodeLoad(src.name),
              to = NodeLoad(dst.name),
              name = name,
              transpose = false
            )
          )
        )
      )
    case EdgeRef(name, src, dst) =>
      Set(
        Expand(
          from = NodeLoad(src.name),
          to = NodeLoad(dst.name),
          name = name,
          transpose = false
        ) //TODO: figure out if the additional edge is needed,
//        Expand(
//          from = NodeLoad(dst.name),
//          to = NodeLoad(src.name),
//          name = name,
//          transpose = true
//        )
      )
  }

  type CoverPlan = (Set[EdgeRef], Plan)

  /**
    * Consume the query graph and produce
    * a logical execution plan with bindings
    */
  def fromQueryGraph(
      qg: mutable.Map[NodeRef, QGEdges],
      out: NodeRef*
  ): MasterPlan = {
    // start somwhere, maybe edges?
    // do the obvious and create an expand a-[:label]->b or a MasterPlan if one of the nodes is
    // on the output path
    val edges = qg.values.flatMap { e => e.in ++ e.out }.toSet

    val outSet = out.toSet
    // starting candidates
    var plans: Map[Set[EdgeRef], Plan] =
      edges.flatMap { e =>
        firstPlanFromEdge(out.toSet)(e).map(plan => Set(e) -> plan)
      }.toMap

    var cand: Map[Set[EdgeRef], Plan] = Map.empty

    do {
      // test if there are plans we can join and join them
      for {
        p1 <- plans
        p2 <- plans
        if p1 != p2
      } {
        val joinedPlan = join(out.toSet)(p1, p2)
        joinedPlan.foreach { newPlan => plans = plans + newPlan }
      }
      // expand the plans
      for (p <- plans) {
        expand(qg, outSet)(p).foreach { newPlan => plans = plans + newPlan }
      }
    } while (cand.size >= 1)

    // println(plans.keys)
    plans.collect { case (_, mp: MasterPlan) => mp }.last
  }

  def expand(qg: mutable.Map[NodeRef, QGEdges], out: Set[NodeRef])(
      p1: CoverPlan
  ): Seq[CoverPlan] = p1 match {
    case (edges, plan) =>
      // we can expand -> or <- from the out node
      // we can expand -> or <- from the in node
      val expandLeft = input(plan).toSeq.flatMap { inputNode =>
        qg.neighbours(inputNode)
          .filterNot(edges)
          .flatMap(edge =>
            firstPlanFromEdge(out)(edge).map(p => Set(edge) -> p)
          )
          .flatMap {
            case (edgeCover, edgePlan) =>
              attachRight(edgePlan, plan).map(p => (edges ++ edgeCover) -> p)
          }
      }

      val expandRight = output(plan).toSeq.flatMap { outputNode =>
        qg.neighbours(outputNode)
          .filterNot(edges)
          .flatMap(edge =>
            firstPlanFromEdge(out)(edge).map(p => Set(edge) -> p)
          )
          .flatMap {
            case (edgeCover, edgePlan) =>
              attachRight(plan, edgePlan).map(p => (edges ++ edgeCover) -> p)
          }
      }
      expandRight ++ expandLeft
  }

  /**
    * Two cover plans can join if (the set of edges they cover are disjoint ???)
    * 1. the output of one plan is the input of another
    * 2. ???
    */
  def join(
      out: Set[NodeRef]
  )(p1: CoverPlan, p2: CoverPlan): Option[CoverPlan] = {
    val (edges1, plan1) = p1
    val (edges2, plan2) = p2
    val out = output(plan1)
    val in = input(plan2)
    if (out == in && plan1 != plan2) {
      // a -> b, b -> c can join into a -> b -> c
      attachRight(plan1, plan2)
        .map { newPlan => (edges1 ++ edges2) -> newPlan }
    } else None
  }

  @tailrec
  def output(p: Plan): Option[NodeRef] = p match {
    case NodeLoad(name)        => Some(NodeRef(name))
    case Expand(_, from, _, _) => output(from)
    case MasterPlan(plans) =>
      plans.keys.headOption // FIXME: a plan can have multiple outputs
    case _ => None
  }

  def input(p: Plan): Option[NodeRef] = p match {
    case NodeLoad(name)        => Some(NodeRef(name))
    case Expand(from, _, _, _) => input(from)
    case MasterPlan(plans)     => plans.headOption.flatMap { p => input(p._2) }
    case _                     => None
  }

  /**
    * Attach p2 to the right of p1
    */
  def attachRight(p1: Plan, p2: Plan): Option[Plan] = (p1, p2) match {
    case (e @ Expand(_, to: Expand, _, _), p: Plan) =>
      Some(e.copy(to = attachRight(to, p).asInstanceOf[BasicPlan]))
    case (e: Expand, p: BasicPlan) =>
      Some(e.copy(to = p))
    case (e: Expand, mp: MasterPlan) =>
      for {
        out <- output(e)
        mpIn <- mp.plans.find { case (_, plan) => input(plan).contains(out) }
        (ref, plan) = mpIn
        newPlan <- attachRight(e, plan)
      } yield MasterPlan(mp.plans + (ref -> newPlan.asInstanceOf[BasicPlan]))
  }

}

// case class Plan(
//     edges: Set[EdgeRef],
//     algExpr: GraphMatrixOp
// ) {

//   def merge(other: Plan): Option[Plan] = {
//     if (this.output == other.output) {
//       Some(
//         Plan(
//           this.edges ++ other.edges,
//           this.algExpr.union(other.algExpr)
//         )
//       )
//     } else None
//   }

//   def cost: Int = algExpr.cost

//   lazy val output: Option[NodeRef] = {
//     def goRight(n: GraphMatrixOp): Option[NodeRef] = n match {
//       case MatMul(_, r)  => goRight(r)
//       case Nodes(name)   => Some(NodeRef(name))
//       case _: Edges      => None
//       case Transpose(op) => goRight(op)
//     }

//     goRight(algExpr)
//   }

//   lazy val input: Option[NodeRef] = {
//     def goLeft(n: GraphMatrixOp): Option[NodeRef] = n match {
//       case MatMul(l, _)  => goLeft(l)
//       case Nodes(name)   => Some(NodeRef(name))
//       case _: Edges      => None
//       case Transpose(op) => goLeft(op)
//     }

//     goLeft(algExpr)
//   }

//   def expand(qg: mutable.Map[NodeRef, QGEdges]): Iterable[Plan] = {
//     output
//       .map { node =>
//         qg.neighbours(node)
//           .filterNot(edges) // exclude edges we know about
//           .flatMap { edge =>
//             edge match {
//               case EdgeRef(name, `node`, dst) => // (node) -[e]-> (dst)
//                 val outAlgExpr =
//                   MatMul(
//                     left = algExpr,
//                     right = MatMul(Edges(name), Nodes(dst.name))
//                   )
//                 val inAlgExpr =
//                   this.algExpr.union(
//                     MatMul(
//                       left = Nodes(dst.name),
//                       right = MatMul(
//                         left = Transpose(Edges(name)),
//                         right = Nodes(node.name)
//                       )
//                     )
//                   )
//                 Seq(
//                   Plan(edges + edge, outAlgExpr),
//                   Plan(edges + edge, inAlgExpr)
//                 )
//               case EdgeRef(name, src, `node`) =>
//                 val outAlgExpr =
//                   MatMul(
//                     left = algExpr,
//                     right = MatMul(Transpose(Edges(name)), Nodes(src.name))
//                   )
//                 val inAlgExpr =
//                   this.algExpr.union(
//                     MatMul(
//                       left = Nodes(src.name),
//                       right = MatMul(
//                         left = Edges(name),
//                         right = Nodes(node.name)
//                       )
//                     )
//                   )
//                 Seq(
//                   Plan(edges + edge, outAlgExpr),
//                   Plan(edges + edge, inAlgExpr)
//                 )
//             }

//           }
//       }
//       .getOrElse(Iterable.empty[Plan])
//   }
// }
