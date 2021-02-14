package com.github.fabianmurariu.g4s.traverser

import com.github.fabianmurariu.g4s.traverser.Traverser.QGEdges

import scala.collection.mutable

object LogicalPlan {

  sealed abstract class Step { self =>

    def show: String = self match {
      case LoadNodes(ref) => ref.name
      case Select(e) =>
        s"sel[${e.count};${e.map(_.show)}]"
      case Filter(exp, filter) =>
        s"[${exp.count};${exp.map(_.show)}](${filter.show})"
      case Expand(from, name, true) =>
        s"([${from.count};${from.map(_.show)}])<-[:$name]-"
      case Expand(from, name, false) =>
        s"([${from.count};${from.map(_.show)}])-[:$name]->"
    }

   def depth: Int = self match {
     case _: LoadNodes     => 1
     case Expand(rc, _, _) => rc.count + rc.map(_.depth) + 1
     case Filter(rc, f)     => 1 + rc.map(_.depth) + f.depth
     case Select(e)        => 1 + e.map(_.depth)
   }
  }

  sealed trait Diagonal extends Step

  /**
    * transform an expand to a diagonal matrix
    * by reducing over columns
    * and setting the resulting vector
    * on a diagonal
    * */
  case class Select(e: Rc[Step]) extends Diagonal

  /**
    * get the diagonal matrix for the node label
    * */
  case class LoadNodes(ref: NodeRef) extends Diagonal

  /**
   * expand from a frontier or a diagonal matrix
   * over an edge, equivalent to (a)->[:X] or (a)<-[:X] if transposed
   * */
  case class Expand(
      from: Rc[Step],
      edgeType: String,
      transpose: Boolean
  ) extends Step

  /**
   * apply a diagonal fitler after an expansion step
   * to reduce the output to only the nodes present
   * on the [[Diagonal]] matrix
   * */
  case class Filter(expand: Rc[Step], filter: Diagonal) extends Step

  /**
   * context for keeping track
   * of subgraphs explored
   * */
  type LookupTable[A] = mutable.Map[LKey, Rc[A]]


  def dfsCompileExpansion(
      qg: mutable.Map[NodeRef, QGEdges],
      seen: LookupTable[Step],
      exclude: Set[EdgeRef] = Set.empty
  )(edge: EdgeRef, sel: NodeRef): Rc[Step] = {

    val e @ EdgeRef(name, src, dst) = edge
    val key1: LKey = LKey(sel, exclude, expand = true)

    val plan = seen.getOrElseUpdate(key1, {
      val transpose = (dst != sel)
      val child = if (transpose) dst else src
      new Rc(
        Expand(
          dfsCompilePlan(qg, seen, exclude + e)(child).ref,
          name,
          transpose
        )
      )
    })
    plan
  }


  def dfsCompilePlan(
      qg: mutable.Map[NodeRef, QGEdges],
      seen: LookupTable[Step],
      exclude: Set[EdgeRef] = Set.empty
  )(sel: NodeRef): Rc[Step] = {

    val key = LKey(sel, exclude)
    val plan = seen.getOrElseUpdate(
      key, {
        val neighbours = qg
          .neighbours(sel)
          .filterNot(exclude)
          .toSet

        if (neighbours.isEmpty) {
          new Rc(LoadNodes(sel))
        } else if (neighbours.size == 1) {
          val e = neighbours.head
          val exc = (exclude ++ ((neighbours -- exclude) - e))
          val expand =
            dfsCompileExpansion(qg, seen, exc)(e, sel)
          val filter = new Rc(
            Filter(
              expand.ref,
              LoadNodes(sel)
            )
          )
          filter
        } else {
          val subPaths: Seq[(EdgeRef, Rc[Step])] = neighbours.toSeq.map { e =>
          val exc = (exclude ++ ((neighbours -- exclude) - e))
            e -> dfsCompileExpansion(qg, seen, exc)(e, sel)
          }
          val (_, plan) = subPaths.head

          val filter = new Rc(Filter(plan.ref, LoadNodes(sel)))

          val first = Select(filter.ref)
          val rest = subPaths.tail
          val combinedPlan = rest.foldLeft(first) {
            case (select, (_, expand)) =>
              Select(new Rc(Filter(expand.ref, select)).ref)
          }
          combinedPlan.e // we actually don't care about the last select
        }
      }
    )
    plan
  }

  def compilePlans(
      qg: mutable.Map[NodeRef, QGEdges],
      table: LookupTable[Step],
      exclude: Set[EdgeRef] = Set.empty
  )(sel: Iterable[NodeRef]): Seq[Rc[Step]] = {

    val b = Seq.newBuilder[Rc[Step]]
    sel.foreach { n => b += dfsCompilePlan(qg, table, exclude)(n) }
    b.result()
  }

  class Rc[+A](a: A, private var rc: Int = 0) {
    def ref: Rc[A] = {
      rc += 1
      this
    }

    def deref: Option[A] = {
      if (rc == -1) None
      else {
        rc -= 1
        Some(a)
      }
    }

    def count: Int =
      rc

    def map[B](f: A => B): B = {
      f(a)
    }
  }

  case class LKey(sel:NodeRef, exclude: Set[EdgeRef], expand: Boolean = false)
}
