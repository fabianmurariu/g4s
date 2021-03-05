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
      case Filter(rc, f)    => 1 + rc.map(_.depth) + f.depth
      case Select(e)        => 1 + e.map(_.depth)
    }

    /** BINDINGS */
    // the Matrix M(x,y)
    // has information about the source node x on rows
    // and about the destination node y on columns
    // these are used as a guide when
    // deciding what plan to execute next
    // for example (a)->(b)->(c) return a, c only requires a*E*b*E*c
    // because the resulting matrix has
    // a nodes on the row axis
    // c nodes on the column axix
    /**
      * node binding present at the row level
      * */
    def row: Set[NodeRef]

    /**
      * node binding present at the column level
      * */
    def column: Set[NodeRef]
  }

  sealed trait Diagonal extends Step

  /**
    * transform an expand or filter to a diagonal matrix
    * by reducing over columns
    * and setting the resulting vector
    * on a diagonal
    * */
  case class Select(e: Rc[Step]) extends Diagonal {
    // transforming into a diagonal preserves only the column
    // binding
    val row: Set[NodeRef] = e.map(_.column)
    val column: Set[NodeRef] = e.map(_.column)
  }

  /**
    * get the diagonal matrix for the node label
    * */
  case class LoadNodes(ref: NodeRef) extends Diagonal {
    val row: Set[NodeRef] = Set(ref)
    val column: Set[NodeRef] = Set(ref)
  }

  /**
    * expand from a frontier or a diagonal matrix
    * over an edge, equivalent to (a)->[:X] or (a)<-[:X] if transposed
    * */
  case class Expand(
      from: Rc[Step],
      edgeType: String,
      transpose: Boolean
  ) extends Step {
    val row: Set[NodeRef] = from.map(_.row)
    val column: Set[NodeRef] = Set.empty
  }

  /**
    * apply a diagonal fitler after an expansion step
    * to reduce the output to only the nodes present
    * on the [[Diagonal]] matrix
    * */
  case class Filter(expand: Rc[Step], filter: Diagonal) extends Step {
    val row: Set[NodeRef] = expand.map(_.row)
    val column: Set[NodeRef] = filter.column
  }

  /**
    * context for keeping track
    * of subgraphs explored
    * */
  type LookupTable[A] = mutable.Map[LKey, Rc[A]]

  def emptyLookupTable = mutable.Map.empty[LKey, Rc[Step]]

  private[traverser] def dfsCompileExpansion(
      qg: mutable.Map[NodeRef, QGEdges],
      seen: LookupTable[Step] = emptyLookupTable,
      exclude: Set[EdgeRef] = Set.empty,
      input: Option[NodeRef] = None
  )(edge: EdgeRef, sel: NodeRef): Rc[Step] = {

    val e @ EdgeRef(name, src, dst) = edge
    val key1: LKey = LKey(sel, exclude, expand = true)

    val plan = seen.getOrElseUpdate(key1, {
      val transpose = (dst != sel)
      val child = if (transpose) dst else src
      new Rc(
        Expand(
          dfsCompilePlan(qg, seen, exclude + e)(child, input).ref,
          name,
          transpose
        )
      )
    })
    plan
  }

  /**
    * compile plan for
    * query matrix (input) -> (sel)
    * */
  private[traverser] def dfsCompilePlan(
      qg: mutable.Map[NodeRef, QGEdges],
      seen: LookupTable[Step] = emptyLookupTable,
      exclude: Set[EdgeRef] = Set.empty
  )(sel: NodeRef, input: Option[NodeRef] = None): Rc[Step] = {

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
            dfsCompileExpansion(qg, seen, exc, input)(e, sel)
          val filter = new Rc(
            Filter(
              expand.ref,
              LoadNodes(sel)
            )
          )
          if (input.contains(sel)) {
            // we reached a node that is part of the root output edge
            // we need to get it on the row side and keep it there
            new Rc(Select(filter.ref))
          } else filter
        } else {
          // this is where we need
          // to ensure the input node
          // of the root plan is
          // the last one to expand
          // against the selection

          val subPaths: Vector[(EdgeRef, Rc[Step])] = neighbours.toVector.map { e =>
            val exc = (exclude ++ ((neighbours -- exclude) - e))
            e -> dfsCompileExpansion(qg, seen, exc)(e, sel)
          }

          val last = subPaths.find{case (_, subPath) => subPath.map(p => input.exists(p.row))}

          val subPathsInputRemoved = subPaths.filterNot(subPath => last.contains(subPath))

          val (_, plan) = subPathsInputRemoved.head

          val filter = new Rc(Filter(plan.ref, LoadNodes(sel)))

          val first = Select(filter.ref)
          val rest = subPathsInputRemoved.tail

          val combinedPlan:Select = rest.foldLeft(first) {
            case (select, (_, expand)) =>
              Select(new Rc(Filter(expand.ref, select)).ref)
          }

          // we are tracking the input and it's present in this subtree
          last match {
            case None =>
              combinedPlan.e // we actually don't care about the last select
            case Some((_, expand)) =>
              new Rc(Filter(expand.ref, combinedPlan)).ref
          }
        }
      }
    )
    plan
  }

  def compilePlans(
      qg: mutable.Map[NodeRef, QGEdges],
      table: LookupTable[Step] = emptyLookupTable,
      exclude: Set[EdgeRef] = Set.empty
  )(sel: Iterable[NodeRef]): Seq[Rc[Step]] = {
    // first we determine the node matrices required
    // out of the selection (return) statement
    // eg (a)->(b)-(c) return a,c means return a single
    // matrix M(a, c)
    sel
      .sliding(2, 1)
      .map(_.toList)
      .map { group =>
        val a = group.head
        val b = group.tail.headOption
        (a, b)
      }
      .foldLeft(List.empty[Rc[Step]]) {
        case (b, (n, input)) =>
          // find if we get the plan for free
          // on the row or column of a previous plan
          val step = dfsCompilePlan(qg, table, exclude)(n, input).ref
          // val step = b.find{s =>
          //   s.map(_.row).contains(n) || s.map(_.column).contains(n)
          // } match {
          //   case Some(found) => found.ref
          //   case None =>
          // }
          input match {
            case None =>
              step :: b
            case _ =>
              step :: step.ref :: b
          }
      }
      .reverse
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

  case class LKey(sel: NodeRef, exclude: Set[EdgeRef], expand: Boolean = false)

}
