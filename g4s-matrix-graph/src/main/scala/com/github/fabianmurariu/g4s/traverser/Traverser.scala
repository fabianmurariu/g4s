package com.github.fabianmurariu.g4s.traverser

import cats.data.State
import com.github.fabianmurariu.g4s.ops.{Edges, GraphMatrixOp, MatMul, Nodes, Transpose}

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.reflect.runtime.universe.{Traverser => _, _}

object Traverser {

  case class QGEdges(
      out: mutable.Set[EdgeRef] = mutable.Set.empty,
      in: mutable.Set[EdgeRef] = mutable.Set.empty
  )

  type QueryGraph = mutable.Map[NodeRef, QGEdges]
  type Traverser[T] = State[QueryGraph, T]

  def emptyQG = mutable.Map.empty[NodeRef, QGEdges]

  def node[T](implicit tt: TypeTag[T]): Traverser[NodeRef] = State { qg =>
    val label = tt.tpe.toString()
    val ref = NodeRef(label)
    val qgUpdated = qg.get(ref) match {
      case None =>
        qg.update(ref, QGEdges())
        qg
      case _ =>
        qg
    }
    (qgUpdated, ref)
  }

  def edge[T](src: NodeRef, dst: NodeRef)(
      implicit tt: TypeTag[T]
  ): Traverser[EdgeRef] = State { qg =>
    val label = tt.tpe.toString()
    val ref = (for {
      sEdges <- qg.get(src)
      dEdges <- qg.get(dst)
    } yield {
      val e = EdgeRef(label, src, dst)
      sEdges.out += e
      dEdges.in += e
      e
    }).getOrElse(
      throw new IllegalStateException(s"Unable to find node pair $src $dst")
    )
    (qg, ref)
  }

  implicit class QueryGraphOps(val qg: QueryGraph) extends AnyVal {

    def out(v: NodeRef): Iterable[EdgeRef] =
      qg.get(v).toSeq.flatMap(_.out)

    def in(v: NodeRef): Iterable[EdgeRef] =
      qg.get(v).toSeq.flatMap(_.in)

    def outV(v: NodeRef): Iterable[NodeRef] =
      out(v).map(_.dst)

    def inV(v: NodeRef): Iterable[NodeRef] =
      in(v).map(_.src)

    def neighbours(v: NodeRef) =
      out(v) ++ in(v)

    def dfs(v: NodeRef) = {
      val out = mutable.Map[NodeRef, Option[NodeRef]](v -> None)

      def innerDfs(node: NodeRef): Unit = {
        (outV(node) ++ inV(node))
          .foreach { child =>
            if (!out.contains(child)) {
              out += (child -> Some(node))
              innerDfs(child)
            }
          }
      }
      innerDfs(v)
      out
    }

    def removeVertex(v: NodeRef) = {
      qg.outV(v).foreach { dst =>
        val into = qg(dst).in
        into.retain(_.src != v)
      }
      qg.remove(v)
    }

    def connectedComponentsUndirected
        : Seq[mutable.Map[NodeRef, Option[NodeRef]]] = {
      var iter = qg.keysIterator
      val out = mutable.ArrayBuffer[mutable.Map[NodeRef, Option[NodeRef]]]()

      while (iter.hasNext) {
        val cc = dfs(iter.next())
        out += cc
        iter = qg.keysIterator
          .filterNot(node => out.map(_.contains(node)).reduce(_ || _))
      }

      out
    }

    /**
      * Starting from a node find all paths in the Query graph
      * return them as q Queue of edges
      */
    def walkPaths(v: NodeRef): Seq[Queue[EdgeRef]] = {
      val ns = qg.neighbours(v)
      var paths = ns.map(Queue(_))
      val out = Seq.newBuilder[Queue[EdgeRef]]

      val seen: mutable.Set[EdgeRef] = mutable.Set(ns.toList: _*)

      while (paths.nonEmpty) {
        val path :: rest = paths // get the first path in the queue
        val last = path.last
        val children = (qg.neighbours(last.dst) ++ qg.neighbours(last.src))
          .filterNot(seen) // expand the last node
        if (children.isEmpty) {
          out += path
          paths = rest
        } else {
          // FIXME: this part kinds sucks
          val newPaths = children.map { c =>
            seen += c
            path.enqueue(c)
          }.toList
          paths = newPaths ++ rest
        }
      }

      out.result()
    }

    def longestPath: Queue[EdgeRef] = {
      qg.keySet
        .flatMap(walkPaths)
        .maxBy(_.length)
    }

    /**
      * Transform the Query graph into matrix operations
      */
    def compile: Either[QGCompileError, GraphMatrixOp] = {
      if (qg.size <= 1) {
        Left(MinNodeGraphError)
      } else {

        val path = qg.longestPath
        println(s"Longest path -> ${path}")
        val (e, p) = path.dequeue
        val algOp = MatMul(
          left = MatMul(
            left = Nodes(e.src.name),
            right = Edges(e.name)
          ),
          right = Nodes(e.dst.name)
        )
        // e.dst is used to check if the next edge points into the dest (dst <-) or out of it (dst ->)
        val (_, op) = p.foldLeft((e.dst, algOp)) {
          case ((lastDst, op), nextE)
              if nextE.src == lastDst => //we're pointed out ->
            val nextOp = MatMul(
              left = MatMul(
                left = op,
                right = Edges(nextE.name)
              ),
              right = Nodes(nextE.dst.name)
            )
            nextE.dst -> nextOp
          case ((lastDst, op), nextE)
              if nextE.dst == lastDst => //we're pointed out <-
            val nextOp = MatMul(
              left = MatMul(
                left = op,
                right = Transpose(Edges(nextE.name))
              ),
              right = Nodes(nextE.src.name)
            )
            nextE.src -> nextOp
        }
        Right(op)
      }
    }

    /**
      * Starting from n:NodeRef create the matrix operation tree
      *
      */
    def select(n: NodeRef, exclude: Set[EdgeRef] = Set.empty): GraphMatrixOp = {
      // 1. get adjacency edges, remove edges explored
      val edges = neighbours(n).filterNot(exclude).toSet
      if (edges.isEmpty) Nodes(n.name)
      else {
        edges.head match {
          case e @ EdgeRef(name, src, `n`) =>
            // edge points from src to n (src -[:name]-> n)
            MatMul(
              left = MatMul(left = select(src, edges), right = Edges(name)),
              right = select(n, exclude + e)
            )
          case e @ EdgeRef(name, `n`, dst) =>
            // edge points from n to dst (n -[:name]-> dst)
            MatMul(
              left = MatMul(
                left = select(dst, edges),
                right = Transpose(Edges(name))
              ),
              right = select(n, exclude + e)
            )
        }
      }
    }

    type Plan = (Set[NodeRef], GraphMatrixOp)

    def plan: GraphMatrixOp = {

      def canJoin(p1: Plan, p2: Plan): Boolean = {
        false
      }

      def join(p1: Plan, p2: Plan): Option[Plan] = {
        None
      }

      def expandPlan(p: Plan): Seq[Plan] =
        Seq.empty

      def cost(p: Plan): Int =
        p._2.cost

      def contains(p1: Plan, p: Plan): Boolean = {
        p._1.subsetOf(p1._1)
      }

      var planTable: Map[Set[NodeRef], GraphMatrixOp] =
        qg.keys.map(qn => Set(qn) -> Nodes(qn.name)).toMap

      var candidates: Vector[Plan] = Vector()

      do {

        for (p1 <- planTable) {
          for (p2 <- planTable) {
            join(p1, p2) match {
              case None =>
              case Some(p1join2) =>
                candidates = candidates :+ p1join2
            }
          }
        }

        planTable.flatMap(plan => expandPlan(plan)).foreach { p: Plan =>
          candidates = candidates :+ p
        }

        if (candidates.length >= 1) {
          val bestPlan = candidates.minBy(cost)
          for (plan <- planTable.iterator) {
            if (contains(bestPlan, plan)) {
              planTable = planTable - plan._1
            }
          }

        }
      } while (candidates.length >= 1)

      planTable.head._2

    }
  }

  case class Path[T](path: List[T], seen: Set[T], orig: NodeRef)

  sealed trait QGCompileError
  object MinNodeGraphError
      extends RuntimeException("Cannot process query with 1 or less nodes")
      with QGCompileError
}
