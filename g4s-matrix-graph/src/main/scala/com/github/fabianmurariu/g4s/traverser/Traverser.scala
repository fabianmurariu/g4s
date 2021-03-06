package com.github.fabianmurariu.g4s.traverser

import cats.data.State
import com.github.fabianmurariu.g4s.ops.{
  Edges,
  GraphMatrixOp,
  MatMul,
  Nodes,
  Transpose
}

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

  case class Ret(ns: NodeRef*)

  def emptyQG: mutable.Map[NodeRef,QGEdges] = mutable.Map.empty[NodeRef, QGEdges]

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

  implicit class QueryGraphOps(private val qg: QueryGraph) extends AnyVal {

    def out(v: NodeRef): Iterable[EdgeRef] =
      qg.get(v).toSeq.flatMap(_.out)

    def in(v: NodeRef): Iterable[EdgeRef] =
      qg.get(v).toSeq.flatMap(_.in)

    def outV(v: NodeRef): Iterable[NodeRef] =
      out(v).map(_.dst)

    def inV(v: NodeRef): Iterable[NodeRef] =
      in(v).map(_.src)

    def neighbours(v: NodeRef): Iterable[EdgeRef] =
      out(v) ++ in(v)

    def edges:Set[EdgeRef] =
      qg.values.flatMap(e => e.in ++ e.out).toSet

    def dfs(v: NodeRef): mutable.Map[NodeRef,Option[NodeRef]] = {
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

    def removeVertex(v: NodeRef): Option[QGEdges] = {
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

  }

  case class Path[T](path: List[T], seen: Set[T], orig: NodeRef)

  sealed trait QGCompileError
  object MinNodeGraphError
      extends RuntimeException("Cannot process query with 1 or less nodes")
      with QGCompileError

  class EdgeNotAttachedToPlan(e: EdgeRef)
      extends RuntimeException(s"Edge ${e} not attached to plan")
      with QGCompileError

}
