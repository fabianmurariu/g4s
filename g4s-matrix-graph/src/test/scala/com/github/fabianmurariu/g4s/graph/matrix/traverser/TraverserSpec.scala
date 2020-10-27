package com.github.fabianmurariu.g4s.graph.matrix.traverser

import com.github.fabianmurariu.g4s.sparse.grbv2.Matrix
import cats.data.State
import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.reflect.runtime.universe.{Traverser => _, _}
import cats.effect.Sync
// AST for matrix operations before executing the actual GRB ops
import cats.implicits._
trait GraphMatrixOp

case class Nodes(label: String) extends GraphMatrixOp

case class Edges(tpe: String) extends GraphMatrixOp

case class MatMul(left: GraphMatrixOp, right: GraphMatrixOp)
    extends GraphMatrixOp

/**
 * Bare minimum graph powered by adjacency matrices
 */
class GrBGraph[F[_], V, E](
    edges: Matrix[F, Boolean],
    edgesT: Matrix[F, Boolean],
    labels: mutable.Map[String, Matrix[F, Boolean]],
    types: mutable.Map[String, Matrix[F, Boolean]],
    dbNodes: mutable.Map[Long, V],
    id: cats.effect.concurrent.Ref[F, Long]
)(implicit F: Sync[F]) {

  def newMatrix: F[Matrix[F, Boolean]] = {
    Matrix[F, Boolean](16, 16).allocated.map(_._1)
  }

  def insertV[B <: V](v: B)(implicit tt: TypeTag[B]): F[Long] =
    for {
      vId <- id.getAndUpdate(i => i + 1)
      tpe = tt.tpe.toString()
      nodeMat <- {
        labels.get(tpe) match {
          case None    => newMatrix
          case Some(m) => F.pure(m)
        }
      }
      _ <- nodeMat.set(vId, vId, true)
      _ <- F.delay {
        dbNodes.put(vId, v)
        labels.put(tpe, nodeMat)
      }
    } yield vId

  def edge[B <: E](src: Long, dst: Long)(
      implicit tt: TypeTag[B]
  ): F[Unit] = for {
    _ <- edges.set(src, dst, true)
    _ <- edgesT.set(dst, src, true)
      tpe = tt.tpe.toString()
    edgeMat <- {
      types.get(tpe) match {
        case None => newMatrix
        case Some(m) => F.pure(m)
      }
    }
    _ <- edgeMat.set(src, dst, true)
    _ <- F.delay{
      types.put(tpe, edgeMat)
    }
  } yield ()

  def run(op: GraphMatrixOp): Matrix[F, Boolean] = {
    ???
  }
}

sealed class Ref
case class NodeRef(val name: String) extends Ref
case class EdgeRef(val name: String, val src: NodeRef, val dst: NodeRef)
    extends Ref

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
      *
      * 1. Find the longest path of the query
      * 2. Walk each edge and determine orientation
      * 3a. Replace with matrix multiply vs edges if edge is ->
      * 3v. Replace with matrix multiply vs transpose edges if edge is <-
      */
    def compile: Either[QGCompileError, GraphMatrixOp] = {
      Left(SingleNodeGraphError)
    }
  }

  sealed trait QGCompileError
  object SingleNodeGraphError
      extends RuntimeException("Cannot process query with single node")
      with QGCompileError
}

sealed trait V
class A extends V
class B extends V
class C extends V
class D extends V
class E extends V

sealed trait Relation
class X extends Relation
class Y extends Relation

class TraverserSpec extends munit.FunSuite {
  import Traverser._
  import scala.collection.mutable
  val aTag = "com.github.fabianmurariu.g4s.graph.matrix.traverser.A"
  val bTag = "com.github.fabianmurariu.g4s.graph.matrix.traverser.B"
  val cTag = "com.github.fabianmurariu.g4s.graph.matrix.traverser.C"
  val dTag = "com.github.fabianmurariu.g4s.graph.matrix.traverser.D"
  val eTag = "com.github.fabianmurariu.g4s.graph.matrix.traverser.E"
  val xTag = "com.github.fabianmurariu.g4s.graph.matrix.traverser.X"
  val yTag = "com.github.fabianmurariu.g4s.graph.matrix.traverser.Y"

  test("single node traverser") {
    val query = node[A]

    val qg = query.runS(emptyQG).value

    val expected = mutable.Map(
      NodeRef(aTag) -> QGEdges()
    )

    assertEquals(qg, expected)
  }

  test("one node traverser expand (A)-[:X]->(B)") {
    val query = for {
      a <- node[A]
      b <- node[B]
      e <- edge[X](a, b)
    } yield e

    val qg = query.runS(emptyQG).value
    assertEquals(qg.connectedComponentsUndirected.size, 1)
  }

  test("walk a path for [A] -> [B] -> [C]") {

    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield ()

    val qg = query.runS(emptyQG).value
    val actual = qg.walkPaths(NodeRef(aTag))
    val expected = List(
      Queue(
        EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag)),
        EdgeRef(yTag, NodeRef(bTag), NodeRef(cTag))
      )
    )

    assertEquals(actual, expected)
    assertEquals(qg.connectedComponentsUndirected.size, 1)
  }

  test("walk a path with splits [A] -> [B], [A] -> [C], start from A") {

    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](a, c)
    } yield ()

    val qg = query.runS(emptyQG).value
    val actual = qg.walkPaths(NodeRef(aTag))
    val expected = List(
      Queue(
        EdgeRef(yTag, NodeRef(aTag), NodeRef(cTag))
      ),
      Queue(
        EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag))
      )
    )

    assertEquals(actual, expected)
    assertEquals(qg.connectedComponentsUndirected.size, 1)
  }

  test("walk a path with splits [A] -> [B], [A] -> [C] start from B") {

    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](a, c)
    } yield ()

    val qg = query.runS(emptyQG).value
    val actual = qg.walkPaths(NodeRef(bTag))
    val expected = List(
      Queue(
        EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag)),
        EdgeRef(yTag, NodeRef(aTag), NodeRef(cTag))
      )
    )

    assertEquals(actual, expected)
    assertEquals(qg.connectedComponentsUndirected.size, 1)
  }

  test("evaluate 2 disjoint paths [A] -> [B], [C] -> [D]") {

    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      d <- node[D]
      _ <- edge[X](a, b)
      _ <- edge[Y](c, d)
    } yield ()

    val qg = query.runS(emptyQG).value
    val actual1 = qg.walkPaths(NodeRef(aTag))
    val expected1 = List(
      Queue(
        EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag))
      )
    )

    assertEquals(actual1, expected1)

    val actual2 = qg.walkPaths(NodeRef(cTag))
    val expected2 = List(
      Queue(
        EdgeRef(yTag, NodeRef(cTag), NodeRef(dTag))
      )
    )

    assertEquals(actual2, expected2)
    assertEquals(qg.connectedComponentsUndirected.size, 2)
  }

//  test("walk a path in a graph with a cycle [A] -> [B] -> [A]") {
//
//    val query = for {
//      a <- node[A]
//      b <- node[B]
//      _ <- edge[X](a, b)
//      _ <- edge[Y](b, a)
//    } yield ()
//
//    val qg = query.runS(emptyQG).value
//    val actual1 = qg.walkPaths(NodeRef(aTag))
//    val expected1 = List(
//      Queue(
//        EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag)),
//        EdgeRef(yTag, NodeRef(bTag), NodeRef(aTag))
//      ),
//      Queue(
//        EdgeRef(yTag, NodeRef(bTag), NodeRef(aTag))
//      )
//    )
//
//    assertEquals(actual1, expected1)
//  }

  test("find the longest path of a graph disjoint") {

    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      d <- node[D]
      e <- node[E]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
      _ <- edge[X](d, e)
    } yield ()

    val qg = query.runS(emptyQG).value
    val longestPath = qg.longestPath

    val expectedLongestPath = Queue(
      EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag)),
      EdgeRef(yTag, NodeRef(bTag), NodeRef(cTag))
    )

    assertEquals(longestPath, expectedLongestPath)
  }

  test("find the longest path of a graph") {

    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      d <- node[D]
      e <- node[E]
      _ <- edge[X](a, b)
      _ <- edge[X](a, c)
      _ <- edge[Y](c, d)
      _ <- edge[X](d, e)
    } yield ()

    val qg = query.runS(emptyQG).value
    val longestPath = qg.longestPath

    val expectedLongestPath = Queue(
      EdgeRef(xTag, NodeRef(dTag), NodeRef(eTag)),
      EdgeRef(yTag, NodeRef(cTag), NodeRef(dTag)),
      EdgeRef(xTag, NodeRef(aTag), NodeRef(cTag)),
      EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag))
    )

    assertEquals(longestPath, expectedLongestPath)
  }

  test("the simplest query graph with 1 node results in error") {
    node[A].runS(emptyQG).value.compile
  }

  test("(a) -[:X]-> (b) is a*X*b ") {
    val query = for {
      a <- node[A]
      b <- node[B]
      _ <- edge[X](a, b)
    } yield ()

    val Right(matOps) = query.runS(emptyQG).value.compile

    assertEquals(matOps, MatMul(MatMul(Nodes(aTag), Edges(xTag)), Nodes(bTag)))

  }
}