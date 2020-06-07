package com.github.fabianmurariu.g4s.graph

import cats.free.Free
import cats.arrow.FunctionK
import cats.{Id, ~>}
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import scala.collection.mutable
import com.github.fabianmurariu.g4s.sparse.mutable.MatrixLike
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.MxM
import com.github.fabianmurariu.g4s.sparse.grb.ElemWise
import com.github.fabianmurariu.g4s.sparse.mutable.Matrix
import com.github.fabianmurariu.g4s.sparse.grb.MatrixHandler
import scala.collection.generic.CanBuildFrom
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.sparse.grb.MatrixBuilder
import zio._
import fs2.Stream

/**
  * Naive first pass implementation of a Graph database
  * over sparse matrices that may be of type GraphBLAS
  * FIXME:
  *  this is mutable but all functions return effects
  *  multiple threads are not supported here
  * */
final class GraphDB[M[_]](
    private[graph] val nodes: M[Boolean],
    private[graph] val edgesOut: M[Boolean],
    private[graph] val edgesIn: M[Boolean],
    private[graph] val relTypes: mutable.Map[String, M[Boolean]],
    private[graph] val labelIndex: mutable.Map[String, mutable.Set[
      Long
    ]], // node labels and node ids
    nodeData: mutable.Map[Long, mutable.Set[String]],
    edgeData: mutable.Map[(Long, Long), mutable.Set[String]],
    var count: Long
)(
    implicit M: Matrix[M],
    MH: MatrixHandler[M, Boolean],
    MB: MatrixBuilder[Boolean],
    OPS: BuiltInBinaryOps[Boolean]
) { self =>

  def insertVertex(labels: Vector[String]): Task[Long] = IO.effect {
    val vxId = count;
    M.set(nodes)(vxId, vxId, true)

    labels.foreach { label =>
      val set = labelIndex.getOrElseUpdate(label, mutable.Set(vxId))
      set += vxId
    }

    nodeData.getOrElseUpdate(vxId, mutable.Set(labels: _*))

    count += 1
    vxId
  }

  def insertEdge(
      src: Long,
      tpe: String,
      dest: Long
  ): Task[(Long, Long)] =
    for {
      _ <- IO.effect {
        M.set(edgesOut)(src, dest, true)
        M.set(edgesIn)(dest, src, true)
      }

      tpeMat <- {
        relTypes.get(tpe) match {
          case None => {
            M.makeUnsafe[Boolean](16, 16).map { m =>
              relTypes.put(tpe, m)
              m
            }
          }
          case Some(m) => IO.effect(m)
        }
      }
      pair <- IO.effect {

        M.set(tpeMat)(src, dest, true)

        val edgeTypeData =
          self.edgeData.getOrElseUpdate((src, dest), mutable.Set(tpe))
        edgeTypeData += tpe
        (src, dest)

      }
    } yield pair

  /**
    * TODO:
    * assumed relation is we want the matrix including all the labels
    * but we could create a boolean expression saying we want :a labels and not :b labels etc..
    */
  def vectorMatrixForLabels(labels: Set[String]): TaskManaged[M[Boolean]] = {
    val mRes = M.make(16, 16)
    mRes.map { m =>
      labels.foldLeft(m) { (mat, label) =>
        labelIndex.get(label).foreach { nodes =>
          nodes.foldLeft(m) { (mat, vid) =>
            MH.set(m)(vid, vid, true)
            m
          }
        }
        m
      }
    }
  }

  /**
    * TODO:
    * see vectorMatrixForLabels
    */
  def edgeMatrixForTypes(
      types: Set[String],
      out: Boolean
  ): TaskManaged[M[Boolean]] = {
    val outRes = M.make(16, 16)

    outRes.mapM { out =>
      types
        .flatMap(tpe => relTypes.get(tpe))
        .foldLeft(IO.effect(out)) { (intoIO, mat) =>
          for {
            into <- intoIO
            union <- Matrix[M].union(into)(Left(OPS.land))(into, mat)
          } yield union
        }
    }
  }

  /**
    * transform a matrix of vertices to the external world
    * view of Id + Labels
    */
  def labeledVertices(m: M[Boolean]): Vector[(Long, Set[String])] = {
    val (_, _, js) = MH.copyData(m)

    js.map(vId =>
        vId -> self.nodeData.get(vId).map(_.to[Set]).getOrElse(Set.empty)
      )
      .toVector
  }

  /**
    * Naive approach of moving from sparse matrix to external data =
    * FIXME: for more serious business check-out
    * https://groups.google.com/a/lbl.gov/forum/#!topic/graphblas/CU2bTF1DV_8
    * and
    * https://github.com/GraphBLAS/LAGraph/blob/1eb22f947e606fe18e8e0aef2ba9b9047010b718/Source/Algorithm/LAGraph_dense_relabel.c
    */
  def renderEdges(m: M[Boolean]): Vector[(Long, String, Long)] = {
    val (_, is, js) = MH.copyData(m)

    (0 until js.length)
      .foldLeft(Vector.newBuilder[(Long, String, Long)]) { (b, i) =>
        val src = is(i)
        val dest = js(i)
        edgeData.get(src, dest).foreach { tpes =>
          tpes.foreach { tpe => b += ((src, tpe, dest)) }
        }
        b
      }
      .result()
  }

}

object GraphDB {

  def default: TaskManaged[GraphDB[GrBMatrix]] =
    for {
      nodes <- GrBMatrix[Boolean](16, 16)
      edgesIn <- GrBMatrix[Boolean](16, 16)
      edgesOut <- GrBMatrix[Boolean](16, 16)
      graph <- Managed.makeEffect(
        new GraphDB[GrBMatrix](
          nodes,
          edgesIn,
          edgesOut,
          mutable.Map.empty,
          mutable.Map.empty,
          mutable.Map.empty,
          mutable.Map.empty,
          0L
        )
      ) { gdb => gdb.relTypes.valuesIterator.foreach(_.close()) }
    } yield graph

}

sealed trait GraphStep[+T]

case class CreateNode(labels: Vector[String]) extends GraphStep[Long]
case class CreateEdge(src: Long, tpe: String, dest: Long)
    extends GraphStep[(Long, Long)]
case class QueryStep(q: Query) extends GraphStep[QueryResult]

sealed trait Direction
case object Undirected extends Direction
case object Out extends Direction
case object In extends Direction

sealed trait Query { self =>

  /**
    * (a) -[edgeTypes..]-> (b)
    */
  def out(edgeTypes: String*) = {
    if (edgeTypes.isEmpty) {
      AndThen(self, AllEdgesOut)
    } else {
      AndThen(self, Edges(edgeTypes.toSet, Out))
    }
  }

  /**
    * (a) <-[edgeTypes..]- (b)
    */
  def in(edgeTypes: String*) = {
    if (edgeTypes.isEmpty) {
      AndThen(self, AllEdgesIn)
    } else {
      AndThen(self, Edges(edgeTypes.toSet, In))
    }
  }

  /**
    * .. (b: vertexLabel)
    */
  def v(vertexLabels: String*) = {
    if (vertexLabels.isEmpty) {
      AndThen(self, AllVertices)
    } else {
      AndThen(self, Vertex(vertexLabels.toSet))
    }
  }

  /**
    * shorthand for .out().v(vertexLabel)
    * (a) -> (b:vertexLabel ..)
    */
  def outV(vertexLabel: String, vertexLabels: String*) =
    AndThen(out(), Vertex((vertexLabel +: vertexLabels).toSet))

  /**
    * shorthand for .in().v(vertexLabel)
    * (a) <- (b:vertexLabel ..)
    */
  def inV(vertexLabel: String, vertexLabels: String*) =
    AndThen(in(), Vertex((vertexLabel +: vertexLabels).toSet))
}

sealed trait EdgeQuery extends Query
sealed trait VertexQuery extends Query

case object AllVertices extends VertexQuery
case object AllEdges extends EdgeQuery
case object AllEdgesOut extends EdgeQuery
case object AllEdgesIn extends EdgeQuery

case class Edges(types: Set[String], direction: Direction) extends EdgeQuery
case class Vertex(labels: Set[String]) extends VertexQuery
case class AndThen(cur: Query, next: Query) extends Query

sealed trait QueryResult
case class VerticesRes(vs: Vector[(Long, Set[String])]) extends QueryResult
case class EdgesRes(es: Vector[(Long, String, Long)]) extends QueryResult

object GraphStep {

  type Step[T] = Free[GraphStep, T]
  type Observable[T] = Stream[Task, T]

  def interpreter[M[_]: Matrix](g: GraphDB[M]) = new (GraphStep ~> Observable) {
    def apply[A](fa: GraphStep[A]): Observable[A] = fa match {
      case CreateNode(labels) =>
        Stream.eval(
          g.insertVertex(labels)
          // TODO: way too small of an operation, should use chunks
        )

      case CreateEdge(src, tpe, dest) =>
        Stream.eval(
          g.insertEdge(src, tpe, dest)
        )

      // TODO: we need to know what type is the last step of the path in order to render
      // we should probably use some form of associated types or a Render typeclass
      case QueryStep(q: EdgeQuery) =>
        Stream.eval(matAsEdges(foldQueryPath(g, q))(g))

      case QueryStep(q: VertexQuery) =>
        Stream.eval(matAsVertices(foldQueryPath(g, q))(g))

      case QueryStep(q @ AndThen(_, _: EdgeQuery)) =>
        Stream.eval(matAsEdges(foldQueryPath(g, q))(g))

      case QueryStep(q @ AndThen(_, _: VertexQuery)) =>
        Stream.eval(matAsVertices(foldQueryPath(g, q))(g))
    }

  }

  def lift[G[_], A](g: G[A]): TaskManaged[G[A]] =
    Managed.fromEffect(IO.effect(g))

  def foldQueryPath[M[_]: Matrix](g: GraphDB[M], q: Query)(
      implicit OPS: BuiltInBinaryOps[Boolean]
  ): TaskManaged[M[Boolean]] = {

    def loop(
        semiring: GrBSemiring[Boolean, Boolean, Boolean],
        q: Query
    ): TaskManaged[M[Boolean]] =
      q match {
        case AllVertices => lift(g.nodes)
        case AllEdgesOut => lift(g.edgesOut)
        case AllEdgesIn  => lift(g.edgesIn)
        case Vertex(labels) =>
          g.vectorMatrixForLabels(labels)
        case Edges(types, Out) =>
          g.edgeMatrixForTypes(types, true)
        case Edges(types, In) =>
          g.edgeMatrixForTypes(types, false)
        case AndThen(cur, next) =>
          for {
            left <- loop(semiring, cur)
            right <- loop(semiring, next)
            out <- MxM[M].mxmNew(semiring)(left, right)
          } yield out
      }

    for {
      add <- GrBMonoid[Boolean](OPS.any, true)
      semiring <- GrBSemiring[Boolean, Boolean, Boolean](add, OPS.pair)
      out <- loop(semiring, q)
    } yield out
  }

  def matAsVertices[M[_]](
      mat: TaskManaged[M[Boolean]]
  )(g: GraphDB[M]): Task[VerticesRes] =
    mat.use { m => IO.effect(VerticesRes(g.labeledVertices(m))) }

  def matAsEdges[M[_]](
      mat: TaskManaged[M[Boolean]]
  )(g: GraphDB[M]): Task[EdgesRes] =
    mat.use { m => IO.effect(EdgesRes(g.renderEdges(m))) }

  def createNode(label: String, labels: String*) = {
    Free.liftF(CreateNode((label +: labels).toVector))
  }

  def createEdge(src: Long, tpe: String, dest: Long) = {
    Free.liftF(CreateEdge(src, tpe, dest))
  }

  def query(q: Query) =
    Free.liftF(QueryStep(q))

  def vs = AllVertices

}
