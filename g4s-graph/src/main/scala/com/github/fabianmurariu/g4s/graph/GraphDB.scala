package com.github.fabianmurariu.g4s.graph

import cats.free.Free
import cats.arrow.FunctionK
import cats.{Id, ~>}
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import scala.collection.mutable
import monix.execution.atomic.AtomicLong
import monix.reactive.Observable
import com.github.fabianmurariu.g4s.sparse.mutable.MatrixLike
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.MxM
import com.github.fabianmurariu.g4s.sparse.grb.ElemWise
import cats.effect.Resource
import cats.effect.Sync
import com.github.fabianmurariu.g4s.sparse.mutable.Matrix
import monix.eval.Task
import com.github.fabianmurariu.g4s.sparse.grb.MatrixHandler
import scala.collection.generic.CanBuildFrom
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.sparse.grb.MatrixBuilder

/**
  * Naive first pass implementation of a Graph database
  * over sparse matrices that may be of type GraphBLAS
  * FIXME:
  *  this is dangerously unsafe and mutable
  *  there is no guarantee about intermediate matrices and when they are
  *  supposed to be released
  *  multiple threads are not supported here
 *   Resource should be able to encode the ownership from rust
  * */
sealed class GraphDB[M[_]](
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
) extends AutoCloseable { self =>

  def close(): Unit = {
    M.release(nodes)
    M.release(edgesOut)
    M.release(edgesIn)
    relTypes.valuesIterator.foreach(M.release)
  }

  def insertVertex(labels: Vector[String]): Long = {
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

  def insertEdge(src: Long, tpe: String, dest: Long): (Long, Long) = {
    M.set(edgesOut)(src, dest, true)
    M.set(edgesIn)(dest, src, true)

    val tpeMat = relTypes.getOrElseUpdate(tpe, M.make[Boolean](16, 16))
    M.set(tpeMat)(src, dest, true)

    val edgeTypeData =
      self.edgeData.getOrElseUpdate((src, dest), mutable.Set(tpe))
    edgeTypeData += tpe
    (src, dest)

  }

  /**
    * TODO:
    * assumed relation is we want the matrix including all the labels
    * but we could create a boolean expression saying we want :a labels and not :b labels etc..
    */
  def vectorMatrixForLabels(labels: Set[String]): M[Boolean] = {
    val m = M.make(16, 16)
    labels.foldLeft(m) { (mat, label) =>
      labelIndex.get(label).foreach { nodes =>
        nodes.foldLeft(m) { (mat, vid) =>
          MH.set(m)(vid, vid, true)
          m
        }
      }
      m
    }
    m
  }

  /**
    * TODO:
    * see vectorMatrixForLabels
    */
  def edgeMatrixForTypes(types: Set[String], out: Boolean): M[Boolean] = {
    val out = M.make(16, 16)
    types
      .flatMap(tpe => relTypes.get(tpe))
      .foldLeft(out)(Matrix[M].unionInto(Left(OPS.land)))
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
  import cats.implicits._
  def default[F[_]: Sync]: Resource[F, GraphDB[GrBMatrix]] = {
    Resource.fromAutoCloseable(
      Sync[F].delay(
        new GraphDB[GrBMatrix](
          GrBMatrix[Boolean](16, 16),
          GrBMatrix[Boolean](16, 16),
          GrBMatrix[Boolean](16, 16),
          mutable.Map.empty,
          mutable.Map.empty,
          mutable.Map.empty,
          mutable.Map.empty,
          0L
        )
      )
    )
  }
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

  def interpreter[M[_]: Matrix](g: GraphDB[M]) = new (GraphStep ~> Observable) {
    def apply[A](fa: GraphStep[A]): Observable[A] = fa match {
      case CreateNode(labels) =>
        Observable.fromTask(
          Task.delay(g.insertVertex(labels))
        )

      case CreateEdge(src, tpe, dest) =>
        Observable.fromTask(
          Task.delay(g.insertEdge(src, tpe, dest))
        )

      case QueryStep(q: EdgeQuery) =>
        Observable.fromTask(
          Task.delay(
            matAsEdges(foldQueryPath(g, q))(g)
          )
        )

      case QueryStep(q: VertexQuery) =>
        Observable.fromTask(
          Task.delay(
            matAsVertices(foldQueryPath(g, q))(g)
          )
        )

      case QueryStep(q @ AndThen(_, _: EdgeQuery)) =>
        Observable.fromTask(
          Task.delay(
            matAsEdges(foldQueryPath(g, q))(g)
          )
        )

      case QueryStep(q @ AndThen(_, _: VertexQuery)) =>
        Observable.fromTask(
          Task.delay(
            matAsVertices(foldQueryPath(g, q))(g)
          )
        )
    }

  }

  /**
   * Used to determine when we can release an intermediary
   * matrix or if we have a matrix that is not ours to release
   *
   */
  sealed trait FoldSignal[M[_]] { self =>
    def m:M[Boolean]
    def isCore = self.isInstanceOf[Core[M]]
    def isIntermediate = !isCore
  }
  case class Core[M[_]](m:M[Boolean]) extends FoldSignal[M]
  case class Intermediate[M[_]](m: M[Boolean]) extends FoldSignal[M]

  def foldQueryPath[M[_]: Matrix](g: GraphDB[M], q: Query)(
      implicit OPS: BuiltInBinaryOps[Boolean]
  ): FoldSignal[M] = {
    var add: GrBMonoid[Boolean] = null
    var semiring: GrBSemiring[Boolean, Boolean, Boolean] = null
    try {
      add = GrBMonoid(OPS.any, true)
      semiring = GrBSemiring(add, OPS.pair)

      // FIXME: in order for this to correctly fold it needs to
      // be a stack based interpreter otherwise we don't know
      // when to copy the left most mat then use that with mxm
      // here we leak intermediary matrices
      def loop(q: Query): FoldSignal[M] = {
        q match {
          case AllVertices => Core(g.nodes)
          case AllEdgesOut => Core(g.edgesOut)
          case AllEdgesIn  => Core(g.edgesIn)
          case Vertex(labels) =>
            Intermediate(g.vectorMatrixForLabels(labels))
          case Edges(types, Out) =>
            Intermediate(g.edgeMatrixForTypes(types, true))
          case Edges(types, In) =>
            Intermediate(g.edgeMatrixForTypes(types, false))
          case AndThen(cur, next) =>
            val left = foldQueryPath(g, cur)
            val right = foldQueryPath(g, next)

            try {
              Intermediate(MxM[M].mxm(semiring)(left.m, right.m))
            } finally {
              if (left.isIntermediate) {
                Matrix[M].release(left.m)
              }
              if (right.isIntermediate) {
                Matrix[M].release(right.m)
              }
            }
        }
      }
      loop(q)
    } finally {
      add.close()
      semiring.close()
    }
  }

  // TODO: Matrix is too big for these operations, there should be a Closeable like typeclass
  def matAsVertices[M[_]:Matrix](mat: FoldSignal[M])(g: GraphDB[M]): VerticesRes = {
    val m = mat.m
    try {
      VerticesRes(g.labeledVertices(m))
    } finally {
      if (mat.isIntermediate) {
        Matrix[M].release(m)
      }
    }
  }

  def matAsEdges[M[_]:Matrix](mat: FoldSignal[M])(g: GraphDB[M]): EdgesRes = {
    val m = mat.m
    try {
      EdgesRes(g.renderEdges(m))
    } finally {
      if (mat.isIntermediate) {
        Matrix[M].release(m)
      }
    }
  }

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
