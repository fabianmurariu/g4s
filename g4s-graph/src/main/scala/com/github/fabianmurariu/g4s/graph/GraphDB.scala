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

/**
  * Naive first pass implementation of a Graph database
  * over sparse matrices that may be of type GraphBLAS
  *
  */
sealed class GraphDB[M[_]](
    private[graph] val nodes: M[Boolean],
    edgesOut: M[Boolean],
    edgesIn: M[Boolean],
    relTypes: mutable.Map[String, M[Boolean]],
    labelIndex: mutable.Map[String, mutable.Set[Long]], // node labels and node ids
    nodeData: mutable.Map[Long, Set[String]],
    var count: Long
)(implicit M: Matrix[M], MH:MatrixHandler[M, Boolean])
    extends AutoCloseable { self =>

  def close(): Unit = {
    M.release(nodes)
    M.release(edgesOut)
    M.release(edgesIn)
    relTypes.valuesIterator.foreach(M.release)
  }

  def insertVertex(labels:Set[String]):Long = {
    val vxId = count;
    M.set(nodes)(vxId, vxId, true)

    labelIndex.foreach{
      case (label, nodes) =>
        if(labels(label))
          nodes += vxId
    }

    nodeData.put(vxId, labels)

    count+=1
    vxId
  }

  def insertEdge(src:Long, tpe:String, dest:Long): (Long, Long) = {
    M.set(edgesOut)(src, dest, true)
    M.set(edgesIn)(dest, src, true)

    val tpeMat = relTypes.getOrElseUpdate(tpe, M.make[Boolean](16, 16))
    M.set(tpeMat)(src, dest, true)

    (src, dest)

  }

  /**
   * transform a matrix of vertices to the external world
   * view of Id + Labels
   */
  def labeledVertices(m:M[Boolean]): Vector[(Long, Set[String])] = {
    val (_, _, js) = MH.copyData(m)

    js.map(vId => vId -> self.nodeData.get(vId).getOrElse(Set.empty)).toVector
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
          0L
        )
      )
    )
  }
}

sealed trait GraphStep[+T]

case class CreateNode(labels: Vector[String]) extends GraphStep[Long]
case class CreateEdge(src:Long, tpe: String, dest:Long) extends GraphStep[(Long, Long)]
case class QueryStep(q:Query) extends GraphStep[QueryResult]

sealed trait Direction
case object Undirected extends Direction
case object Out extends Direction
case object In extends Direction

sealed trait Query { self =>
  /**
   * (a) -> (b)
   */
  def out = FlatMap(self, AllEdgesOut)

  /**
   * (a) <- (b)
   */
  def in = FlatMap(self, AllEdgesIn)

  /**
   * (a) -[edgeTypes..]-> (b)
   */
  def out(edgeType:String, edgeTypes:String*) =
    FlatMap(self, Edges((edgeType +: edgeTypes).toSet, Out))

  /**
   * (a) <-[edgeTypes..]- (b)
   */
  def in(edgeType:String, edgeTypes:String*) =
    FlatMap(self, Edges((edgeType +: edgeTypes).toSet, In))

  /**
   * (a) -> (b:vertexLabel ..)
   */
  def outV(vertexLabel:String, vertexLabels: String*) =
    FlatMap(out, Vertex((vertexLabel +: vertexLabels).toSet))

  /**
   * (a) <- (b:vertexLabel ..)
   */
  def inV(vertexLabel:String, vertexLabels: String*) =
    FlatMap(in, Vertex((vertexLabel +: vertexLabels).toSet))

  /**
   * .. (b: vertexLabel)
   */
  def v(vertexLabel:String, vertexLabels: String*) =
    FlatMap(self, Vertex((vertexLabel +: vertexLabels).toSet))
}

case object AllVertices extends Query
case object AllEdges extends Query
case object AllEdgesOut extends Query
case object AllEdgesIn extends Query

case class Edges(types:Set[String], direction: Direction) extends Query
case class Vertex(labels: Set[String]) extends Query

case class FlatMap(cur:Query, next: Query) extends Query

sealed trait QueryResult
case class Vertices(vs:Vector[(Long, Set[String])]) extends QueryResult


object GraphStep {

  type Step[T] = Free[GraphStep, T]

  def interpreter[M[_]](g: GraphDB[M]) = new (GraphStep ~> Observable) {
    def apply[A](fa: GraphStep[A]): Observable[A] = fa match {
      case CreateNode(labels) => Observable.fromTask(
        Task.delay(g.insertVertex(labels.toSet))
      )

      case CreateEdge(src, tpe, dest) => Observable.fromTask(
        Task.delay(g.insertEdge(src, tpe, dest))
      )

      case QueryStep(AllVertices) =>

        Observable.fromTask(
          Task.delay(
            matAsVertices(g.nodes)(g)
          ))
    }

  }


  def matAsVertices[M[_]](mat:M[Boolean])(g: GraphDB[M]):Vertices = {
    Vertices(g.labeledVertices(mat))
  }

  def createNode(label:String, labels:String*) = {
    Free.liftF(CreateNode((label +: labels).toVector))
  }

  def createEdge(src:Long, tpe:String, dest:Long) = {
    Free.liftF(CreateEdge(src, tpe, dest))
  }

  def query(q:Query) =
    Free.liftF(QueryStep(q))

  def vs = AllVertices

}
