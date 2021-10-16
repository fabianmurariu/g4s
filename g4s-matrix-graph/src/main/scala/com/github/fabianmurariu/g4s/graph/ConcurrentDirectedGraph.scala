package com.github.fabianmurariu.g4s.graph

import cats.implicits._
import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grb._

import scala.reflect.runtime.universe.{
  Traverser => _,
  Bind => _,
  Select => _,
  _
}
import scala.reflect.ClassTag
import com.github.fabianmurariu.g4s.sparse.grb.GrBInvalidIndex
import com.github.fabianmurariu.g4s.matrix.BlockingMatrix
import scala.collection.mutable.ArrayBuffer
import com.github.fabianmurariu.g4s.optim.EvaluatorGraph
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.kernel.MonadCancel
import cats.effect.kernel.Resource
import cats.effect.std.Queue
import com.github.fabianmurariu.g4s.optim.StatsStore

/**
  * Concurrent graph implementation
  * does not do transactions
  */
class ConcurrentDirectedGraph[V: ClassTag, E](
    nodes: BlockingMatrix[Boolean],
    edges: BlockingMatrix[Boolean],
    edgesTranspose: BlockingMatrix[Boolean],
    edgeTypes: LabelledMatrices,
    edgeTypesTranspose: LabelledMatrices,
    nodeLabels: LabelledMatrices,
    ds: DataStore[V, E],
    store: Queue[IO, StatsStore]
)
    extends EvaluatorGraph {

  self =>

  sealed trait PlanOutput
  case class Releasable(g: GrBMatrix[IO, Boolean]) extends PlanOutput
  case class UnReleasable(g: BlockingMatrix[Boolean]) extends PlanOutput
  case class IdTable(table: Iterable[ArrayBuffer[Long]]) extends PlanOutput

  sealed trait IdScan
  case class Return1(ret: Array[Long]) extends IdScan
  case class Return2(ret1: Array[Long], ret2: Array[Long]) extends IdScan
  case class ReturnN(ret: Seq[ArrayBuffer[Long]]) extends IdScan

  override def withStats[B](f: StatsStore => IO[B]): IO[B] =
    MonadCancel[IO].bracket(store.take)(f)(store.offer)
  override def lookupEdges(
      tpe: Option[String],
      transpose: Boolean
  ): IO[(BlockingMatrix[Boolean], Long)] = transpose match {
    case true =>
      for {
        mat <- tpe
          .map(edgeTypesTranspose.getOrCreate(_))
          .getOrElse(IO.delay(edgesTranspose))
        card <- mat.use(_.nvals)
      } yield (mat, card)
    case false =>
      for {
        mat <- tpe.map(edgeTypes.getOrCreate(_)).getOrElse(IO.delay(edges))
        card <- mat.use(_.nvals)
      } yield (mat, card)
  }

  override def lookupNodes(
      tpe: Option[String]
  ): IO[(BlockingMatrix[Boolean], Long)] = {
    for {
      mat <- tpe.map(nodeLabels.getOrCreate(_)).getOrElse(IO.delay(nodes))
      card <- mat.use(_.nvals)
    } yield (mat, card)
  }

  /**
    * Only call this inside use block of a [[BlockingMatrix]]
    * */
  private def update(src: Long, dst: Long)(
      mat: GrBMatrix[IO, Boolean]
  ): IO[Unit] = {

    def loopUpdateAndResize: IO[Unit] =
      mat
        .set(src, dst, true)
        .handleErrorWith {
          case _: GrBInvalidIndex =>
            (for {
              matShape <- mat.shape
              (rows, cols) = matShape
              _ <- mat.resize(rows * 2, cols * 2)
            } yield ()) *> loopUpdateAndResize
        }

    loopUpdateAndResize
  }

  def getV(id: Long): IO[Option[V]] = ds.getV(id)

  /**
    * Insert the vertex and get back the id
    */
  def insertVertex[T <: V](v: T)(implicit tt: TypeTag[T]): IO[Long] = {
    // persist on the DataStore
    // possibly resize?
    val  tpe = tt.tpe.toString

    val doWork = for {
      id <- ds.persistV(v)
      nodeTpeMat <- nodeLabels.getOrCreate(tpe)
      _ <- nodeTpeMat.use(update(id, id))
      _ <- nodes.use(update(id, id))
    } yield id

    val updateStats = withStats(store => IO(store.addNode(tpe))).start

    
    doWork <* updateStats
  }

  /**
    * Insert edge into the graph
    * this is idempotent
    *
    * @param src
    * source node
    * @param dst
    * dest node
    * @param e
    * edge metadata
    * @param tt
    * type info TODO: should replace with shapeless
    * @return
    */
  def insertEdge[T <: E](src: Long, dst: Long, e: T)(
      implicit tt: TypeTag[T]
  ): IO[Unit] = {
    val tpe = tt.tpe.toString
    val doWork = edges.use(update(src, dst)) *> // update edges
      edgesTranspose.use(update(dst, src)) *> // update edges transpose
      (edgeTypes.getOrCreate(tpe) >>= (_.use(update(src, dst)))) *> // update edges for type
      (edgeTypesTranspose.getOrCreate(tpe) >>= (_.use(update(dst, src)))) *> // update edges for type
      ds.persistE(src, dst, e) // write the edge to the datastore

      val stats = withStats(stats => IO(stats.addEdge(tpe))).start
      doWork <&  stats
  }

  
}

object ConcurrentDirectedGraph {
  def apply[V, E](
      implicit G: GRB,
      CT: ClassTag[V]
  ): Resource[IO, ConcurrentDirectedGraph[V, E]] =
    for {
      size <- Resource.eval(Ref[IO].of((4L * 1024L, 4L * 1024L)))
      edges <- BlockingMatrix[Boolean](size)
      nodes <- BlockingMatrix[Boolean](size)
      edgesT <- BlockingMatrix[Boolean](size)
      edgeTypes <- LabelledMatrices(size)
      edgeTypesTranspose <- LabelledMatrices(size)
      nodeLabels <- LabelledMatrices(size)
      ds <- Resource.eval(DataStore.default[V, E])
      stats <- Resource.eval(
        Queue
          .bounded[IO, StatsStore](1)
          .flatMap(q => q.offer(StatsStore()).map(_ => q))
      )
    } yield new ConcurrentDirectedGraph(
      nodes,
      edges,
      edgesT,
      edgeTypes,
      edgeTypesTranspose,
      nodeLabels,
      ds,
      stats
    )

}
