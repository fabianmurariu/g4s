package com.github.fabianmurariu.g4s.graph

import cats.Parallel
import cats.effect.{Concurrent, Resource}
import cats.implicits._
import com.github.fabianmurariu.g4s.sparse.grb.{
  BuiltInBinaryOps,
  GRB,
  GrBSemiring,
  MxM,
  SparseMatrixHandler
}
import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.g4s.traverser._

import scala.reflect.runtime.universe.{Traverser => _, _}
import fs2.Chunk
import scala.reflect.ClassTag
import cats.effect.concurrent.Ref
import com.github.fabianmurariu.g4s.sparse.grb.GrBInvalidIndex

/**
  * Concurrent graph implementation
  * does not do transactions
  */
class ConcurrentDirectedGraph[F[_], V, E](
    edges: BlockingMatrix[F, Boolean],
    edgesTranspose: BlockingMatrix[F, Boolean],
    edgeTypes: LabelledMatrices[F],
    edgeTypesTranspose: LabelledMatrices[F],
    nodeLabels: LabelledMatrices[F],
    semiRing: GrBSemiring[Boolean, Boolean, Boolean],
    ds: DataStore[F, V, E]
)(implicit F: Concurrent[F], P: Parallel[F], G: GRB) { self =>


  /**
   * Only call this inside use block of a [[BlockingMatrix]]
   * */
  private def update(src:Long, dst:Long)(mat: GrBMatrix[F, Boolean]):F[Unit] = {

    def loopUpdateAndResize:F[Unit] =
      mat.set(src, dst, true)
        .handleErrorWith{case _:GrBInvalidIndex =>
          (for {
            matShape <- mat.shape
            (rows, cols) = matShape
            _ <- mat.resize(rows * 2, cols * 2)
          } yield ()) *> loopUpdateAndResize
        }

    loopUpdateAndResize
  }

  def getV(id: Long): F[Option[V]] = ds.getV(id)

  /**
    * Insert the vertex and get back the id
    */
  def insertVertex[T <: V](v: T)(implicit tt: TypeTag[T]): F[Long] = {
    // persist on the DataStore
    // possibly resize?
    for {
      id <- ds.persistV(v)
      tpe = tt.tpe.toString
      nodeTpeMat <- nodeLabels.getOrCreate(tpe)
      _ <- nodeTpeMat.use(update(id, id))
    } yield id
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
  ): F[Unit] = {
    val tpe = tt.tpe.toString
    edges.use(update(src, dst)) &> // update edges
      edgesTranspose.use(update(dst, src)) &> // update edges transpose
      (edgeTypes.getOrCreate(tpe) >>= (_.use(update(src, dst)))) &> // update edges for type
      (edgeTypesTranspose.getOrCreate(tpe) >>= (_.use(update(dst, src)))) &> // update edges for type
      ds.persistE(src, dst, e) // write the edge to the datastore
  }

  /**
    *
    * Scan over a blocking matrix and return nodes
    * @param m
    */
  def scanNodes(m: BlockingMatrix[F, Boolean]): fs2.Stream[F, V] = {
    m.toStream().flatMap {
      case (_, js, _) => fs2.Stream.eval(ds.getVs(js))
      .flatMap(arr => fs2.Stream.chunk(Chunk.array(arr)))
    }
  }
}


object ConcurrentDirectedGraph {
  def apply[F[_]: Parallel: Concurrent, V, E](
      implicit G: GRB, CT: ClassTag[V]
  ): Resource[F, ConcurrentDirectedGraph[F, V, E]] =
    for {
      size <- Resource.liftF(Ref.of[F, (Long, Long)]((1024L, 1024L)))
      edges <- BlockingMatrix[F, Boolean](size)
      edgesT <- BlockingMatrix[F, Boolean](size)
      edgeTypes <- LabelledMatrices[F](size)
      edgeTypesTranspose <- LabelledMatrices[F](size)
      nodeLabels <- LabelledMatrices[F](size)
      ds <- Resource.liftF(DataStore.default[F, V, E])
      semiRing <- GrBSemiring[F, Boolean, Boolean, Boolean](
        BuiltInBinaryOps.boolean.any,
        BuiltInBinaryOps.boolean.pair,
        false
      )
    } yield new ConcurrentDirectedGraph(
      edges,
      edgesT,
      edgeTypes,
      edgeTypesTranspose,
      nodeLabels,
      semiRing,
      ds
    )
}