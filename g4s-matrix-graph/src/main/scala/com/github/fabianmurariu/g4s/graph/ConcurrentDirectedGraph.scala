package com.github.fabianmurariu.g4s.graph

import cats.Parallel
import cats.effect.concurrent.Semaphore
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

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.{Traverser => _, _}
import fs2.Chunk
import scala.reflect.ClassTag

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
      _ <- nodeTpeMat.use(_.set(id, id, true))
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
    edges.use(_.set(src, dst, true)) &> // update edges
      edgesTranspose.use(_.set(dst, src, true)) &> // update edges transpose
      (edgeTypes.getOrCreate(tpe) >>= (_.use(_.set(src, dst, true)))) &> // update edges for type
      (edgeTypesTranspose.getOrCreate(tpe) >>= (_.use(_.set(dst, src, true)))) &> // update edges for type
      ds.persistE(src, dst, e) // write the edge to the datastore
  }

  /**
    * This is how we get stuff out
    * the core of the graph,
    * any other extraction function should rely on eval
    * @param p
    * the plan on how to get stuff out of the graph
    * @return
    *
    */
  def evalToMatrix(
      p: MasterPlan
  ): F[Map[NodeRef, BlockingMatrix[F, Boolean]]] = {

    def foldPlan(plan: BasicPlan): F[BlockingMatrix[F, Boolean]] = plan match {
      case Expand(NodeLoad(leftNode), NodeLoad(rightNode), name, false) =>
        // a pure case where we produce a matrix output we do not override
        // FIXME: the biggest issue here is the size of the matrix, we might need a global lock when resizing matrices, or resize them on the fly when GRB fails
        for {
          a <- nodeLabels.getOrCreate(leftNode)
          b <- nodeLabels.getOrCreate(rightNode)
          e <- edgeTypes.getOrCreate(name)
          size <- e.use(_.nrows)
          out <- F.delay(GrBMatrix.unsafe[F, Boolean](size, size))
          resMat <- a.use { aMat =>
            b.use { bMat =>
              e.use { eMat =>
                MxM[F]
                  .mxm(out)(aMat, eMat)(semiRing)
                  .flatMap(interim =>
                    MxM[F].mxm(interim)(interim, bMat)(semiRing)
                  )
              }
            }
          }
          res <- BlockingMatrix.fromGrBMatrix(resMat)
        } yield res
      case _ => F.raiseError(new NotImplementedError)
    }

    p.plans.toVector.foldLeftM(Map.empty[NodeRef, BlockingMatrix[F, Boolean]]) {
      case (acc, (key, plan)) =>
        foldPlan(plan).map(m => acc + (key -> m))
    }
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

class LabelledMatrices[F[_]](
    private[graph] val mats: ConcurrentHashMap[
      String,
      BlockingMatrix[F, Boolean]
    ]
)(implicit F: Concurrent[F], G: GRB) {

  private def defaultProvider
      : F[java.util.function.Function[String, BlockingMatrix[F, Boolean]]] =
    for {
      lock <- Semaphore[F](1)
    } yield _ => new BlockingMatrix(lock, GrBMatrix.unsafe[F, Boolean](16, 16))

  def getOrCreate(label: String): F[BlockingMatrix[F, Boolean]] =
    for {
      matMaker <- defaultProvider
      mat <- F.delay(mats.computeIfAbsent(label, matMaker))
    } yield mat

}

object LabelledMatrices {
  def apply[F[_]](
      implicit G: GRB,
      F: Concurrent[F]
  ): Resource[F, LabelledMatrices[F]] =
    Resource.make(F.pure(new LabelledMatrices[F](new ConcurrentHashMap()))) {
      labelledMats =>
        //FIXME: this should have some concurrency issues if not all computations have finished
        labelledMats.mats
          .values()
          .iterator()
          .asScala
          .toStream
          .foldLeftM(())((_, mat) => mat.release)
    }
}

object ConcurrentDirectedGraph {
  def apply[F[_]: Parallel: Concurrent, V, E](
      implicit G: GRB, CT: ClassTag[V]
  ): Resource[F, ConcurrentDirectedGraph[F, V, E]] =
    for {
      edges <- BlockingMatrix[F, Boolean](16, 16)
      edgesT <- BlockingMatrix[F, Boolean](16, 16)
      edgeTypes <- LabelledMatrices[F]
      edgeTypesTranspose <- LabelledMatrices[F]
      nodeLabels <- LabelledMatrices[F]
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
