package com.github.fabianmurariu.g4s.graph

import cats.Parallel
import cats.effect.{Concurrent, Resource}
import cats.implicits._
import com.github.fabianmurariu.g4s.sparse.grb.{
  BuiltInBinaryOps,
  GRB,
  GrBSemiring
}
import com.github.fabianmurariu.g4s.sparse.grbv2.{GrBMatrix, MxM, Diag}

import scala.reflect.runtime.universe.{
  Traverser => _,
  Bind => _,
  Select => _,
  _
}
import fs2.Chunk
import scala.reflect.ClassTag
import cats.effect.concurrent.Ref
import com.github.fabianmurariu.g4s.sparse.grb.GrBInvalidIndex
import cats.data.State
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

  sealed trait PlanOutput
  case class Releasable(g: GrBMatrix[F, Boolean]) extends PlanOutput
  case class UnReleasable(g: BlockingMatrix[F, Boolean]) extends PlanOutput
  case class IdTable(table: Seq[ArrayBuffer[Long]]) extends PlanOutput

  sealed trait IdScan
  case class Return1(ret: Array[Long]) extends IdScan
  case class Return2(ret1: Array[Long], ret2: Array[Long]) extends IdScan
  case class ReturnN(ret: Seq[ArrayBuffer[Long]]) extends IdScan


  def lookupEdges(
      tpe: String,
      transpose: Boolean
  ): F[(BlockingMatrix[F, Boolean], Long)] = transpose match {
    case true =>
      for {
       mat <- edgeTypesTranspose.getOrCreate(tpe)
       card <- mat.use(_.nvals)
      } yield (mat, card)
    case false =>
      for {
       mat <- edgeTypes.getOrCreate(tpe)
       card <- mat.use(_.nvals)
      } yield (mat, card)
  }

  def lookupNodes(tpe:String):F[(BlockingMatrix[F, Boolean], Long)] = {
    for {
      mat <- nodeLabels.getOrCreate(tpe)
      card <- mat.use(_.nvals)
    } yield (mat, card)
  }

  def joinResults(a: PlanOutput, b: PlanOutput): F[PlanOutput] = (a, b) match {
    case (Releasable(aMat), Releasable(bMat)) =>
      for {
        tplsA <- GrBTuples.fromGrBExtract(aMat.extract)
        tplsB <- GrBTuples.fromGrBExtract(bMat.extract)
        _ <- F.delay {
          println(tplsA.show)
          println(tplsB.show)
        }
        out <- F.delay {
          GrBTuples.rowJoinOnBinarySearch(tplsA.asRows, 1, tplsB)
        }
      } yield IdTable(out)
  }

  def matMul(a: PlanOutput, b: PlanOutput): F[PlanOutput] = (a, b) match {
    case (Releasable(from), Releasable(to)) =>
      F.bracket(F.pure(from)) { from => MxM[F].mxm(to)(from, to)(semiRing) }(
          _.release
        )
        .map(Releasable)
    case (UnReleasable(fromBM), UnReleasable(toBM)) =>
      fromBM.use { from =>
        toBM.use { to =>
          for {
            shape <- to.shape
            (rows, cols) = shape
            out <- GrBMatrix.unsafe[F, Boolean](rows, cols)
            output <- MxM[F].mxm(out)(from, to)(semiRing)
          } yield Releasable(output)
        }
      }
    case (UnReleasable(fromBM), Releasable(to)) =>
      fromBM.use { from => MxM[F].mxm(to)(from, to)(semiRing) }.map(Releasable)
    case (Releasable(from), UnReleasable(toBM)) =>
      toBM.use { to => MxM[F].mxm(from)(from, to)(semiRing) }.map(Releasable)
  }

  def grbEval(
      output: GrBMatrix[F, Boolean],
      from: GrBMatrix[F, Boolean],
      edge: GrBMatrix[F, Boolean],
      to: GrBMatrix[F, Boolean]
  ): F[GrBMatrix[F, Boolean]] = {
    MxM[F]
      .mxm(output)(from, edge)(semiRing, None, None, None) >>= {
      res: GrBMatrix[F, Boolean] =>
        MxM[F].mxm(res)(res, to)(semiRing, None, None, None)
    }
  }

  def evalAlgebra(
      fromPM: PlanOutput,
      edgeBM: BlockingMatrix[F, Boolean],
      toPM: PlanOutput
  ): F[Releasable] =
    (fromPM, toPM) match {
      case (Releasable(from), Releasable(to)) =>
        edgeBM
          .use { edge =>
            F.bracket(F.pure(to)) {
              grbEval(from, from, edge, _)
            }(_.release)
          }
          .map(Releasable)
      case (UnReleasable(fromBM), UnReleasable(toBM)) =>
        edgeBM.use { edge =>
          fromBM.use { from =>
            toBM.use { to =>
              for {
                shape <- to.shape
                (rows, cols) = shape
                out <- GrBMatrix.unsafe[F, Boolean](rows, cols)
                output <- grbEval(out, from, edge, to)
              } yield Releasable(output)
            }
          }
        }
      case (UnReleasable(fromBM), Releasable(to)) =>
        edgeBM
          .use { edge => fromBM.use { from => grbEval(to, from, edge, to) } }
          .map(Releasable)
      case (Releasable(from), UnReleasable(toBM)) =>
        edgeBM
          .use { edge => toBM.use { to => grbEval(from, from, edge, to) } }
          .map(Releasable)
    }

  /**
    * make an expansion matrix into a filter matrix
    * */
  def expandToFilter(
      expand: PlanOutput
  ): F[GrBMatrix[F, Boolean]] = {
    F.bracket(F.pure(expand)) {
      case Releasable(m) =>
        for {
          s <- m.shape
          (rows, cols) = s
          filter <- GrBMatrix.unsafe[F, Boolean](rows, cols)
          _ <- m.reduceColumns(BuiltInBinaryOps.boolean.any).use { v =>
            Diag[F].diag(filter)(v)
          }
        } yield filter

    } { case Releasable(m) => m.release }
  }

  /**
    * Only call this inside use block of a [[BlockingMatrix]]
    * */
  private def update(src: Long, dst: Long)(
      mat: GrBMatrix[F, Boolean]
  ): F[Unit] = {

    def loopUpdateAndResize: F[Unit] =
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
      case (_, js, _) =>
        fs2.Stream
          .eval(ds.getVs(js))
          .flatMap(arr => fs2.Stream.chunk(Chunk.array(arr)))
    }
  }
}

object ConcurrentDirectedGraph {
  def apply[F[_]: Parallel: Concurrent, V, E](
      implicit G: GRB,
      CT: ClassTag[V]
  ): Resource[F, ConcurrentDirectedGraph[F, V, E]] =
    for {
      size <- Resource.liftF(Ref.of[F, (Long, Long)]((4L * 1024L, 4L * 1024L)))
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
