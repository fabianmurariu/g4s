package com.github.fabianmurariu.g4s.graph

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
import com.github.fabianmurariu.g4s.sparse.grb.GrBInvalidIndex
import com.github.fabianmurariu.g4s.matrix.BlockingMatrix
import scala.collection.mutable.ArrayBuffer
import com.github.fabianmurariu.g4s.optim.EvaluatorGraph
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.kernel.MonadCancel
import cats.effect.kernel.Resource

/**
  * Concurrent graph implementation
  * does not do transactions
  */
class ConcurrentDirectedGraph[V:ClassTag, E](
    nodes: BlockingMatrix[Boolean],
    edges: BlockingMatrix[Boolean],
    edgesTranspose: BlockingMatrix[Boolean],
    edgeTypes: LabelledMatrices,
    edgeTypesTranspose: LabelledMatrices,
    nodeLabels: LabelledMatrices,
    semiRing: GrBSemiring[Boolean, Boolean, Boolean],
    ds: DataStore[V, E]
)(implicit val G: GRB)
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

  // def joinResults(a: PlanOutput, b: PlanOutput): IO[PlanOutput] = (a, b) match {
  //   case (Releasable(aMat), Releasable(bMat)) =>
  //     for {
  //       tplsA <- GrBTuples.fromGrBExtract(aMat.extract)
  //       tplsB <- GrBTuples.fromGrBExtract(bMat.extract)
  //       _ <- F.delay {
  //         println(tplsA.show)
  //         println(tplsB.show)
  //       }
  //       out <- F.delay {
  //         GrBTuples.rowJoinOnBinarySearch(tplsA.asRows, 1, tplsB)
  //       }
  //     } yield IdTable(out)
  // }

  // def matMul(a: PlanOutput, b: PlanOutput): IO[PlanOutput] = (a, b) match {
  //   case (Releasable(from), Releasable(to)) =>
  //     F.bracket(IO.delay(from)) { from => MxM[F].mxm(to)(from, to)(semiRing) }(
  //         _.release
  //       )
  //       .map(Releasable)
  //   case (UnReleasable(fromBM), UnReleasable(toBM)) =>
  //     fromBM.use { from =>
  //       toBM.use { to =>
  //         for {
  //           shape <- to.shape
  //           (rows, cols) = shape
  //           out <- GrBMatrix.unsafe[Boolean](rows, cols)
  //           output <- MxM[F].mxm(out)(from, to)(semiRing)
  //         } yield Releasable(output)
  //       }
  //     }
  //   case (UnReleasable(fromBM), Releasable(to)) =>
  //     fromBM.use { from => MxM[F].mxm(to)(from, to)(semiRing) }.map(Releasable)
  //   case (Releasable(from), UnReleasable(toBM)) =>
  //     toBM.use { to => MxM[F].mxm(from)(from, to)(semiRing) }.map(Releasable)
  // }

  def grbEval(
      output: GrBMatrix[IO, Boolean],
      from: GrBMatrix[IO, Boolean],
      edge: GrBMatrix[IO, Boolean],
      to: GrBMatrix[IO, Boolean]
  ): IO[GrBMatrix[IO, Boolean]] = {
    MxM[IO]
      .mxm(output)(from, edge)(semiRing, None, None, None) >>= {
      res: GrBMatrix[IO, Boolean] =>
        MxM[IO].mxm(res)(res, to)(semiRing, None, None, None)
    }
  }

  def evalAlgebra(
      fromPM: PlanOutput,
      edgeBM: BlockingMatrix[Boolean],
      toPM: PlanOutput
  ): IO[Releasable] =
    (fromPM, toPM) match {
      case (Releasable(from), Releasable(to)) =>
        edgeBM
          .use { edge =>
            MonadCancel[IO].bracket(IO.pure(to)) {
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
                out <- GrBMatrix.unsafe[IO, Boolean](rows, cols)
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
  ): IO[GrBMatrix[IO, Boolean]] = {
    MonadCancel[IO].bracket(IO.delay(expand)) {
      case Releasable(m) =>
        for {
          s <- m.shape
          (rows, cols) = s
          filter <- GrBMatrix.unsafe[IO, Boolean](rows, cols)
          _ <- m.reduceColumns(BuiltInBinaryOps.boolean.any).use { v =>
            Diag[IO].diag(filter)(v)
          }
        } yield filter

    } { case Releasable(m) => m.release }
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
    for {
      id <- ds.persistV(v)
      tpe = tt.tpe.toString
      nodeTpeMat <- nodeLabels.getOrCreate(tpe)
      _ <- nodeTpeMat.use(update(id, id))
      _ <- nodes.use(update(id, id))
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
  ): IO[Unit] = {
    val tpe = tt.tpe.toString
    edges.use(update(src, dst)) *> // update edges
      edgesTranspose.use(update(dst, src)) *> // update edges transpose
      (edgeTypes.getOrCreate(tpe) >>= (_.use(update(src, dst)))) *> // update edges for type
      (edgeTypesTranspose.getOrCreate(tpe) >>= (_.use(update(dst, src)))) *> // update edges for type
      ds.persistE(src, dst, e) // write the edge to the datastore
  }

  /**
    *
    * Scan over a blocking matrix and return nodes
    * @param m
    */
  def scanNodes(m: BlockingMatrix[Boolean]): fs2.Stream[IO, V] = {
    m.toStream().flatMap {
      case (_, js, _) =>
        fs2.Stream
          .eval(ds.getVs(js))
          .flatMap(arr => fs2.Stream.chunk(Chunk.array(arr)))
    }
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
      semiRing <- GrBSemiring[IO, Boolean, Boolean, Boolean](
        BuiltInBinaryOps.boolean.any,
        BuiltInBinaryOps.boolean.pair,
        false
      )
    } yield new ConcurrentDirectedGraph(
      nodes,
      edges,
      edgesT,
      edgeTypes,
      edgeTypesTranspose,
      nodeLabels,
      semiRing,
      ds
    )

}
