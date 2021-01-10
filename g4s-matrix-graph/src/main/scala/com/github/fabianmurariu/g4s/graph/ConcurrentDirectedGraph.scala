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

import scala.reflect.runtime.universe.{Traverser => _, Bind => _, _}
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

  sealed trait PlanMatrix
  case class Releasable(g: GrBMatrix[F, Boolean]) extends PlanMatrix
  case class UnReleasable(g: BlockingMatrix[F, Boolean]) extends PlanMatrix
  // TODO: the graph should be able to evaluate a traverser object
  // very likely Traverser will need to be redefined
  // there could be a sub type of graph supporting only non-FP
  // access to GrBMatrix without any generics and only with Long indices
  // that can evaluate PlanStep instead of traverser
  def evalPlans(ret: NodeRef*)(
      bindings: LookupTable[cats.Id, PlanStep]
  ): F[LookupTable[F, PlanMatrix]] = {

    def resolveBinding(from: Bind[cats.Id, PlanStep])(
        matBindings: LookupTable[F, PlanMatrix]
    ) = bindings(from.key) match {

      case LValue(rc, plan) if rc <= 1 =>
        // we should not hold on to the output of this plan
        matBindings.lookup(from.key).flatMap {
          case None =>
            // we compute but do not store, the mat is needed but only once
            F.delay(bindings.decrement(from.key)) *> resolvePlan(plan)(
              matBindings
            )
          case Some(mat) =>
            F.delay(bindings.decrement(from.key)) *> //clear from planning
              F.suspend(matBindings.decrement(from.key)) *> // clear from mat bindings
              F.pure(mat)
        }
      case LValue(_, plan) =>
        // we compute and store this plan since it will be used again
        matBindings
          .lookupOrBuildPlan(from.key) {
            F.delay(bindings.decrement(from.key)) *> resolvePlan(plan)(
              matBindings
            )
          }
          .flatMap { // do not release this matrix if it will be re-used
            case Releasable(m) =>
              BlockingMatrix.fromGrBMatrix(m).map(UnReleasable)
            case keep: UnReleasable => F.pure(keep)
          }

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
        fromPM: PlanMatrix,
        edgeBM: BlockingMatrix[F, Boolean],
        toPM: PlanMatrix
    ): F[PlanMatrix] =
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
                  out <- F.delay(GrBMatrix.unsafe[F, Boolean](rows, cols))
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
      * go through the plan and resolve matrices
      * MxM against the semiring at the Expansion points
      * Only release intermediate results
      * */
    def resolvePlan(ps: PlanStep)(
        matBindings: LookupTable[F, PlanMatrix]
    ): F[PlanMatrix] = ps match {
      case Expand(from, tpe, to: LoadNodes, transpose) =>
        F.suspend {
          val fromGrBMat = resolveBinding(from)(matBindings)

          val edgeDirection =
            if (transpose) edgeTypesTranspose.getOrCreate(tpe)
            else edgeTypes.getOrCreate(tpe)

          for {
            edgeB <- edgeDirection
            toB <- resolvePlan(to)(matBindings)
            fromB <- fromGrBMat
            out <- evalAlgebra(fromB, edgeB, toB)
          } yield out

        }
      case LoadNodes(ref) =>
        nodeLabels.getOrCreate(ref.name).map(UnReleasable)

      case _ => F.raiseError(new NotImplementedError)
    }

    for {
      table <- F.suspend(LookupTable[F, PlanMatrix])
      _ <- ret.toStream
        .foldLeftM[F, LookupTable[F, PlanMatrix]](table) { (t, ret) =>
          val key = LKey(ret, Set.empty)
          for {
            binding <- F.delay(bindings(key))
            LValue(_, plan) = binding
            _ <- t.lookupOrBuildPlan(key)(resolvePlan(plan)(t))
          } yield t
        }
    } yield table

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
