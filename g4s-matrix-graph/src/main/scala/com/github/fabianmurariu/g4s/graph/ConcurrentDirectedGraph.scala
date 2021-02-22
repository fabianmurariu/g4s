package com.github.fabianmurariu.g4s.graph

import cats.Parallel
import cats.effect.{Concurrent, Resource}
import cats.implicits._
import com.github.fabianmurariu.g4s.sparse.grb.{
  BuiltInBinaryOps,
  GRB,
  GrBSemiring,
  MxM
}
import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.g4s.traverser._

import scala.reflect.runtime.universe.{Traverser => _, Bind => _, _}
import fs2.Chunk
import scala.reflect.ClassTag
import cats.effect.concurrent.Ref
import com.github.fabianmurariu.g4s.sparse.grb.GrBInvalidIndex
import cats.data.State
import scala.collection.mutable
import com.github.fabianmurariu.g4s.traverser.Traverser.QGEdges
import com.github.fabianmurariu.g4s.traverser.Traverser.Ret
import com.github.fabianmurariu.g4s.sparse.grb.Diag
import com.github.fabianmurariu.g4s.traverser.LogicalPlan.Step
import com.github.fabianmurariu.g4s.traverser.LogicalPlan.LoadNodes
import com.github.fabianmurariu.g4s.traverser.LogicalPlan.Expand
import com.github.fabianmurariu.g4s.traverser.LogicalPlan.Filter

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

  type QueryGraph = mutable.Map[NodeRef, QGEdges]
  type Plans = Map[NodeRef, PlanMatrix]
  type Context = Ref[F, Plans]

  def resolveTraverser(
      t: State[QueryGraph, Ret]
  ): fs2.Stream[F, Vector[V]] = {

    val (qg, ret) = t.run(mutable.Map.empty[NodeRef, QGEdges]).value

    val plans = LogicalPlan.compilePlans(qg)(ret.ns).toVector.zip(ret.ns)

    val planMat: F[Plans] =
      Ref.of(Map.empty[NodeRef, PlanMatrix]).flatMap { context =>
        plans
          .traverse_ {
            case (step, n) =>
              (F.delay(step.deref.get) >>= foldEvalPlan(context))
                .flatMap { case (_, mat) => context.update(_.+(n -> mat)) }
          }
          .flatMap(_ => context.get)

      }


    ???
  }

  def foldEvalPlan(
      context: Context
  )(step: Step): F[(Context, PlanMatrix)] = step match {
    case LoadNodes(ref) =>
      nodeLabels.getOrCreate(ref.name).map(m => context -> UnReleasable(m))
    case Expand(from, to, true) =>
      for {
        fromS <- F.delay(from.deref.get)
        a <- foldEvalPlan(context)(fromS)
        (ctx1, fromMat) = a
        edgeMat <- edgeTypesTranspose.getOrCreate(to)

        expMat <- matMul(fromMat, UnReleasable(edgeMat))
      } yield (ctx1, expMat)

    case Expand(from, to, false) =>
      for {
        fromS <- F.delay(from.deref.get)
        a <- foldEvalPlan(context)(fromS)
        (ctx1, fromMat) = a
        edgeMat <- edgeTypes.getOrCreate(to)

        expMat <- matMul(fromMat, UnReleasable(edgeMat))
      } yield (ctx1, expMat)
    case Filter(expand, LoadNodes(n)) =>
      for {
        expandS <- F.delay(expand.deref.get)
        a <- foldEvalPlan(context)(expandS)
        (ctx1, expandMat) = a
        nodeMat <- nodeLabels.getOrCreate(n.name)
        filterMat <- matMul(expandMat, UnReleasable(nodeMat))
      } yield (ctx1, filterMat)
  }

  def matMul(a: PlanMatrix, b: PlanMatrix): F[PlanMatrix] = (a, b) match {
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
      fromPM: PlanMatrix,
      edgeBM: BlockingMatrix[F, Boolean],
      toPM: PlanMatrix
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

  // def expand(e: Expand, matBindings: LookupTable[F, PlanMatrix])(
  //     to: => F[PlanMatrix]
  // ): F[PlanMatrix] = e match {
  //   case Expand(from, tpe, _, transpose) =>
  //     val fromGrBMat = resolveBinding(from)(matBindings)

  //     val edgeDirection =
  //       if (transpose) edgeTypesTranspose.getOrCreate(tpe)
  //       else edgeTypes.getOrCreate(tpe)

  //     for {
  //       edgeB <- edgeDirection
  //       toB <- to
  //       fromB <- fromGrBMat
  //       out <- evalAlgebra(fromB, edgeB, toB)
  //     } yield out
  // }

  /**
    * make an expansion matrix into a filter matrix
    * */
  def expandToFilter(
      expand: GrBMatrix[F, Boolean]
  ): F[GrBMatrix[F, Boolean]] = {
    F.bracket(F.pure(expand)) { m =>
      for {
        s <- m.shape
        (rows, cols) = s
        filter <- GrBMatrix.unsafe[F, Boolean](rows, cols)
        _ <- m.reduceColumns(BuiltInBinaryOps.boolean.any).use { v =>
          Diag[F].diag(filter)(v)
        }
      } yield filter

    }(_.release)
  }

  /**
    * go through the plan and resolve matrices
    * MxM against the semiring at the Expansion points
    * Only release intermediate results
    * */
  // def resolvePlan(ps: PlanStep)(
  //     matBindings: LookupTable[F, PlanMatrix]
  // ): F[PlanMatrix] = ps match {
  //   case e: Expand =>
  //     F.suspend {
  //       expand(e, matBindings)(resolvePlan(e.to)(matBindings))
  //     }
  //   case LoadNodes(ref) =>
  //     nodeLabels.getOrCreate(ref.name).map(UnReleasable)

  //   case Union(binds) =>
  //     // first element resolves as usual
  //     val firstBind = binds.head
  //     val first: F[PlanMatrix] = matBindings
  //       .lookupOrBuildPlan(firstBind.key)(
  //         resolvePlan(firstBind.lookup)(matBindings)
  //       )
  //       .flatMap {
  //         // then gets turned into a filter ( a diagonal matrix with only the output nodes on the diagonal )
  //         case Releasable(m) =>
  //           expandToFilter(m).map(Releasable)

  //         case pm => F.pure(pm) // for completeness FIXME: this needs to be made filter also
  //       }

  //     first.flatMap {
  //       binds.tail.toVector.foldLeftM(_) {
  //         case (filter, bind) =>
  //           bind.lookup match {
  //             case e: Expand =>
  //               expand(e, matBindings)(F.delay(filter)).flatMap {
  //                 case Releasable(m) => expandToFilter(m).map(Releasable)
  //                 case pm            => F.pure(pm)
  //               }
  //             case _ =>
  //               F.raiseError(
  //                 new IllegalStateException(
  //                   "Union should have binds to Expand only"
  //                 )
  //               )
  //           }
  //       }
  //     }

  // }

  // for {
  //   table <- F.suspend(LookupTable[F, PlanMatrix])
  //   _ <- ret.toStream
  //     .foldLeftM[F, LookupTable[F, PlanMatrix]](table) { (t, ret) =>
  //       val key = LKey(ret, Set.empty)
  //       for {
  //         binding <- F.delay(bindings(key))
  //         LValue(_, plan) = binding
  //         _ <- t.lookupOrBuildPlan(key)(resolvePlan(plan)(t))
  //       } yield t
  //     }
  // } yield table

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
      size <- Resource.liftF(Ref.of[F, (Long, Long)]((2L * 1024L, 2L * 1024L)))
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
