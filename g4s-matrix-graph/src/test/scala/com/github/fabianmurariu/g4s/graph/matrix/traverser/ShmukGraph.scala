package com.github.fabianmurariu.g4s.graph.matrix.traverser

import cats.implicits._
import cats.effect.{Resource, Sync}
import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grb.GRB

import cats.effect.concurrent.MVar2
import java.util.concurrent.ConcurrentHashMap
import cats.effect.Concurrent
import cats.effect.concurrent.Semaphore

trait DataStore[F[_], V, E] {

  def persist(id: Long, v: V): F[Unit]

  def reserveId: F[Long]
}

case class BlockingMatrix[F[_], A](lock: Semaphore[F], mat: GrBMatrix[F, A])(
    implicit F: Sync[F]
) {
  def use[B](f: GrBMatrix[F, A] => F[B]): F[B] =
    F.bracket(lock.acquire)(_ => f(mat))(_ => lock.release)
}

class LabelledMatrices[F[_]](
    val mats: ConcurrentHashMap[String, BlockingMatrix[F, Boolean]],
    val rootLock: MVar2[F, Boolean]
)(implicit F: Concurrent[F], G: GRB) {

  def defaultProvider: F[String => BlockingMatrix[F, Boolean]] =
    for {
      lock <- Semaphore[F](1)
    } yield _ => BlockingMatrix(lock, GrBMatrix.unsafe[F, Boolean](16, 16))

  def getOrCreate(label: String): F[BlockingMatrix[F, Boolean]] =
    for {
      matMaker <- defaultProvider
      mat <- F.delay(
        mats.computeIfAbsent(
          label,
          new java.util.function.Function[String, BlockingMatrix[F, Boolean]] {
            def apply(key: String): BlockingMatrix[F, Boolean] = matMaker(key)
          }
        )
      )
    } yield mat

}

/**
 * Locked graph
 * does not do transactions
 * it does do concurrent access
 */
class ShmukGraph[F[_], V, E](
    edges: BlockingMatrix[F, Boolean],
    edgesT: BlockingMatrix[F, Boolean],
    edgeTypes: LabelledMatrices[F],
    nodeLabels: LabelledMatrices[F],
    ds: DataStore[F, V, E]
)(implicit F: Sync[F], G: GRB) {}
/*// *
//  * Bare minimum graph powered by adjacency matrices
//  * used for testing, kinda shmuky
//  */
// class ShmukGraph[F[_], V, E](edges: BlockingMatrix[F, Boolean],
//                              edgesT: BlockingMatrix[F, Boolean],

//                               private[traverser] val labels: mutable.Map[String, BlockingMatrix[F, Boolean]],
//                               private[traverser] val types: mutable.Map[String, BlockingMatrix[F, Boolean]],
//                               ds:DataStore[F, V, E]
//                             )(implicit F: Sync[F], G:GRB) {

//   def insertV(v: V)(implicit tt: TypeTag[V]): F[Long] =
//     for {
//       vId <- ds.reserveId
//       tpe = tt.tpe.toString()
//       nodeMat <- {
//         labels.get(tpe) match {
//           case None    => F.pure(GrBMatrix.unsafe[F, Boolean](16, 16))
//           case Some(m) => F.pure(m)
//         }
//       }
//       _ <- nodeMat.set(vId, vId, true)
//       _ <- F.delay {
//         dbNodes.put(vId, v)
//         labels.put(tpe, nodeMat)
//       }
//     } yield vId

//   def edge[T <: E](src: Long, dst: Long)(
//     implicit tt: TypeTag[T]
//   ): F[Unit] =
//     for {
//       _ <- edges.set(src, dst, true)
//       _ <- edgesT.set(dst, src, true)
//       tpe = tt.tpe.toString()
//       edgeMat <- {
//         types.get(tpe) match {
//           case None    => F.pure(GrBMatrix.unsafe[F, Boolean](16, 16))
//           case Some(m) => F.pure(m)
//         }
//       }
//       _ <- edgeMat.set(src, dst, true)
//       _ <- F.delay {
//         types.put(tpe, edgeMat)
//       }
//     } yield ()

//   def run(op: GraphMatrixOp): F[GrBMatrix[F, Boolean]] = {

//     ???
//   }
// }

// object ShmukGraph {
//   def apply[F[_], V, E](implicit F: Sync[F], G:GRB): Resource[F, ShmukGraph[F, V, E]] =
//     for {
//       es <- GrBMatrix[F, Boolean](16, 16)
//       esT <- GrBMatrix[F, Boolean](16, 16)
//       id <- Resource.liftF(cats.effect.concurrent.Ref.of[F, Long](0L))
//       g <- Resource.make(
//         F.pure(
//           new ShmukGraph[F, V, E](
//             es,
//             esT,
//             mutable.Map.empty,
//             mutable.Map.empty,
//             mutable.Map.empty,
//             id
//           )
//         )
//       ) { g =>
//         for {
//           _ <- g.labels.values.toStream.foldM[F, Unit](())((_, mat) =>
//             mat.pointer.map(_.close())
//           )
//           _ <- g.types.values.toStream.foldM[F, Unit](())((_, mat) =>
//             mat.pointer.map(_.close())
//           )
//         } yield ()
//       }
//     } yield g
// }
