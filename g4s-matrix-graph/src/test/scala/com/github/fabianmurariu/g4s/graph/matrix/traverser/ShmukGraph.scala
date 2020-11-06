package com.github.fabianmurariu.g4s.graph.matrix.traverser

import cats.implicits._
import cats.effect.{Resource, Sync}
import com.github.fabianmurariu.g4s.sparse.grbv2.Matrix

import scala.collection.mutable
import scala.reflect.runtime.universe.{Traverser => _, _}

/**
 * Bare minimum graph powered by adjacency matrices
 * used for testing, kinda shmuky
 */
class ShmukGraph[F[_], V, E](
                              edges: Matrix[F, Boolean],
                              edgesT: Matrix[F, Boolean],
                              private[traverser] val labels: mutable.Map[String, Matrix[F, Boolean]],
                              private[traverser] val types: mutable.Map[String, Matrix[F, Boolean]],
                              dbNodes: mutable.Map[Long, V],
                              id: cats.effect.concurrent.Ref[F, Long]
                            )(implicit F: Sync[F]) {

  def newMatrix: F[Matrix[F, Boolean]] = {
    Matrix[F, Boolean](16, 16).allocated.map(_._1)
  }

  def insertV[T <: V](v: T)(implicit tt: TypeTag[T]): F[Long] =
    for {
      vId <- id.getAndUpdate(i => i + 1)
      tpe = tt.tpe.toString()
      nodeMat <- {
        labels.get(tpe) match {
          case None    => newMatrix
          case Some(m) => F.pure(m)
        }
      }
      _ <- nodeMat.set(vId, vId, true)
      _ <- F.delay {
        dbNodes.put(vId, v)
        labels.put(tpe, nodeMat)
      }
    } yield vId

  def edge[T <: E](src: Long, dst: Long)(
    implicit tt: TypeTag[T]
  ): F[Unit] =
    for {
      _ <- edges.set(src, dst, true)
      _ <- edgesT.set(dst, src, true)
      tpe = tt.tpe.toString()
      edgeMat <- {
        types.get(tpe) match {
          case None    => newMatrix
          case Some(m) => F.pure(m)
        }
      }
      _ <- edgeMat.set(src, dst, true)
      _ <- F.delay {
        types.put(tpe, edgeMat)
      }
    } yield ()

  def run(op: GraphMatrixOp): F[Matrix[F, Boolean]] = {

    ???
  }
}

object ShmukGraph {
  def apply[F[_], V, E](implicit F: Sync[F]): Resource[F, ShmukGraph[F, V, E]] =
    for {
      es <- Matrix[F, Boolean](16, 16)
      esT <- Matrix[F, Boolean](16, 16)
      id <- Resource.liftF(cats.effect.concurrent.Ref.of[F, Long](0L))
      g <- Resource.make(
        F.pure(
          new ShmukGraph[F, V, E](
            es,
            esT,
            mutable.Map.empty,
            mutable.Map.empty,
            mutable.Map.empty,
            id
          )
        )
      ) { g =>
        for {
          _ <- g.labels.values.toStream.foldM[F, Unit](())((_, mat) =>
            mat.pointer.map(_.close())
          )
          _ <- g.types.values.toStream.foldM[F, Unit](())((_, mat) =>
            mat.pointer.map(_.close())
          )
        } yield ()
      }
    } yield g
}
