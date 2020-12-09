package com.github.fabianmurariu.g4s.graph

import java.util.concurrent.ConcurrentHashMap

import cats.effect.{Sync, concurrent}
import cats.implicits._
import scala.reflect.ClassTag

trait DataStore[F[_], V, E] {

  def persistV(v: V): F[Long]

  def persistE(src: Long, dst: Long, e: E): F[Unit]

  def getV(id: Long): F[Option[V]]

  def getVs(ids: Array[Long]): F[Array[V]]
}

object DataStore {

  private case class NodeBlock[V, E](
      v: V,
      out: ConcurrentHashMap[Long, E],
      in: ConcurrentHashMap[Long, E]
  )

  private class DefaultDataStoreImpl[F[_], V, E](
      ids: concurrent.Ref[F, Long],
      map: ConcurrentHashMap[Long, NodeBlock[V, E]]
  )(implicit F: Sync[F], CT: ClassTag[V])
      extends DataStore[F, V, E] {

    def nodeDefault(v: V): F[
      java.util.function.BiFunction[Long, NodeBlock[V, E], NodeBlock[V, E]]
    ] = F.pure(
      {
        case (_, null) =>
          NodeBlock(v, new ConcurrentHashMap(), new ConcurrentHashMap())
        case (_, nb: NodeBlock[V, E] @unchecked) => nb.copy(v = v)
      }
    )

    override def persistV(v: V): F[Long] =
      for {
        vId <- ids.getAndUpdate(id => id + 1L)
        blockBuilder <- nodeDefault(v)
        _ <- F.delay(map.compute(vId, blockBuilder))
      } yield vId

    def edgeOut(dst: Long, e: E): F[
      java.util.function.BiFunction[Long, NodeBlock[V, E], NodeBlock[V, E]]
    ] = F.pure(
      {
        case (_, nb @ NodeBlock(_, outMap, _)) =>
          outMap.put(dst, e)
          nb
      }
    )

    def edgeIn(src: Long, e: E): F[
      java.util.function.BiFunction[Long, NodeBlock[V, E], NodeBlock[V, E]]
    ] = F.pure(
      {
        case (_, nb @ NodeBlock(_, _, inMap)) =>
          inMap.put(src, e)
          nb
      }
    )

    override def persistE(src: Long, dst: Long, e: E): F[Unit] =
      for {
        srcUpdate <- edgeOut(dst, e)
        dstUpdate <- edgeIn(src, e)
        _ <- F.delay {
          map.compute(src, srcUpdate)
          map.compute(dst, dstUpdate)
        }
      } yield ()

    override def getV(id: Long): F[Option[V]] =
      F.delay(Option(map.get(id)).map(_.v))

    def getVs(ids: Array[Long]): F[Array[V]] =
      F.delay {
        ids.iterator
          .map(map.get(_))
          .filter(_ != null)
          .map(_.v)
          .toArray
      }
  }

  def default[F[_], V, E](
      implicit S: Sync[F],
      CT: ClassTag[V]
  ): F[DataStore[F, V, E]] =
    concurrent.Ref
      .of[F, Long](0)
      .map(ref => new DefaultDataStoreImpl(ref, new ConcurrentHashMap()))
}
