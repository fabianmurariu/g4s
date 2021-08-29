package com.github.fabianmurariu.g4s.graph

import java.util.concurrent.ConcurrentHashMap

import scala.reflect.ClassTag
import cats.effect.IO
import cats.effect.kernel.Ref

trait DataStore[V, E] {

  def persistV(v: V): IO[Long]

  def persistE(src: Long, dst: Long, e: E): IO[Unit]

  def getV(id: Long): IO[Option[V]]

  def getVs(ids: Array[Long]): IO[Array[V]]
}

object DataStore {

  private case class NodeBlock[V, E](
      v: V,
      out: ConcurrentHashMap[Long, E],
      in: ConcurrentHashMap[Long, E]
  )

  private class DefaultDataStoreImpl[V, E](
      ids: Ref[IO, Long],
      map: ConcurrentHashMap[Long, NodeBlock[V, E]]
  )(implicit CT: ClassTag[V])
      extends DataStore[V, E] {

    def nodeDefault(v: V): IO[
      java.util.function.BiFunction[Long, NodeBlock[V, E], NodeBlock[V, E]]
    ] = IO.delay(
      {
        case (_, null) =>
          NodeBlock(v, new ConcurrentHashMap(), new ConcurrentHashMap())
        case (_, nb: NodeBlock[V, E] @unchecked) => nb.copy(v = v)
      }
    )

    override def persistV(v: V): IO[Long] =
      for {
        vId <- ids.getAndUpdate(id => id + 1L)
        blockBuilder <- nodeDefault(v)
        _ <- IO.delay(map.compute(vId, blockBuilder))
      } yield vId

    def edgeOut(dst: Long, e: E): IO[
      java.util.function.BiFunction[Long, NodeBlock[V, E], NodeBlock[V, E]]
    ] = IO.delay(
      {
        case (_, nb @ NodeBlock(_, outMap, _)) =>
          outMap.put(dst, e)
          nb
      }
    )

    def edgeIn(src: Long, e: E): IO[
      java.util.function.BiFunction[Long, NodeBlock[V, E], NodeBlock[V, E]]
    ] = IO.delay(
      {
        case (_, nb @ NodeBlock(_, _, inMap)) =>
          inMap.put(src, e)
          nb
      }
    )

    override def persistE(src: Long, dst: Long, e: E): IO[Unit] =
      for {
        srcUpdate <- edgeOut(dst, e)
        dstUpdate <- edgeIn(src, e)
        _ <- IO.delay {
          map.compute(src, srcUpdate)
          map.compute(dst, dstUpdate)
        }
      } yield ()

    override def getV(id: Long): IO[Option[V]] =
      IO.delay(Option(map.get(id)).map(_.v))

    def getVs(ids: Array[Long]): IO[Array[V]] =
      IO.delay {
        ids.iterator
          .map(map.get(_))
          .filter(_ != null)
          .map(_.v)
          .toArray
      }
  }

  def default[V:ClassTag, E]: IO[DataStore[V, E]] =
    Ref[IO]
      .of(0L)
      .map(ref => new DefaultDataStoreImpl(ref, new ConcurrentHashMap()))
}
