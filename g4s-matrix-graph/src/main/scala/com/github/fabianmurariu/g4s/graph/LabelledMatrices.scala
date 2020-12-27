package com.github.fabianmurariu.g4s.graph

import cats.implicits._
import java.util.concurrent.ConcurrentHashMap
import cats.effect.concurrent.Ref
import cats.effect.Concurrent
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix

import scala.collection.JavaConverters._
import cats.effect.concurrent.Semaphore
import cats.effect.Resource

class LabelledMatrices[F[_]](
    private[graph] val mats: ConcurrentHashMap[
      String,
      BlockingMatrix[F, Boolean]
    ],
    shape: Ref[F, (Long, Long)]
)(implicit F: Concurrent[F], G: GRB) {

  private def defaultProvider
      : F[java.util.function.Function[String, BlockingMatrix[F, Boolean]]] =
    for {
      lock <- Semaphore[F](1)
      s <- shape.get
      (rows, cols) = s
    } yield _ =>
      new BlockingMatrix(lock, GrBMatrix.unsafe[F, Boolean](rows, cols))

  def getOrCreate(label: String): F[BlockingMatrix[F, Boolean]] =
    for {
      matMaker <- defaultProvider
      mat <- F.delay(mats.computeIfAbsent(label, matMaker))
    } yield mat

}

object LabelledMatrices {
  def apply[F[_]](shape: Ref[F, (Long, Long)])(
      implicit G: GRB,
      F: Concurrent[F]
  ): Resource[F, LabelledMatrices[F]] =
    Resource.make(
      F.pure(new LabelledMatrices[F](new ConcurrentHashMap(), shape))
    ) {
      _.mats
        .values()
        .iterator()
        .asScala
        .toStream
        .foldLeftM(())((_, mat) => mat.release)
    }
}
