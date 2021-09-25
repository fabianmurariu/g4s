package com.github.fabianmurariu.g4s.graph

import java.util.concurrent.ConcurrentHashMap
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix

import scala.jdk.CollectionConverters.IteratorHasAsScala
import cats.effect.Resource
import com.github.fabianmurariu.g4s.matrix.BlockingMatrix
import cats.implicits._
import cats.effect.kernel.Ref
import cats.effect.IO
import cats.effect.std.Semaphore

class LabelledMatrices(
    private[graph] val mats: ConcurrentHashMap[
      String,
      BlockingMatrix[Boolean]
    ],
    shape: Ref[IO, (Long, Long)]
)(implicit G: GRB) {

  private def defaultProvider
      : IO[java.util.function.Function[String, BlockingMatrix[Boolean]]] =
    for {
      lock <- Semaphore[IO](1)
      s <- shape.get
      (rows, cols) = s
      f <- GrBMatrix.unsafeFn[IO, Boolean](rows, cols)
    } yield {s:String => new BlockingMatrix(lock, f(s))}

  def getOrCreate(label: String): IO[BlockingMatrix[Boolean]] =
    for {
      matMaker <- defaultProvider
      mat <- IO.delay(mats.computeIfAbsent(label, matMaker))
    } yield mat

}

object LabelledMatrices {
  def apply(shape: Ref[IO, (Long, Long)])(
      implicit G: GRB,
  ): Resource[IO, LabelledMatrices] =
    Resource.make(
      IO.delay(new LabelledMatrices(new ConcurrentHashMap(), shape))
    ) {
      _.mats
        .values()
        .iterator()
        .asScala
        .to(Vector)
        .foldLeftM(())((_, mat) => mat.release)
    }
}
