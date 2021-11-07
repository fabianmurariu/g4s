package com.github.fabianmurariu.g4s.matrix

import cats.effect.kernel.Deferred
import cats.effect.std.Queue
import cats.effect.IO
import cats.effect.kernel.Ref
import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import cats.effect.kernel.Resource
import scala.reflect.ClassTag
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import com.github.fabianmurariu.g4s.sparse.grb.SparseMatrixHandler

sealed trait MatMsg[I]
sealed trait MatMsgResp[I, O] extends MatMsg[I] {
  def reply: Deferred[IO, O]
}

case class Set[T](i: Array[Long], j: Array[Long], v: Array[T]) extends MatMsg[T]
/**
  * TODO: Actor aproach to blocking matrix
  * 
  *
  * @param mat
  * @param mailbox
  */
class BlockingMatrixV2[T](
    val mat: GrBMatrix[IO, T],
    val mailbox: Queue[IO, MatMsg[T]],
) extends Actor[T] {

  override def apply(v1: MatMsg[T]): IO[Unit] = v1 match {
    case Set(is: Array[Long], js: Array[Long], vs: Array[T]) =>
         mat.set(is, js, vs)
  }

  def set(is: Array[Long], js: Array[Long], vs: Array[T]):IO[Unit] = 
    mailbox.offer(Set(is, js, vs))
}

object BlockingMatrixV2 {
  def apply[T: ClassTag: Reduce: SparseMatrixHandler](rows: Long, cols: Long)(implicit G: GRB) =
    for {
      mat <- GrBMatrix[IO, T](rows, cols)
      out <- Resource {
        for {
            mailbox <- Queue.bounded[IO, MatMsg[T]](5)
            shutdown <- Ref[IO].of(false)
            bMat <- IO.delay(new BlockingMatrixV2[T](mat, mailbox))
            fib <- bMat.receiveLoop.start
        } yield (bMat, fib.cancel)
      }
    } yield out
}

trait Actor[T] extends (MatMsg[T] => IO[Unit]) { self =>
  def mailbox: Queue[IO, MatMsg[T]]

  def receiveLoop: IO[Unit] =
    mailbox.take.flatMap(self.apply).flatMap(_ => receiveLoop)

}
