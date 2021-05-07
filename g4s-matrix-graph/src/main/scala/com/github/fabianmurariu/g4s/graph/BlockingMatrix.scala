package com.github.fabianmurariu.g4s.graph

import cats.implicits._
import cats.effect.{Concurrent, Resource}
import cats.effect.concurrent.Semaphore
import com.github.fabianmurariu.g4s.sparse.grb.{GRB, SparseMatrixHandler}
import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import fs2.Stream
import cats.effect.concurrent.Ref
import scala.reflect.ClassTag
import com.github.fabianmurariu.g4s.sparse.grb.Reduce

class BlockingMatrix[F[_], A: ClassTag: Reduce](
    lock: Semaphore[F],
    mat: GrBMatrix[F, A],
    var shutdown: Boolean = false
)(
    implicit F: Concurrent[F]
) {
  self =>
  def use[B](f: GrBMatrix[F, A] => F[B]): F[B] =
    F.bracket(lock.acquire) { _ =>
      if (shutdown) {
        F.raiseError[B](new IllegalStateException("Attempting to modify"))
      } else {
        f(mat)
      }
    }(_ => lock.release)

  def release: F[Unit] =
    F.bracket(lock.acquire)(_ => F.delay(self.shutdown = true))(_ =>
      lock.release
    )

  /**
    * Creates a [[fs2.Stream]] of this Blocking matrix
    * each block might not be of the same size
    * @return
    */
  def toStream(off: Long = 0, size: Long = 1024)(
      implicit SMH: SparseMatrixHandler[A],
      g: GRB
  ): Stream[F, (Array[Long], Array[Long], Array[A])] = {

    def step(
        offset: Long,
        pageSize: Long
    ): Stream[F, (Long, Long, Array[Long], Array[Long], Array[A])] =
      Stream
        .eval(use { mat =>
          val from = for {
            shape <- mat.shape
            (rows, cols) = shape
            newOffset = Math.min(offset + pageSize, rows)
          } yield {
            val rowRange = offset until newOffset
            val size = (newOffset - offset)
            (
              rows - newOffset,
              newOffset,
              size,
              cols,
              mat(rowRange, 0L until cols)
            )
          }

          from.flatMap {
            case (remaining, newOffset, rows, cols, selection) =>
              // selection
              //   .show()
              //   .map(sel => println(s"Step: remaining: $remaining, newOffset: $newOffset, selection: [$sel] into: (${rows}x${cols})")) *>
              F.bracket(GrBMatrix.unsafe[F, A](rows, cols)) { mat =>
                mat
                  .update(selection)
                  .flatMap(_.extract)
                  .map {
                    case (is, js, vs) =>
                      val adjustedOffset = newOffset - rows
                      val adjustedIs = is.map(_ + adjustedOffset)
                      (remaining, newOffset, adjustedIs, js, vs)
                  }
              }(_.release)
          }
        })
        .flatMap {
          case ck @ (0, _, _, _, _) => Stream.eval(F.pure(ck))
          case ck @ (_, newOffset, _, _, _) =>
            Stream.eval(F.pure(ck)) ++ step(newOffset, size)
        }

    step(off, size).map {
      case (_, _, is, js, vs) => (is, js, vs)
    }
  }
}

object BlockingMatrix {
  def apply[F[_]: Concurrent, A: SparseMatrixHandler: ClassTag: Reduce](
      shape: Ref[F, (Long, Long)]
  )(
      implicit G: GRB
  ): Resource[F, BlockingMatrix[F, A]] =
    for {
      matSize <- Resource.liftF(shape.get)
      (rows, cols) = matSize
      m <- GrBMatrix[F, A](rows, cols)
      bm <- Resource.liftF(fromGrBMatrix(m))
    } yield bm

  def fromGrBMatrix[F[_]: Concurrent, A: ClassTag: Reduce](
      mat: GrBMatrix[F, A]
  ): F[BlockingMatrix[F, A]] =
    Semaphore[F](1).map(lock => new BlockingMatrix(lock, mat))
}
