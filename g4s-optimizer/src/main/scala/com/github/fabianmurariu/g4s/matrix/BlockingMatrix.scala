package com.github.fabianmurariu.g4s.matrix

import cats.effect.Resource
import com.github.fabianmurariu.g4s.sparse.grb.{GRB, SparseMatrixHandler}
import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import fs2.Stream
import scala.reflect.ClassTag
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import cats.effect.std.Semaphore
import cats.effect.IO
import cats.effect.kernel.MonadCancel
import cats.effect.kernel.Ref

class BlockingMatrix[A: ClassTag: Reduce](
    lock: Semaphore[IO],
    mat: GrBMatrix[IO, A],
    var shutdown: Boolean = false
) {
  self =>
  def use[B](f: GrBMatrix[IO, A] => IO[B]): IO[B] =
    MonadCancel[IO].bracket(lock.acquire) { _ =>
      if (shutdown) {
        IO.raiseError[B](new IllegalStateException("Attempting to modify"))
      } else {
        f(mat)
      }
    }(_ => lock.release)

  def release: IO[Unit] =
    MonadCancel[IO].bracket(lock.acquire)(_ => IO.delay(self.shutdown = true))(
      _ => lock.release
    )

  /**
    * Creates a [[fs2.Stream]] of this Blocking matrix
    * each block might not be of the same size
    * @return
    */
  def toStream(off: Long = 0, size: Long = 1024)(
      implicit SMH: SparseMatrixHandler[A],
      g: GRB
  ): Stream[IO, (Array[Long], Array[Long], Array[A])] = {

    def step(
        offset: Long,
        pageSize: Long
    ): Stream[IO, (Long, Long, Array[Long], Array[Long], Array[A])] =
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
              MonadCancel[IO].bracket(GrBMatrix.unsafe[IO, A](rows, cols)) {
                mat =>
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
          case ck @ (0, _, _, _, _) => Stream.eval(IO.pure(ck))
          case ck @ (_, newOffset, _, _, _) =>
            Stream.eval(IO.pure(ck)) ++ step(newOffset, size)
        }

    step(off, size).map {
      case (_, _, is, js, vs) => (is, js, vs)
    }
  }
}

object BlockingMatrix {
  def apply[A: SparseMatrixHandler: ClassTag: Reduce](
      shape: Ref[IO, (Long, Long)]
  )(
      implicit G: GRB
  ): Resource[IO, BlockingMatrix[A]] =
    for {
      matSize <- Resource.eval(shape.get)
      (rows, cols) = matSize
      m <- GrBMatrix[IO, A](rows, cols)
      bm <- Resource.eval(fromGrBMatrix(m))
    } yield bm

  def fromGrBMatrix[A: ClassTag: Reduce](
      mat: GrBMatrix[IO, A]
  ): IO[BlockingMatrix[A]] =
    Semaphore[IO](1).map(lock => new BlockingMatrix(lock, mat))
}
