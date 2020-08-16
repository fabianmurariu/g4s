package com.github.fabianmurariu.g4s.sparse.grbv2

import cats.effect.Resource
import java.nio.Buffer
import cats.Monad
import cats.implicits._
import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.g4s.sparse.grb.{
  MatrixBuilder,
  SparseMatrixHandler
}
import cats.effect.Sync
import cats.MonadError
import cats.Applicative
import cats.Foldable

trait Matrix[F[_], A] extends BaseMatrix[F] { self =>
  private[grbv2] def H: SparseMatrixHandler[A]

  def get(i: Long, j: Long): F[Option[A]] = pointer.map { mp =>
    H.get(mp.ref)(i, j).headOption
  }

  def set(i: Long, j: Long, a: A): F[Unit] = pointer.map { mp =>
    H.set(mp.ref)(i, j, a)
  }

  def set(is:Iterable[Long], js:Iterable[Long], vs:Iterable[A]):F[Matrix[F, A]] = {
    (is, js, vs).zipped.toStream.foldM[F, Matrix[F, A]](self){
      case (m, (i, j, v)) => m.set(i, j, v).map(_ => self)
    }
  }

  def extract: F[(Array[Long], Array[Long], Array[A])] = pointer.map { mp =>
    H.extractTuples(mp.ref)
  }

  def duplicateF(implicit FF:Sync[F]): Resource[F, Matrix[F, A]] =
    for {
      p <- Resource.liftF(pointer)
      pDup <- Pointer.apply(GRBCORE.dupMatrix(p.ref))(FF)
    } yield new Matrix[F, A]{

      override def F: Monad[F] = FF

      override def H: SparseMatrixHandler[A] = self.H

      val pointer = F.pure(pDup)
    }
}

trait BaseMatrix[F[_]] extends Container[F] { self =>

  def remove(i: Long, j: Long): F[Unit] = pointer.map { mp =>
    GRBCORE.removeElementMatrix(mp.ref, i, j)
  }

  def ncols: F[Long] = pointer.map { mp => GRBCORE.ncols(mp.ref) }

  def nrows: F[Long] = pointer.map { mp => GRBCORE.nrows(mp.ref) }

  def nvals: F[Long] = pointer.map { mp => GRBCORE.nvalsMatrix(mp.ref) }

  def resize(rows: Long, cols: Long): F[Unit] = pointer.map { mp =>
    GRBCORE.resizeMatrix(mp.ref, rows, cols)
  }

  def clear: F[Unit] = pointer.map { mp => GRBCORE.clearMatrix(mp.ref) }

}

trait Container[F[_]] { self =>

  private[grbv2] def pointer: F[Pointer]

  implicit def F: Monad[F]

}

private[grbv2] sealed trait Pointer {
  def ref: Buffer
  def free: Unit
}

class MatrixPointer(val ref: Buffer) extends Pointer {
  def free =
    GRBCORE.freeMatrix(ref)
}

class VectorPointer(val ref: Buffer) extends Pointer {
  def free =
    GRBCORE.freeVector(ref)
}

object Pointer {

  private[grbv2] def matrixPointer[F[_]](
      b: => Buffer
  )(implicit M: Applicative[F]): Resource[F, Either[Throwable, MatrixPointer]] = {
    Resource.make {
      M.pure {
        try {
          Right(
            new MatrixPointer(b)
          )
        } catch {
          case t: Throwable =>
            Left(t)
        }
      }
    } {
      case Right(matPointer) => M.pure(matPointer.free)
      case _                 => M.unit
    }
  }

  private[grbv2] def apply[F[_]](
      b: => Buffer
  )(implicit M: Sync[F]): Resource[F, MatrixPointer] = {
    Resource.make (M.delay(new MatrixPointer(b))) (p => M.delay(p.free))
  }

  private[grbv2] def vectorPointer[F[_]](
      b: Buffer
  )(implicit M: Monad[F]): Resource[F, VectorPointer] = {
    Resource.make(M.pure(new VectorPointer(b)))(p => M.pure(p.free))
  }
}

object Matrix {
  def matrixOrError[F[_], A](rows: Long, cols: Long)(
      implicit M: Monad[F],
      MB: MatrixBuilder[A],
      SMH: SparseMatrixHandler[A]
  ): Resource[F, Either[Throwable, Matrix[F, A]]] = {
    Pointer.matrixPointer[F](MB.build(rows, cols)).map {
      _.map { mp =>
        new Matrix[F, A] {
          val pointer = M.pure(mp)
          override def F = M
          override def H = SMH
        }
      }
    }
  }

  def apply[F[_], A](rows: Long, cols: Long)(
      implicit M: Sync[F],
      MB: MatrixBuilder[A],
      SMH: SparseMatrixHandler[A]
  ): Resource[F, Matrix[F, A]] = {
    for {
      p <- Resource.make {
        M.delay(new MatrixPointer(MB.build(rows, cols)))
      } { p => M.delay { p.free } }
    } yield new Matrix[F, A] {
      val pointer = M.pure(p)
      override def F = M
      override def H = SMH
    }
  }
}
