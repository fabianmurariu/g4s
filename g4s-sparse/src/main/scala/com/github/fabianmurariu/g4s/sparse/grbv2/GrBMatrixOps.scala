package com.github.fabianmurariu.g4s.sparse.grbv2

import cats.Functor
import cats.implicits._
import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.g4s.sparse.grb.GrBError
import com.github.fabianmurariu.g4s.sparse.grb.{SparseMatrixHandler => SMH}
import com.github.fabianmurariu.g4s.sparse.grb.{SparseVectorHandler => SVH}
import scala.reflect.ClassTag
import cats.effect.Sync
import cats.Monad
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import cats.effect.Resource
import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import cats.effect.kernel.MonadCancel

object GrBMatrixOps {

  type Uncancelable[F[_]] = MonadCancel[F, Throwable]

  def force[F[_]: Sync](p: F[MatrixPointer]) =
    p.flatMap { mp =>
      Sync[F].uncancelable(_ =>
        Sync[F].blocking(GrBError.check(GRBCORE.grbWaitMatrix(mp.ref)))
      )
    }

  def get[F[_]: Functor, A: SMH](
      pointer: F[MatrixPointer],
      i: Long,
      j: Long
  ): F[Option[A]] = pointer.map { mp => SMH[A].get(mp.ref)(i, j).headOption }

  def set[F[_]: Functor, A: SMH](
      pointer: F[MatrixPointer],
      i: Long,
      j: Long,
      a: A
  ): F[Unit] = pointer.map { mp => SMH[A].set(mp.ref)(i, j, a) }

  def set[F[_]: Uncancelable, A: SMH: ClassTag](
      pointer: F[MatrixPointer],
      is: Iterable[Long],
      js: Iterable[Long],
      vs: Iterable[A]
  ): F[Unit] =
    MonadCancel[F].uncancelable(_ =>
      pointer
        .map { mp => SMH[A].setAll(mp.ref)(is.toArray, js.toArray, vs.toArray) }
    )

  def set[F[_]: Sync, A: SMH: ClassTag](
      pointer: F[MatrixPointer],
      tuples: Iterable[(Long, Long, A)]
  ): F[Unit] = {
    val ts = tuples
    for {
      is <- Sync[F].delay(ts.iterator.map(_._1).toArray)
      js <- Sync[F].delay(ts.iterator.map(_._2).toArray)
      vs <- Sync[F].delay(ts.iterator.map(_._3).toArray)
      _ <- set(pointer, is, js, vs)
    } yield ()
  }

  def diag[F[_]: Sync](
      mp: F[MatrixPointer],
      vp: F[VectorPointer],
      diag: Long
  ): F[Unit] =
    for {
      m <- mp
      v <- vp
      _ <- Sync[F].uncancelable(_ =>
        Sync[F].delay(GrBError.check(GRBOPSMAT.diag(m.ref, v.ref, diag, null).toLong))
      )
    } yield ()

  def extract[F[_]: Sync, A: SMH: ClassTag](
      pointer: F[MatrixPointer]
  ): F[(Array[Long], Array[Long], Array[A])] = pointer.flatMap { mp =>
    force(pointer).map(_ => SMH[A].extractTuples(mp.ref))
  }

  def duplicate[F[_]: Sync](
      pointer: F[MatrixPointer]
  )(implicit G: GRB): Resource[F, MatrixPointer] =
    for {
      p <- Resource.eval(pointer)
      pDup <- Resource.fromAutoCloseable(
        Sync[F].delay(new MatrixPointer(GRBCORE.dupMatrix(p.ref)))
      )
    } yield pDup

  def show[F[_]: Sync, A: SMH: ClassTag](
      pointer: F[MatrixPointer],
      limit: Int = 10
  ): F[String] =
    for {
      rows <- nrows(pointer)
      cols <- ncols(pointer)
      vals <- extract(pointer)
      n <- nvals(pointer)
    } yield vals match {
      case (is, js, vs) =>
        val extraItems = if (limit >= n) "" else " .. "
        is.lazyZip(js)
          .lazyZip(vs)
          .take(limit)
          .map { case (i, j, v) => s"($i,$j):$v" }
          .mkString(
            s"[nvals=$n ${rows}x${cols}:${implicitly[ClassTag[A]]} {",
            ", ",
            s"$extraItems}]"
          )
    }

  def remove[F[_]: Functor](
      pointer: F[MatrixPointer],
      i: Long,
      j: Long
  ): F[Unit] =
    pointer.map(mp => GrBError.check(GRBCORE.removeElementMatrix(mp.ref, i, j)))

  def ncols[F[_]: Functor](pointer: F[MatrixPointer]): F[Long] =
    pointer.map { mp => GRBCORE.ncols(mp.ref) }

  def nrows[F[_]: Functor](pointer: F[MatrixPointer]): F[Long] =
    pointer.map { mp => GRBCORE.nrows(mp.ref) }

  def shape[F[_]: Monad](pointer: F[MatrixPointer]): F[(Long, Long)] =
    for {
      n <- ncols(pointer)
      r <- nrows(pointer)
    } yield (r, n)

  def nvals[F[_]: Functor](pointer: F[MatrixPointer]): F[Long] =
    pointer.map { mp => GRBCORE.nvalsMatrix(mp.ref) }

  def resize[F[_]: Functor](
      pointer: F[MatrixPointer],
      rows: Long,
      cols: Long
  ): F[Unit] = pointer.map { mp =>
    GrBError.check(GRBCORE.resizeMatrix(mp.ref, rows, cols))
  }

  def clear[F[_]: Functor](pointer: F[MatrixPointer]): F[Unit] =
    pointer.map(mp => GrBError.check(GRBCORE.clearMatrix(mp.ref)))

  def release[F[_]: Functor](pointer: F[MatrixPointer]): F[Unit] =
    pointer.map { mp =>
      GrBError.check(GRBCORE.freeMatrix(mp.ref))
    }

  def reduce0[F[_]: Sync, A: SVH](
      pointer: F[MatrixPointer],
      op: GrBBinaryOp[A, A, A],
      desc: Option[GrBDescriptor]
  )(implicit G: GRB): Resource[F, GrBVector[F, A]] = {
    (for {
      r <- Resource.eval(nrows(pointer))
      v <- GrBVector[F, A](r)
    } yield v).evalMap { vec =>
      for {
        v <- vec.pointer
        m <- pointer
        _ <- Sync[F].delay {
          GRBOPSMAT.matrixReduceBinOp(
            v.ref,
            null,
            null,
            op.pointer,
            m.ref,
            desc.map(_.pointer).orNull
          )
        }
      } yield vec
    }
  }
}
