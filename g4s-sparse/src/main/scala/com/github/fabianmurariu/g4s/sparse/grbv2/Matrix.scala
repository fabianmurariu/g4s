package com.github.fabianmurariu.g4s.sparse.grbv2

import cats.effect.Resource
import java.nio.Buffer
import cats.Monad
import cats.implicits._
import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.g4s.sparse.grb.{SparseMatrixHandler, grb}
import cats.effect.Sync
import cats.MonadError
import cats.Applicative
import cats.Foldable
import scala.reflect.ClassTag
import scala.{specialized => sp}
import cats.effect.Bracket
import cats.effect.ExitCase
import cats.effect.ExitCase.{Completed, Error}
import com.github.fabianmurariu.unsafe.GRAPHBLAS
import java.{util => ju}
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.ElemWise
import cats.data.EitherT
import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.sparse.grb.EqOp
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import com.github.fabianmurariu.g4s.sparse.grb.SparseVectorHandler
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import com.github.fabianmurariu.g4s.sparse.grb.GrBError

trait Matrix[F[_], @sp(Boolean, Byte, Short, Int, Long, Float, Double) A]
    extends BaseMatrix[F] { self =>
  private[grbv2] def H: SparseMatrixHandler[A]

  def get(i: Long, j: Long): F[Option[A]] = pointer.map { mp =>
    H.get(mp.ref)(i, j).headOption
  }

  def set(i: Long, j: Long, a: A): F[Unit] = pointer.map { mp =>
    H.set(mp.ref)(i, j, a)
  }

  def set(is: Iterable[Long], js: Iterable[Long], vs: Iterable[A])(
      implicit CT: ClassTag[A]
  ): F[Matrix[F, A]] =
    pointer
      .map { mp => H.setAll(mp.ref)(is.toArray, js.toArray, vs.toArray) }
      .map(_ => self)

  def set(
      tuples: Iterable[(Long, Long, A)]
  )(implicit CT: ClassTag[A]): F[Matrix[F, A]] = {
    val s = tuples.toStream
    val is = s.map(_._1)
    val js = s.map(_._2)
    val vs = s.map(_._3)
    set(is, js, vs)
  }

  def extract: F[(Array[Long], Array[Long], Array[A])] = pointer.map { mp =>
    H.extractTuples(mp.ref)
  }

  def reduce(
      op: GrBBinaryOp[A, A, A]
  )(
      implicit SVH: SparseVectorHandler[A],
      S: Sync[F]
  ): Resource[F, SprVector[F, A]] =
    (for {
      r <- Resource.liftF(self.nrows)
      v <- SprVector[F, A](r)
    } yield v).evalMap[F, SprVector[F, A]] { vec =>
      for {
        v <- vec.pointer
        m <- self.pointer
        _ <- S.delay {
          GRBOPSMAT.matrixReduceBinOp(
            v.ref,
            null,
            null,
            op.pointer,
            m.ref,
            null
          )
        }
      } yield vec
    }

  def transpose[X](
      mask: Option[Matrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  )(implicit S: Sync[F]): Resource[F, Matrix[F, A]] = {
    implicit val H: SparseMatrixHandler[A] = self.H
    for {
      shape <- Resource.liftF(self.shape)
      c <- Matrix[F, A](shape._2, shape._1)
      _ <- Resource.liftF(
        MatrixOps.transpose[F, A, A, A, X](c)(self)(mask, accum, desc)
      )
    } yield c
  }


  def apply[R1:GrBRangeLike, R2:GrBRangeLike](isRange:R1, jsRange:R2): MatrixSelection[F, A] = {
    val (ni, is) = GrBRangeLike[R1].toGrB(isRange)
    val (nj, js) = GrBRangeLike[R2].toGrB(jsRange)
    new MatrixSelection(self, is, ni, js, nj)
  }

  def set[X](from: MatrixSelection[F, A],
      mask: Option[Matrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  )(implicit S: Sync[F]): F[Matrix[F, A]] = {

    implicit val H: SparseMatrixHandler[A] = self.H
    MatrixOps.extract(self)(from)(mask, accum, desc)
  }

  def reduce(init: A, monoid: GrBMonoid[A])(implicit R: Reduce[A]): F[A] =
    self.pointer.map { p => R.reduceAll(p.ref)(init, monoid) }

  def reduceN(
      monoid: GrBMonoid[A]
  )(implicit R: Reduce[A], N: Numeric[A]): F[A] =
    reduce(N.zero, monoid)

  def isAll( // TODO: move this into separate MatrixOps
      other: Matrix[F, A]
  )(op: GrBBinaryOp[A, A, Boolean])(implicit S: Sync[F]): F[Boolean] = {

    def eqEither[T](s1: T, s2: T)(msg: String): EitherT[F, String, Boolean] =
      if (s1 == s2) EitherT.fromEither(Right(true))
      else EitherT.fromEither(Left(msg))

    def eqIntersect(
        expectedNVals: Long,
        r: Long,
        c: Long
    ): F[Either[String, Boolean]] = {
      (for {
        m <- Matrix[F, Boolean](r, c)
        land <- GrBMonoid[F, Boolean](BuiltInBinaryOps.boolean.land, false)
      } yield (m, land)).use {
        case (m, land) =>
          val eF: EitherT[F, String, Boolean] = for {
            inter <- EitherT.right(
              ElemWise[F].intersect(m)(Left(op))(self, other)
            )
            nvals <- EitherT.right(inter.nvals)
            _ <- eqEither(expectedNVals, nvals)(
              s"different nvals after intersect $expectedNVals != $nvals"
            )
            check <- EitherT.right(inter.reduce(true, land))
            out <- eqEither(true, check)("different values")
          } yield out
          eF.value
      }
    }

    val eF = for {
      r1 <- EitherT.right(self.nrows)
      r2 <- EitherT.right(other.nrows)
      _ <- eqEither(r1, r2)(s"different rows $r1 != $r2")
      c1 <- EitherT.right(self.ncols)
      c2 <- EitherT.right(other.ncols)
      _ <- eqEither(c1, c2)(s"different cols $c1 != c2")
      v1 <- EitherT.right(self.nvals)
      v2 <- EitherT.right(other.nvals)
      _ <- eqEither(v1, v2)(s"different nvals $v1 != $v2")
      _ <- EitherT(eqIntersect(v1, r1, c1))
    } yield true

    eF.value.map {
      case Right(_) => true
      case Left(_)  => false
    }
  }

  def isEq(other: Matrix[F, A])(implicit EQ: EqOp[A], S: Sync[F]): F[Boolean] =
    isAll(other)(EQ)

  def duplicateF(implicit FF: Sync[F]): Resource[F, Matrix[F, A]] =
    for {
      p <- Resource.liftF(pointer)
      pDup <- Resource.fromAutoCloseable(
        FF.delay(new MatrixPointer(GRBCORE.dupMatrix(p.ref)))
      )
    } yield new Matrix[F, A] {

      override def F: Monad[F] = FF

      override def H: SparseMatrixHandler[A] = self.H

      val pointer = F.pure(pDup)
    }


}

trait BaseMatrix[F[_]] { self =>

  def pointer: F[Pointer]

  implicit def F: Monad[F]

  def remove(i: Long, j: Long): F[Unit] = pointer.map { mp =>
    GRBCORE.removeElementMatrix(mp.ref, i, j)
  }

  def ncols: F[Long] = pointer.map { mp => GRBCORE.ncols(mp.ref) }

  def nrows: F[Long] = pointer.map { mp => GRBCORE.nrows(mp.ref) }

  def shape: F[(Long, Long)] =
    for {
      n <- ncols
      r <- nrows
    } yield (r, n)

  def nvals: F[Long] = pointer.map { mp => GRBCORE.nvalsMatrix(mp.ref) }

  def resize(rows: Long, cols: Long): F[Unit] = pointer.map { mp =>
    GRBCORE.resizeMatrix(mp.ref, rows, cols)
  }

  def clear: F[Unit] = pointer.map { mp => GRBCORE.clearMatrix(mp.ref) }

}

private[grbv2] sealed trait Pointer extends AutoCloseable {
  def ref: Buffer
}

class MatrixPointer(val ref: Buffer) extends Pointer {

  override def close(): Unit =
    GRBCORE.freeMatrix(ref)

}

class VectorPointer(val ref: Buffer) extends Pointer {
  override def close(): Unit =
    GRBCORE.freeVector(ref)
}

object Matrix {

  def csc[F[_], A](rows: Long, cols: Long)(
      implicit M: Sync[F],
      SMH: SparseMatrixHandler[A]
  ) = apply[F, A](rows, cols).evalMap {
    _.pointer.map(p => GRBCORE.makeCSC(p.ref))
  }

  def apply[F[_], A](rows: Long, cols: Long)(
      implicit M: Sync[F],
      SMH: SparseMatrixHandler[A]
  ): Resource[F, Matrix[F, A]] = {
    Resource
      .fromAutoCloseable(
        M.delay {
          grb.GRB
          new MatrixPointer(SMH.createMatrix(rows, cols))
        }
      )
      .map { mp =>
        new Matrix[F, A] {
          val pointer = M.pure(mp)
          override def F = M
          override def H = SMH
        }
      }
  }

  def fromTuples[F[_], A](
      rows: Long,
      cols: Long
  )(is: Array[Long], js: Array[Long], vs: Array[A])(
      implicit M: Sync[F],
      SMH: SparseMatrixHandler[A]
  ): Resource[F, Matrix[F, A]] = {
    Resource
      .fromAutoCloseable(
        M.delay {
          grb.GRB
          new MatrixPointer(SMH.createMatrixFromTuples(rows, cols)(is, js, vs))
        }
      )
      .map { mp =>
        new Matrix[F, A] {
          val pointer = M.pure(mp)
          override def F = M
          override def H = SMH
        }
      }
  }
}
