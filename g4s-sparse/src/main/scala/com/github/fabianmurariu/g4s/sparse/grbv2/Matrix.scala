package com.github.fabianmurariu.g4s.sparse.grbv2

import cats.effect.Resource
import java.nio.Buffer
import cats.implicits._
import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.g4s.sparse.grb.{SparseMatrixHandler, grb}
import cats.effect.Sync
import scala.reflect.ClassTag
import scala.{specialized => sp}
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
import cats.effect.concurrent.MVar2
import cats.effect.Async
import cats.effect.concurrent.MVar
import com.github.fabianmurariu.g4s.sparse.grb.GrBError

sealed trait Matrix[F[_], @sp(Boolean, Byte, Short, Int, Long, Float, Double) A]
    extends BaseMatrix[F] { self =>
  private[grbv2] def H: SparseMatrixHandler[A]

  /**
    * Forces all the pending operations on this matrix to complete
    */
  def force: F[Matrix[F, A]] = pointer.map { mp =>
    GRBCORE.matrixWait(mp.ref)
    self
  }

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
  )(implicit SVH: SparseVectorHandler[A]): Resource[F, SprVector[F, A]] =
    (for {
      r <- Resource.liftF(self.nrows)
      v <- SprVector[F, A](r)
    } yield v).evalMap[F, SprVector[F, A]] { vec =>
      for {
        v <- vec.pointer
        m <- self.pointer
        _ <- self.F.delay {
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
  ): Resource[F, Matrix[F, A]] = {
    implicit val H: SparseMatrixHandler[A] = self.H
    for {
      shape <- Resource.liftF(self.shape)
      c <- Matrix[F, A](shape._2, shape._1)
      _ <- Resource.liftF(
        MatrixOps.transpose[F, A, A, A, X](c)(self)(mask, accum, desc)
      )
    } yield c
  }

  def apply[R1: GrBRangeLike, R2: GrBRangeLike](
      isRange: R1,
      jsRange: R2
  ): MatrixSelection[F, A] = {
    val (ni, is) = GrBRangeLike[R1].toGrB(isRange)
    val (nj, js) = GrBRangeLike[R2].toGrB(jsRange)
    new MatrixSelection(self, is, ni, js, nj)
  }

  def set[X](
      from: MatrixSelection[F, A],
      mask: Option[Matrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  ): F[Matrix[F, A]] = {

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
  )(op: GrBBinaryOp[A, A, Boolean]): F[Boolean] = {

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

  def isEq(other: Matrix[F, A])(implicit EQ: EqOp[A]): F[Boolean] =
    isAll(other)(EQ)

  def duplicateF: Resource[F, Matrix[F, A]] =
    for {
      p <- Resource.liftF(pointer)
      pDup <- Resource.fromAutoCloseable(
        self.F.delay(new MatrixPointer(GRBCORE.dupMatrix(p.ref)))
      )
    } yield new DefaultMatrix[F, A](F.pure(pDup), self.F, self.H)

  def show(limit: Int = 10)(implicit CT: ClassTag[A]): F[String] =
    for {
      s <- shape
      vals <- extract
      n <- nvals
    } yield vals match {
      case (is, js, vs) =>
        (Stream(is: _*), Stream(js: _*), Stream(vs: _*)).zipped
          .take(limit)
          .map { case (i, j, v) => s"($i,$j):$v" }
          .mkString(s"[$n ${s._1}x${s._2}:$CT {", ",", " .. }]")
    }

}

trait BaseMatrix[F[_]] {
  def pointer: F[Pointer]

  implicit def F: Sync[F]

  def remove(i: Long, j: Long): F[Unit] = pointer.map { mp =>
    GrBError.check(GRBCORE.removeElementMatrix(mp.ref, i, j))
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
    GrBError.check(GRBCORE.resizeMatrix(mp.ref, rows, cols))
  }

  def clear: F[Unit] = pointer.map { mp =>
    GrBError.check(GRBCORE.clearMatrix(mp.ref))
  }

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
      .fromAutoCloseable(M.delay {
        grb.GRB
        new MatrixPointer(SMH.createMatrix(rows, cols))
      })
      .map { mp => new DefaultMatrix(M.pure(mp), M, SMH) }
  }

  def sync[F[_], A](rows: Long, cols: Long)(
      implicit M: Async[F],
      SMH: SparseMatrixHandler[A]
  ): Resource[F, Matrix[F, A]] =
    for {
      mp <- Resource
        .fromAutoCloseable(M.delay {
          grb.GRB
          new MatrixPointer(SMH.createMatrix(rows, cols))
        })
      lock <- Resource.liftF(MVar.uncancelableOf(mp.asInstanceOf[Pointer]))
    } yield new SyncMatrix[F, A](lock, M, SMH)

  def fromTuples[F[_], A](
      rows: Long,
      cols: Long
  )(is: Array[Long], js: Array[Long], vs: Array[A])(
      implicit M: Sync[F],
      SMH: SparseMatrixHandler[A]
  ): Resource[F, Matrix[F, A]] = {
    Resource
      .fromAutoCloseable(M.delay {
        grb.GRB
        new MatrixPointer(SMH.createMatrixFromTuples(rows, cols)(is, js, vs))
      })
      .map { mp => new DefaultMatrix(M.pure(mp), M, SMH) }
  }
}

private[grbv2] class DefaultMatrix[F[_], A](
    val pointer: F[Pointer],
    val F: Sync[F],
    val H: SparseMatrixHandler[A]
) extends Matrix[F, A]

/*
 * reimplementation of Matrix with a synchronized API on all the methods
 * force (GrB_wait(matrix)) on each call
 */
private[grbv2] class SyncMatrix[F[_], A](
    val lock: MVar2[F, Pointer],
    val F: Async[F],
    val H: SparseMatrixHandler[A]
) extends Matrix[F, A] {
  def acquireF[B](f: Pointer => F[B]): F[B] =
    F.bracket(lock.take)(f)(lock.put)

  def acquire[B](f: Pointer => B): F[B] =
    acquireF(p => F.delay(f(p)))

// FIXME: this requires a blocker, every time a matrix is acquired
// we force all the operations to complete and that takes time
  def acquireMatrix[B](f: Matrix[F, A] => F[B]): F[B] =
    acquireF { pointer =>
      val matView = new DefaultMatrix[F, A](F.pure(pointer), F, H)
      F.flatMap(matView.force)(f)
    }

  override def pointer: F[Pointer] = lock.read

  override def get(i: Long, j: Long): F[Option[A]] =
    acquireMatrix { _.get(i, j) }

  override def set(i: Long, j: Long, a: A): F[Unit] =
    acquireMatrix { _.set(i, j, a) }

  override def set(is: Iterable[Long], js: Iterable[Long], vs: Iterable[A])(
      implicit CT: ClassTag[A]
  ): F[Matrix[F, A]] =
    acquireMatrix { _.set(is, js, vs) }

  override def set(
      tuples: Iterable[(Long, Long, A)]
  )(implicit CT: ClassTag[A]): F[Matrix[F, A]] =
    acquireMatrix { _.set(tuples) }

  override def extract: F[(Array[Long], Array[Long], Array[A])] =
    acquireMatrix { _.extract }
}
