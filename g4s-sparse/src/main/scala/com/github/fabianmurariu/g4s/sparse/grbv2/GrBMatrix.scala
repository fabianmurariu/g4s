package com.github.fabianmurariu.g4s.sparse.grbv2

import cats.effect.Resource
import cats.implicits._
import cats.effect.Sync

import scala.reflect.ClassTag
import scala.{specialized => sp}

import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.g4s.sparse.grb.SparseMatrixHandler
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.EqOp
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import com.github.fabianmurariu.g4s.sparse.grb.SparseVectorHandler
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import com.github.fabianmurariu.g4s.sparse.grb.GrBError

sealed trait GrBMatrix[F[_], @sp(Boolean, Byte, Short, Int, Long, Float, Double) A] { self =>

  implicit def H:SparseMatrixHandler[A]

  implicit def G:GRB

  implicit def F:Sync[F]

  def pointer:F[MatrixPointer]

  /**
    * Forces all the pending operations on this matrix to complete
    */
  def force: F[GrBMatrix[F, A]] = pointer.map { mp =>
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
  ): F[GrBMatrix[F, A]] =
    pointer
      .map { mp => H.setAll(mp.ref)(is.toArray, js.toArray, vs.toArray) }
      .map(_ => self)

  def set(
      tuples: Iterable[(Long, Long, A)]
  )(implicit CT: ClassTag[A]): F[GrBMatrix[F, A]] = {
    val s = tuples
    val is = s.iterator.map(_._1).toArray
    val js = s.iterator.map(_._2).toArray
    val vs = s.iterator.map(_._3).toArray
    set(is, js, vs)
  }

  def extract: F[(Array[Long], Array[Long], Array[A])] = pointer.flatMap { mp =>
    grbWait.map(_ =>  H.extractTuples(mp.ref))
  }

  def reduce(
      op: GrBBinaryOp[A, A, A]
  )(implicit SVH: SparseVectorHandler[A]): Resource[F, GrBVector[F, A]] =
    reduce0(op, None)

  def reduceColumns(op: GrBBinaryOp[A, A, A])
                   (implicit SVH: SparseVectorHandler[A]): Resource[F, GrBVector[F, A]] =
    for {
      d <- Descriptor[F]
      _ <- Resource.liftF(d.set[Input0, Transpose])
      descP <- Resource.liftF(d.pointer.map(Some(_)))
      v <- reduce0(op, descP)
    } yield v

  def reduce0(
      op: GrBBinaryOp[A, A, A], desc: Option[GrBDescriptor]
  )(implicit SVH: SparseVectorHandler[A]): Resource[F, GrBVector[F, A]] = {
 (for {
      r <- Resource.liftF(self.nrows)
      v <- GrBVector[F, A](r)
    } yield v).evalMap[F, GrBVector[F, A]] { vec =>
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
            desc.map(_.pointer).orNull
          )
        }
      } yield vec
    }
  }

  def transpose[X](
      mask: Option[GrBMatrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  ): Resource[F, GrBMatrix[F, A]] = {
    implicit val H: SparseMatrixHandler[A] = self.H
    for {
      shape <- Resource.liftF(self.shape)
      c <- GrBMatrix[F, A](shape._2, shape._1)
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
      mask: Option[GrBMatrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  ): F[GrBMatrix[F, A]] = {

    implicit val H: SparseMatrixHandler[A] = self.H
    MatrixOps.extract(self)(from)(mask, accum, desc)
  }

  def reduce(init: A, monoid: GrBMonoid[A])(implicit R: Reduce[A]): F[A] =
    self.pointer.map { p => R.reduceAll(p.ref)(init, monoid) }

  def reduceN(
      monoid: GrBMonoid[A]
  )(implicit R: Reduce[A], N: Numeric[A]): F[A] =
    reduce(N.zero, monoid)

  def isEq(other: GrBMatrix[F, A])(implicit EQ: EqOp[A]): F[Boolean] =
    MatrixOps.isAll(self, other)(EQ)

  def duplicateF: Resource[F, GrBMatrix[F, A]] =
    for {
      p <- Resource.liftF(pointer)
      pDup <- Resource.fromAutoCloseable(
        self.F.delay(new MatrixPointer(GRBCORE.dupMatrix(p.ref)))
      )
    } yield new DefaultMatrix[F, A](F.pure(pDup))

  def show(limit: Int = 10)(implicit CT: ClassTag[A]): F[String] =
    for {
      s <- shape
      vals <- extract
      n <- nvals
    } yield vals match {
      case (is, js, vs) =>
        val extraItems = if (limit >= n) "" else " .. "
        (Stream(is: _*), Stream(js: _*), Stream(vs: _*)).zipped
          .take(limit)
          .map { case (i, j, v) => s"($i,$j):$v" }
          .mkString(s"[nvals=$n ${s._1}x${s._2}:$CT {", ", ", s"$extraItems}]")
    }

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

  def release: F[Unit] = pointer.map{mp => 
    GrBError.check(GRBCORE.freeMatrix(mp.ref))
  }

  def grbWait: F[Unit] = pointer.map{mp => 
    GrBError.check(GRBCORE.grbWaitMatrix(mp.ref))
  }
}


object GrBMatrix {

  def csc[F[_], A](rows: Long, cols: Long)(
      implicit M: Sync[F],
      G: GRB,
      SMH: SparseMatrixHandler[A]
  ): Resource[F, Long] = apply[F, A](rows, cols).evalMap {
    _.pointer.map(p => GRBCORE.makeCSC(p.ref))
  }

  def csr[F[_], A](rows: Long, cols: Long)(
      implicit M: Sync[F],
      G: GRB,
      SMH: SparseMatrixHandler[A]
  ): Resource[F, Long] = apply[F, A](rows, cols).evalMap {
    _.pointer.map(p => GRBCORE.makeCSR(p.ref))
  }

  def apply[F[_], A](rows: Long, cols: Long)(
      implicit M: Sync[F],
      G: GRB,
      SMH: SparseMatrixHandler[A]
  ): Resource[F, GrBMatrix[F, A]] = {
    Resource
      .fromAutoCloseable(M.delay {
        new MatrixPointer(SMH.createMatrix(rows, cols))
      })
      .map { mp => new DefaultMatrix(M.pure(mp)) }
  }

  private[g4s] def unsafe[F[_], A](rows: Long, cols:Long)
                         (implicit M:Sync[F], G:GRB, SMH:SparseMatrixHandler[A]):F[GrBMatrix[F, A]] =
    M.delay(new DefaultMatrix(M.pure(new MatrixPointer(SMH.createMatrix(rows, cols)))))

  private[g4s] def unsafeFn[F[_], A](rows: Long, cols:Long)
                         (implicit M:Sync[F], G:GRB, SMH:SparseMatrixHandler[A]):F[String => GrBMatrix[F, A]] =
    M.delay(_ => new DefaultMatrix(M.pure(new MatrixPointer(SMH.createMatrix(rows, cols)))))

  def fromTuples[F[_], A](
      rows: Long,
      cols: Long
  )(is: Array[Long], js: Array[Long], vs: Array[A])(
      implicit M: Sync[F],
      G: GRB,
      SMH: SparseMatrixHandler[A]
  ): Resource[F, GrBMatrix[F, A]] = {
    Resource
      .fromAutoCloseable(M.delay {
        new MatrixPointer(SMH.createMatrixFromTuples(rows, cols)(is, js, vs))
      })
      .map { mp => new DefaultMatrix(M.pure(mp)) }
  }
}

private class DefaultMatrix[F[_], A](override val pointer:F[MatrixPointer])(implicit val F:Sync[F],val G:GRB, val H:SparseMatrixHandler[A])
    extends GrBMatrix[F, A]
