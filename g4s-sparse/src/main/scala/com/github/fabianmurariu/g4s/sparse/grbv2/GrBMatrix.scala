package com.github.fabianmurariu.g4s.sparse.grbv2

import cats.effect.Resource
import cats.implicits._
import cats.effect.Sync

import scala.reflect.ClassTag

import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.g4s.sparse.grb.{SparseMatrixHandler => SMH}
import com.github.fabianmurariu.g4s.sparse.grb.{SparseVectorHandler => SVH}
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.EqOp
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor

sealed class GrBMatrix[F[_]: Sync, A: Reduce: SMH: ClassTag](
    private[grbv2] val pointer: F[MatrixPointer]
)(implicit G: GRB)
    extends SparseMatrix[F, A] {

  private[grbv2] def liftPointer(mp: MatrixPointer): GrBMatrix[F, A] =
    new GrBMatrix(Sync[F].pure(mp))

  override def empty(rows: Long, cols: Long): Resource[F, this.type] = ???

  override def get(i: Long, j: Long): F[Option[A]] =
    GrBMatrixOps.get(pointer, i, j)

  override def set(i: Long, j: Long, a: A): F[Unit] =
    GrBMatrixOps.set[F, A](pointer, i, j, a)

  override def set(
      is: Iterable[Long],
      js: Iterable[Long],
      vs: Iterable[A]
  ): F[Unit] = GrBMatrixOps.set(pointer, is, js, vs)

  override def set(tuples: Iterable[(Long, Long, A)]): F[Unit] =
    GrBMatrixOps.set(pointer, tuples)

  override def extract: F[(Array[Long], Array[Long], Array[A])] =
    GrBMatrixOps.extract(pointer)

  override def reduceRows(
      op: GrBBinaryOp[A, A, A],
      desc: Option[GrBDescriptor] = None
  )(implicit VH: SVH[A]): Resource[F, GrBVector[F, A]] =
    GrBMatrixOps.reduce0(pointer, op, desc)

  def reduceColumns(
      op: GrBBinaryOp[A, A, A],
      desc: Option[GrBDescriptor]
  )(implicit VH: SVH[A]): Resource[F, GrBVector[F, A]] =
    for {
      d <- Descriptor[F]
      _ <- Resource.liftF(d.set[Input0, Transpose])
      descP <- Resource.liftF(d.pointer.map(Some(_)))
      v <- GrBMatrixOps.reduce0(pointer, op, descP)
    } yield v

  override def reduceRows(init: A, monoid: GrBMonoid[A]): F[A] =
    pointer.map { mp => Reduce[A].reduceAll(mp.ref)(init, monoid, None, None) }

  override def reduceColumns(init: A, monoid: GrBMonoid[A]): F[A] =
    (for {
      p <- Resource.liftF(pointer)
      d <- Descriptor[F]
      _ <- Resource.liftF(d.set[Input0, Transpose])
      descP <- Resource.liftF(d.pointer.map(Some(_)))
      a <- Resource.liftF(
        Sync[F].delay(Reduce[A].reduceAll(p.ref)(init, monoid, None, descP))
      )
    } yield a).use(Sync[F].pure(_))

  override def reduceN(monoid: GrBMonoid[A])(implicit N: Numeric[A]): F[A] =
    reduceRows(N.zero, monoid)

  def apply[R1: GrBRangeLike, R2: GrBRangeLike](
      isRange: R1,
      jsRange: R2
  ): MatrixSelection[F, A] = {
    val (ni, is) = GrBRangeLike[R1].toGrB(isRange)
    val (nj, js) = GrBRangeLike[R2].toGrB(jsRange)
    new MatrixSelection(this, is, ni, js, nj)
  }

  override def transpose[X](
      mask: Option[GrBMatrix[F, X]],
      accum: Option[GrBBinaryOp[A, A, A]],
      desc: Option[GrBDescriptor]
  ): Resource[F, GrBMatrix[F, A]] = {
    for {
      s <- Resource.liftF(shape)
      out <- GrBMatrix[F, A](s._2, s._1)
      _ <- Resource.liftF(MatrixOps
            .transpose[F, A, A, A, X](out.pointer)(pointer)(mask, accum, desc))
    } yield out
  }

  override def update[X](
      from: MatrixSelection[F, A],
      mask: Option[GrBMatrix[F, X]],
      accum: Option[GrBBinaryOp[A, A, A]],
      desc: Option[GrBDescriptor]
  ): F[GrBMatrix[F, A]] =
    MatrixOps
      .extract(pointer)(from)(mask, accum, desc)
      .map(mp => new GrBMatrix(Sync[F].pure(mp)))

  def isEq(other: GrBMatrix[F, A])(implicit EQ: EqOp[A]): F[Boolean] =
    MatrixOps.isAll(this, other)(EQ)

  override def duplicateF: Resource[F, GrBMatrix[F, A]] =
    GrBMatrixOps.duplicate(pointer).map { mp =>
      new GrBMatrix[F, A](Sync[F].pure(mp))
    }

  override def show(limit: Int): F[String] =
    GrBMatrixOps.show(pointer, limit)

  override def remove(i: Long, j: Long): F[Unit] =
    GrBMatrixOps.remove(pointer, i, j)

  override def ncols: F[Long] =
    GrBMatrixOps.ncols(pointer)

  override def nrows: F[Long] =
    GrBMatrixOps.nrows(pointer)

  override def shape: F[(Long, Long)] =
    GrBMatrixOps.shape(pointer)

  override def nvals: F[Long] =
    GrBMatrixOps.nvals(pointer)

  override def resize(rows: Long, cols: Long): F[Unit] =
    GrBMatrixOps.resize(pointer, rows, cols)

  override def clear: F[Unit] =
    GrBMatrixOps.clear(pointer)

  override private[grbv2] def release: F[Unit] =
    GrBMatrixOps.release(pointer)

  /**
    * Forces all the pending operations on this matrix to complete
    */
  def force: F[Unit] = GrBMatrixOps.force(pointer)

}

object GrBMatrix {

  def csc[F[_], A: ClassTag: Reduce](rows: Long, cols: Long)(
      implicit M: Sync[F],
      G: GRB,
      SMH: SMH[A]
  ): Resource[F, Long] = apply[F, A](rows, cols).evalMap {
    _.pointer.map(p => GRBCORE.makeCSC(p.ref))
  }

  def csr[F[_], A: ClassTag: Reduce](rows: Long, cols: Long)(
      implicit M: Sync[F],
      G: GRB,
      SMH: SMH[A]
  ): Resource[F, Long] = apply[F, A](rows, cols).evalMap {
    _.pointer.map(p => GRBCORE.makeCSR(p.ref))
  }

  def apply[F[_], A: ClassTag: Reduce](rows: Long, cols: Long)(
      implicit M: Sync[F],
      G: GRB,
      SMH: SMH[A]
  ): Resource[F, GrBMatrix[F, A]] = {
    Resource
      .fromAutoCloseable(M.delay {
        new MatrixPointer(SMH.createMatrix(rows, cols))
      })
      .map { mp => new GrBMatrix(M.pure(mp)) }
  }

  private[g4s] def unsafe[F[_], A: ClassTag: Reduce](rows: Long, cols: Long)(
      implicit M: Sync[F],
      G: GRB,
      SMH: SMH[A]
  ): F[GrBMatrix[F, A]] =
    M.delay(
      new GrBMatrix(M.pure(new MatrixPointer(SMH.createMatrix(rows, cols))))
    )

  private[g4s] def unsafeFn[F[_], A: ClassTag: Reduce](rows: Long, cols: Long)(
      implicit M: Sync[F],
      G: GRB,
      SMH: SMH[A]
  ): F[String => GrBMatrix[F, A]] =
    M.delay(_ =>
      new GrBMatrix(M.pure(new MatrixPointer(SMH.createMatrix(rows, cols))))
    )

  def fromTuples[F[_], A: ClassTag: Reduce](
      rows: Long,
      cols: Long
  )(is: Array[Long], js: Array[Long], vs: Array[A])(
      implicit M: Sync[F],
      G: GRB,
      SMH: SMH[A]
  ): Resource[F, GrBMatrix[F, A]] = {
    Resource
      .fromAutoCloseable(M.delay {
        new MatrixPointer(SMH.createMatrixFromTuples(rows, cols)(is, js, vs))
      })
      .map { mp => new GrBMatrix(M.pure(mp)) }
  }

}
