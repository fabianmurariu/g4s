package com.github.fabianmurariu.g4s.sparse.grbv2

import cats.effect.Resource
import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.EqOp
import com.github.fabianmurariu.g4s.sparse.grb.{SparseVectorHandler => SVH}

trait SparseMatrix[F[_],@specialized(Boolean, Byte, Short, Int, Long, Float, Double) A] {

  /**
    * Forces all the pending operations on this matrix to complete
    */
  def force: F[Unit]

  def get(i: Long, j: Long): F[Option[A]]

  def set(i: Long, j: Long, a: A): F[Unit]

  def set(is: Iterable[Long], js: Iterable[Long], vs: Iterable[A]): F[Unit]

  def set(tuples: Iterable[(Long, Long, A)]): F[Unit]

  def extract: F[(Array[Long], Array[Long], Array[A])]

  def reduceRows(
      op: GrBBinaryOp[A, A, A],
      desc: Option[GrBDescriptor] = None
  )(implicit VH:SVH[A]): Resource[F, GrBVector[F, A]]

  def reduceColumns(
      op: GrBBinaryOp[A, A, A],
      desc: Option[GrBDescriptor] = None
  )(implicit VH:SVH[A]): Resource[F, GrBVector[F, A]]

  def reduceRows(init: A, monoid: GrBMonoid[A]): F[A]

  def reduceColumns(init: A, monoid: GrBMonoid[A]): F[A]

  def reduceN(monoid: GrBMonoid[A])(implicit N: Numeric[A]): F[A]

  def transpose[X](
      mask: Option[GrBMatrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  ): Resource[F, GrBMatrix[F, A]]

  def apply[R1: GrBRangeLike, R2: GrBRangeLike](
      isRange: R1,
      jsRange: R2
  ): MatrixSelection[F, A]

  def update[X](
      from: MatrixSelection[F, A],
      mask: Option[GrBMatrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  ): F[GrBMatrix[F, A]]

  def isEq(other: GrBMatrix[F, A])(implicit EQ: EqOp[A]): F[Boolean]

  def duplicateF: Resource[F, GrBMatrix[F, A]]

  def show(limit: Int = 10): F[String]

  def remove(i: Long, j: Long): F[Unit]

  def ncols: F[Long]

  def nrows: F[Long]

  def shape: F[(Long, Long)]

  /**
    * Number of non empty values
    *
    * @return
    */
  def nvals: F[Long]

  def resize(rows: Long, cols: Long): F[Unit]

  def clear: F[Unit]

  /**
    *
    * @return
    *   An empty variant of this matrix
    *   with the same type
    */
  def empty(rows: Long, cols: Long): Resource[F, this.type]

  /**
    * release the native memory
    *
    * @return
    */
  private[grbv2] def release: F[Unit]

}
