package com.github.fabianmurariu.g4s.sparse.mutable

import java.lang.AutoCloseable
import java.nio.Buffer
import scala.{specialized => sp}
import simulacrum.typeclass
import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.ElemWise
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid

/**
  * The Uber type class for anything matrix related, anything tha implements this
  * type class should be somewhat usable as a matrix, there are caveats and dependencies
  * to [[MatrixHandler]]
  */
@typeclass trait Matrix[F[_]] {
  def nvals[A](f: F[A]): Long
  def nrows[A](f: F[A]): Long
  def ncols[A](f: F[A]): Long
  def clear[A](f: F[A]): Unit
  def duplicate[A](f: F[A]): F[A]
  def resize[A](f: F[A])(rows: Long, cols: Long): Unit

  def release[A](f: F[A]): Unit

  def get[A](
      f: F[A]
  )(i: Long, j: Long)(implicit MH: MatrixHandler[F, A]): Option[A] = {
    MH.get(f)(i, j)
  }

  def set[@sp(Boolean, Byte, Short, Int, Long, Float, Double) A](
      f: F[A]
  )(i: Long, j: Long, a: A)(implicit MH: MatrixHandler[F, A]): Unit = {
    MH.set(f)(i, j, a)
  }

  /**
    * Returns true if op(fa, fb) is true for each pair of items (elem wise)
    * FIXME: Not sure if this belongs here or we need an extension to Matrix to use GrB stuff
    */
  def isAny[A](fa1: F[A])(fa2: F[A], op: GrBBinaryOp[A, A, Boolean])
           (implicit EW: ElemWise[F], R: Reduce[F, Boolean]): Boolean = {
    if (nrows(fa1) != nrows(fa2))
      false
    else if (ncols(fa1) != ncols(fa2))
      false
    else {
      val nvals1 = nvals(fa1)
      val nvals2 = nvals(fa2)
      if (nvals1 != nvals2)
        false
      else {
        val fa3 = EW.intersection(Left(op))(fa1, fa2)
        if (nvals1 != nvals(fa3)) {
          val out = false
          release(fa3)
          out
        } else {
          val m = GrBMonoid[Boolean](GrBBinaryOp.boolean.land, false)
          val out = R.reduceAll(fa3)(false, m, None, None)
          release(fa3)
          m.close()
          out
        }
      }
    }
  }

}

trait MatrixHandler[F[_], @sp(Boolean, Byte, Short, Int, Long, Float, Double) A] {
  @inline
  def set(p: F[A])(i: Long, j: Long, x: A): Unit
  @inline
  def get(f: F[A])(i: Long, j: Long): Option[A]
}

object MatrixHandler {
  def apply[F[_], A](
      getFn: (F[A], Long, Long) => Option[A]
  )(setFn: (F[A], Long, Long, A) => Unit): MatrixHandler[F, A] =
    new MatrixHandler[F, A] {

      override def set(f: F[A])(i: Long, j: Long, x: A): Unit =
        setFn(f, i, j, x)

      override def get(f: F[A])(i: Long, j: Long): Option[A] = getFn(f, i, j)

    }
}
