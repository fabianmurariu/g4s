package com.github.fabianmurariu.g4s.sparse.mutable

import com.github.fabianmurariu.g4s.sparse.grb.ElemWise
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.MxM
import simulacrum.typeclass
import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.EqOp
import com.github.fabianmurariu.g4s.sparse.grb.MatrixHandler
import scala.{specialized => sp}
import com.github.fabianmurariu.g4s.sparse.grb.MatrixBuilder

trait Matrix[M[_]] extends MatrixLike[M] with ElemWise[M] with MxM[M] { self =>

  /**
    * Returns true if op(fa, fb) is true for each pair of items (elem wise)
    */
  def isAny[A](fa1: M[A])(fa2: M[A], op: GrBBinaryOp[A, A, Boolean])(
      implicit R: Reduce[M, Boolean]
  ): Boolean = {
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
        val fa3 = self.intersection(Left(op))(fa1, fa2)
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

  def isEq[A](
      fa1: M[A]
  )(fa2: M[A])(implicit R: Reduce[M, Boolean], EQ: EqOp[A]): Boolean = {
    isAny(fa1)(fa2, EQ)
  }

  def make[A](rows:Long, cols:Long)(implicit MB:MatrixBuilder[A]): M[A]
}

object Matrix {
  trait Ops[M[_], A] extends Any {

    def self: M[A]
    def M: Matrix[M]

    def isAny(fa2: M[A], op: GrBBinaryOp[A, A, Boolean])(
        implicit R: Reduce[M, Boolean]
    ): Boolean = {
      M.isAny(self)(fa2, op)
    }

    def isEq(
        fa2: M[A]
    )(implicit R: Reduce[M, Boolean], EQ: EqOp[A]): Boolean = {
      M.isEq(self)(fa2)
    }

    def get(i: Long, j: Long)(implicit MH: MatrixHandler[M, A]): Option[A] = {
      M.get(self)(i, j)
    }

    def set(i: Long, j: Long, a: A)(implicit MH: MatrixHandler[M, A]): Unit = {
      M.set(self)(i, j, a)
    }

    def nvals: Long = M.nvals(self)
    def nrows: Long = M.nrows(self)
    def ncols: Long = M.ncols(self)

    def clear: Unit = M.clear(self)

    def duplicate: M[A] = M.duplicate(self)

    def resize(rows: Long, cols: Long): Unit = M.resize(self)(rows, cols)

    def release: Unit = M.release(self)

  }

  def apply[M[_]](implicit M:Matrix[M]): Matrix[M] = M
}
