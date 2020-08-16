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
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import zio._
import cats.effect.Resource

trait Matrix[M[_]] extends MatrixLike[M] with ElemWise[M] with MxM[M] { self =>

  /**
    * Returns true if op(fa, fb) is true for each pair of items (elem wise)
    */
  def isAny[A](fa1: M[A])(fa2: M[A], op: GrBBinaryOp[A, A, Boolean])(
      implicit R: Reduce[M, Boolean],
      M: MatrixLike[M]
  ): Task[Boolean] = IO.effectSuspend {
    if (nrows(fa1) != nrows(fa2))
      IO.succeed(false)
    else if (ncols(fa1) != ncols(fa2))
      IO.succeed(false)
    else {
      val nvals1 = nvals(fa1)
      val nvals2 = nvals(fa2)
      if (nvals1 != nvals2)
        IO.succeed(false)
      else {
        val fa3Res = self.intersectionNew(Left(op))(fa1, fa2)
        fa3Res.use { fa3 =>
          if (nvals1 != nvals(fa3)) {
            IO.succeed(false)
          } else {
            val mRes = GrBMonoid[Boolean](BuiltInBinaryOps.boolean.land, false)
            mRes.use { m => R.reduceAll(fa3)(false, m, None, None) }
          }
        }

      }
    }
  }

  def isEq[A](fa1: M[A], fa2: M[A])(
      implicit R: Reduce[M, Boolean],
      EQ: EqOp[A],
      M: MatrixLike[M]
  ): Task[Boolean] = {
    isAny(fa1)(fa2, EQ)
  }

}

object Matrix {
  trait Ops[M[_], A] extends Any {

    def self: M[A]
    def M: Matrix[M]

    def isAny(fa2: M[A], op: GrBBinaryOp[A, A, Boolean])(
        implicit R: Reduce[M, Boolean],
        ML: MatrixLike[M]
    ): Task[Boolean] = {
      M.isAny(self)(fa2, op)
    }

    def isEq(
        fa2: M[A]
    )(
        implicit R: Reduce[M, Boolean],
        EQ: EqOp[A],
        ML: MatrixLike[M]
    ): Task[Boolean] = {
      M.isEq(self, fa2)
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

    def duplicate: Managed[Throwable, M[A]] = M.duplicate(self)

    def resize(rows: Long, cols: Long): Unit = M.resize(self)(rows, cols)

    def copyData(implicit MH: MatrixHandler[M, A]) =
      MH.copyData(self)
  }

  def apply[M[_]](implicit M: Matrix[M]): Matrix[M] = M
}

