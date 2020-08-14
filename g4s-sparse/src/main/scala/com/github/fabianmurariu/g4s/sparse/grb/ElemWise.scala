package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.unsafe.GRBALG
import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import com.github.fabianmurariu.g4s.sparse.mutable.MatrixLike
import zio._
import MatrixLike.ops._

trait ElemWise[M[_]] {

  def union[A, B, C: MatrixBuilder, X](into: M[C])(
      add: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[GrBMatrix[X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(fa: M[A], fb: M[B]): Task[M[C]]

  def unionNew[A, B, C: MatrixBuilder, X](
      add: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[GrBMatrix[X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(fa: M[A], fb: M[B])(implicit M:MatrixLike[M]): Managed[Throwable, M[C]] = {

    val rows = M.nrows(fa)
    val cols = M.ncols(fa)

    val cRes = M.make[C](rows, cols)

    cRes.mapM { c => union(c)(add, mask, accum, desc)(fa, fb) }

  }

  def intersection[A, B, C: MatrixBuilder, X]
  (into: M[C]) (
      mul: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[GrBMatrix[X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(fa: M[A], fb: M[B]): Task[M[C]]

  def intersectionNew[A, B, C: MatrixBuilder, X](
      mul: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[GrBMatrix[X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(fa: M[A], fb: M[B])(implicit M:MatrixLike[M]): Managed[Throwable, M[C]] = {

    val rows = M.nrows(fa)
    val cols = M.ncols(fa)

    val cRes = M.make[C](rows, cols)

    cRes.mapM { c => intersection(c)(mul, mask, accum, desc)(fa, fb) }
  }
}
