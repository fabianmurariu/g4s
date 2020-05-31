package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.unsafe.GRBALG
import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import com.github.fabianmurariu.g4s.sparse.mutable.MatrixLike

trait ElemWise[F[_]] extends MatrixLike[F] {

  def union[A, B, C: MatrixBuilder, X](
      add: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[GrBMatrix[X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(fa: F[A], fb: F[B]): F[C]

  def intersection[A, B, C: MatrixBuilder, X](
      mul: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[GrBMatrix[X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(fa: F[A], fb: F[B]): F[C]
}

