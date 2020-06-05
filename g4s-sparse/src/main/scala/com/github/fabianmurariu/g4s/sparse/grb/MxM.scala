package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.g4s.sparse.mutable.MatrixLike

trait MxM[F[_]] extends MatrixLike[F] {
  def mxm[A, B, C:MatrixBuilder, X](
      semiring: GrBSemiring[A, B, C],
      mask: Option[GrBMatrix[X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(fa:F[A], f:F[B]): F[C]
}

object MxM {
  def apply[M[_]](implicit M:MxM[M]):MxM[M] = M
}
