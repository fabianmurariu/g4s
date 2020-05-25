package com.github.fabianmurariu.g4s.sparse.grb

trait MxM[F[_]] {
  def mxm[A, B, C, X](
      semiring: GrBSemiring[A, B, C],
      mask: Option[GrBMatrix[X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(fa:F[A], f:F[B]): F[C]
}
