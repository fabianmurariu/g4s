package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.g4s.sparse.mutable.MatrixLike
import zio._

trait MxM[M[_]] extends MatrixLike[M] {
  def mxm[A, B, C:MatrixBuilder, X](into:M[C])(
      semiring: GrBSemiring[A, B, C],
      mask: Option[GrBMatrix[X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(fa:M[A], f:M[B]): Task[M[C]]


  def mxmNew[A, B, C:MatrixBuilder, X](
      semiring: GrBSemiring[A, B, C],
      mask: Option[GrBMatrix[X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(fa:M[A], fb:M[B]): TaskManaged[M[C]] = {

    // FIXME: assumes no transpose, this can fail otherwise
    val rows = nrows(fa)
    val cols = ncols(fb)

    val cRes = make[C](rows, cols)

    cRes.mapM { c =>
      mxm(c)(semiring, mask, accum, desc)(fa, fb)
    }
  }
}

object MxM {
  def apply[M[_]](implicit M:MxM[M]):MxM[M] = M
}
