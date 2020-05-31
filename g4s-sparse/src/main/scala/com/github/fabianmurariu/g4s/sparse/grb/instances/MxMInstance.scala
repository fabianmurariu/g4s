package com.github.fabianmurariu.g4s.sparse.grb.instances

import com.github.fabianmurariu.g4s.sparse.grb.MxM
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grb.{
  GrBBinaryOp,
  GrBDescriptor,
  GrBSemiring
}
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import com.github.fabianmurariu.g4s.sparse.grb.MatrixBuilder

trait MxMInstance extends MxM[GrBMatrix] {

  override def mxm[A, B, C:MatrixBuilder, X](
      semiring: GrBSemiring[A, B, C],
      mask: Option[GrBMatrix[X]],
      accum: Option[GrBBinaryOp[C, C, C]],
      desc: Option[GrBDescriptor]
  )(fa: GrBMatrix[A], fb: GrBMatrix[B]): GrBMatrix[C] = {

    // FIXME: assumes no transpose, this can fail otherwise
    val rows = fa.nrows
    val cols = fb.ncols

    val c = GrBMatrix[C](rows, cols)

    val res = GRBOPSMAT.mxm(c.pointer, mask.map(_.pointer).orNull, accum.map(_.pointer).orNull, semiring.pointer, fa.pointer, fb.pointer, desc.map(_.pointer).orNull)
    assert(res == 0)
    // FIXME: either ensure the method can't fail or wrap GrBMatrix[C] with some effect
    c
  }

}
