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
import zio._

trait MxMInstance extends MxM[GrBMatrix] {

  override def mxm[A, B, C:MatrixBuilder, X](c: GrBMatrix[C])(
      semiring: GrBSemiring[A, B, C],
      mask: Option[GrBMatrix[X]],
      accum: Option[GrBBinaryOp[C, C, C]],
      desc: Option[GrBDescriptor]
  )(fa: GrBMatrix[A], fb: GrBMatrix[B]): Task[GrBMatrix[C]] = IO.effect{

    val res = GRBOPSMAT.mxm(c.pointer, mask.map(_.pointer).orNull, accum.map(_.pointer).orNull, semiring.pointer, fa.pointer, fb.pointer, desc.map(_.pointer).orNull)
    assert(res == 0)
    c
  }

}
