package com.github.fabianmurariu.g4s.sparse.grb.instances

import com.github.fabianmurariu.g4s.sparse.grb.ElemWise
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grb.MatrixBuilder
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import zio._

trait ElemWiseInstance extends ElemWise[GrBMatrix] {

  override def union[A, B, C: MatrixBuilder, X](c: GrBMatrix[C])(
      add: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[GrBMatrix[X]],
      accum: Option[GrBBinaryOp[C, C, C]],
      desc: Option[GrBDescriptor]
  )(fa: GrBMatrix[A], fb: GrBMatrix[B]): Task[GrBMatrix[C]]= IO.effect{

      add match {
        case Left(binOp) =>
          val res = GRBOPSMAT.elemWiseAddUnionBinOp(
            c.pointer,
            mask.map(_.pointer).orNull,
            accum.map(_.pointer).orNull,
            binOp.pointer,
            fa.pointer,
            fb.pointer,
            desc.map(_.pointer).orNull
          )
          assert(res == 0, s"GRB error: $res")
        case Right(monoid) =>
          val res = GRBOPSMAT.elemWiseAddUnionMonoid(
            c.pointer,
            mask.map(_.pointer).orNull,
            accum.map(_.pointer).orNull,
            monoid.pointer,
            fa.pointer,
            fb.pointer,
            desc.map(_.pointer).orNull
          )
          assert(res == 0, s"GRB error: $res")
      }
    c
  }

  override def intersection[A, B, C: MatrixBuilder, X](c:GrBMatrix[C])(
      mul: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[GrBMatrix[X]],
      accum: Option[GrBBinaryOp[C, C, C]],
      desc: Option[GrBDescriptor]
  )(fa: GrBMatrix[A], fb: GrBMatrix[B]): Task[GrBMatrix[C]] = IO.effect{

    mul match {
      case Left(binOp) =>
        val res = GRBOPSMAT.elemWiseMulIntersectBinOp(
          c.pointer,
          mask.map(_.pointer).orNull,
          accum.map(_.pointer).orNull,
          binOp.pointer,
          fa.pointer,
          fb.pointer,
          desc.map(_.pointer).orNull
        )
        assert(res == 0)
      case Right(monoid) =>
        val res = GRBOPSMAT.elemWiseMulIntersectMonoid(
          c.pointer,
          mask.map(_.pointer).orNull,
          accum.map(_.pointer).orNull,
          monoid.pointer,
          fa.pointer,
          fb.pointer,
          desc.map(_.pointer).orNull
        )
        assert(res == 0)
    }

    c
  }

}
