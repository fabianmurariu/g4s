package com.github.fabianmurariu.g4s.sparse.grb.instances

import com.github.fabianmurariu.g4s.sparse.grb.ElemWise
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grb.MatrixBuilder
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import com.github.fabianmurariu.unsafe.GRBOPSMAT

trait ElemWiseInstance extends ElemWise[GrBMatrix] {

  override def unionInto[A, B, C: MatrixBuilder, X](
      add: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[GrBMatrix[X]],
      accum: Option[GrBBinaryOp[C, C, C]],
      desc: Option[GrBDescriptor]
  )(fa: GrBMatrix[A], fb: GrBMatrix[B]): GrBMatrix[C] = {

    add match {
      case Left(binOp) =>
        val res = GRBOPSMAT.elemWiseAddUnionBinOp(
          fa.pointer,
          mask.map(_.pointer).orNull,
          accum.map(_.pointer).orNull,
          binOp.pointer,
          fa.pointer,
          fb.pointer,
          desc.map(_.pointer).orNull
        )
        assert(res == 0)
      case Right(monoid) =>
        val res = GRBOPSMAT.elemWiseAddUnionMonoid(
          fa.pointer,
          mask.map(_.pointer).orNull,
          accum.map(_.pointer).orNull,
          monoid.pointer,
          fa.pointer,
          fb.pointer,
          desc.map(_.pointer).orNull
        )
        assert(res == 0)
    }

    fa.asInstanceOf[GrBMatrix[C]]
  }

  override def union[A, B, C: MatrixBuilder, X](
      add: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[GrBMatrix[X]],
      accum: Option[GrBBinaryOp[C, C, C]],
      desc: Option[GrBDescriptor]
  )(fa: GrBMatrix[A], fb: GrBMatrix[B]): GrBMatrix[C] = {

    val rows = nrows(fa)
    val cols = ncols(fa)

    val c = GrBMatrix[C](rows, cols)

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
        assert(res == 0)
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
        assert(res == 0)
    }

    c
  }

  override def intersection[A, B, C: MatrixBuilder, X](
      mul: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[GrBMatrix[X]],
      accum: Option[GrBBinaryOp[C, C, C]],
      desc: Option[GrBDescriptor]
  )(fa: GrBMatrix[A], fb: GrBMatrix[B]): GrBMatrix[C] = {

    val rows = nrows(fa)
    val cols = ncols(fa)

    val c = GrBMatrix[C](rows, cols)

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

  override def intersectionInto[A, B, C: MatrixBuilder, X](
      mul: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[GrBMatrix[X]],
      accum: Option[GrBBinaryOp[C, C, C]],
      desc: Option[GrBDescriptor]
  )(fa: GrBMatrix[A], fb: GrBMatrix[B]): GrBMatrix[C] = {

    mul match {
      case Left(binOp) =>
        val res = GRBOPSMAT.elemWiseMulIntersectBinOp(
          fa.pointer,
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
          fa.pointer,
          mask.map(_.pointer).orNull,
          accum.map(_.pointer).orNull,
          monoid.pointer,
          fa.pointer,
          fb.pointer,
          desc.map(_.pointer).orNull
        )
        assert(res == 0)
    }

    fa.asInstanceOf[GrBMatrix[C]]
  }

}
