package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.unsafe.GRBALG
import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.unsafe.GRBOPSMAT

trait ElemWise[F[_]] {

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

object ElemWise {

  import com.github.fabianmurariu.g4s.sparse.mutable.Matrix.ops._

  implicit val elemWiseForGrBMatrix: ElemWise[GrBMatrix] =
    new ElemWise[GrBMatrix] {

      override def union[A, B, C: MatrixBuilder, X](
          add: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
          mask: Option[GrBMatrix[X]],
          accum: Option[GrBBinaryOp[C, C, C]],
          desc: Option[GrBDescriptor]
      )(fa: GrBMatrix[A], fb: GrBMatrix[B]): GrBMatrix[C] = {

        val rows = fa.nrows
        val cols = fa.ncols

        val c = GrBMatrix[C](rows, cols)

        add match {
          case Left(binOp) =>
            GRBOPSMAT.elemWiseAddUnionBinOp(
              c.pointer,
              mask.map(_.pointer).orNull,
              accum.map(_.pointer).orNull,
              binOp.pointer,
              fa.pointer,
              fb.pointer,
              desc.map(_.pointer).orNull
            )
          case Right(monoid) =>
            GRBOPSMAT.elemWiseAddUnionMonoid(
              c.pointer,
              mask.map(_.pointer).orNull,
              accum.map(_.pointer).orNull,
              monoid.pointer,
              fa.pointer,
              fb.pointer,
              desc.map(_.pointer).orNull
            )
        }

        c
      }

      override def intersection[A, B, C: MatrixBuilder, X](
          mul: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
          mask: Option[GrBMatrix[X]],
          accum: Option[GrBBinaryOp[C, C, C]],
          desc: Option[GrBDescriptor]
      )(fa: GrBMatrix[A], fb: GrBMatrix[B]): GrBMatrix[C] = {

        val rows = fa.nrows
        val cols = fa.ncols

        val c = GrBMatrix[C](rows, cols)

        mul match {
          case Left(binOp) =>
            GRBOPSMAT.elemWiseMulIntersectBinOp(
              c.pointer,
              mask.map(_.pointer).orNull,
              accum.map(_.pointer).orNull,
              binOp.pointer,
              fa.pointer,
              fb.pointer,
              desc.map(_.pointer).orNull
            )
          case Right(monoid) =>
            GRBOPSMAT.elemWiseMulIntersectMonoid(
              c.pointer,
              mask.map(_.pointer).orNull,
              accum.map(_.pointer).orNull,
              monoid.pointer,
              fa.pointer,
              fb.pointer,
              desc.map(_.pointer).orNull
            )
        }

        c
      }

    }

}
