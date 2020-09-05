package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.unsafe.GRBOPSMAT
import com.github.fabianmurariu.g4s.sparse.grbv2.Matrix
import cats.implicits._
import cats.effect.Sync

trait ElemWise[F[_]] {

  def union[A, B, C, X](into: Matrix[F, C])(
      add: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[Matrix[F, X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(fa: Matrix[F, A], fb: Matrix[F, B])(
      implicit F: Sync[F]
  ): F[Matrix[F, C]] = {
    for {
      c <- into.pointer
      a <- fa.pointer
      b <- fb.pointer
      m <- mask.map(_.pointer.map(_.ref)).getOrElse(F.pure(null))
      _ <- F.delay {
      add match {
        case Left(binOp) =>
          val res = GRBOPSMAT.elemWiseAddUnionBinOp(
            c.ref,
            m,
            accum.map(_.pointer).orNull,
            binOp.pointer,
            a.ref,
            b.ref,
            desc.map(_.pointer).orNull
          )
          assert(res == 0, s"GRB error: $res")
        case Right(monoid) =>
          val res = GRBOPSMAT.elemWiseAddUnionMonoid(
            c.ref,
            m,
            accum.map(_.pointer).orNull,
            monoid.pointer,
            a.ref,
            b.ref,
            desc.map(_.pointer).orNull
          )
          assert(res == 0, s"GRB error: $res")
      }
      }
    } yield into
  }


  def intersect[A, B, C, X](into: Matrix[F, C])(
      add: Either[GrBBinaryOp[A, B, C], GrBMonoid[C]],
      mask: Option[Matrix[F, X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(fa: Matrix[F, A], fb: Matrix[F, B])(
      implicit F: Sync[F]
  ): F[Matrix[F, C]] = {
    for {
      c <- into.pointer
      a <- fa.pointer
      b <- fb.pointer
      m <- mask.map(_.pointer.map(_.ref)).getOrElse(F.pure(null))
      _ <- F.delay {
      add match {
        case Left(binOp) =>
          val res = GRBOPSMAT.elemWiseMulIntersectBinOp(
            c.ref,
            m,
            accum.map(_.pointer).orNull,
            binOp.pointer,
            a.ref,
            b.ref,
            desc.map(_.pointer).orNull
          )
          assert(res == 0, s"GRB error: $res")
        case Right(monoid) =>
          val res = GRBOPSMAT.elemWiseMulIntersectMonoid(
            c.ref,
            m,
            accum.map(_.pointer).orNull,
            monoid.pointer,
            a.ref,
            b.ref,
            desc.map(_.pointer).orNull
          )
          assert(res == 0, s"GRB error: $res")
      }
      }
    } yield into
  }

}

object ElemWise {
  def apply[F[_]](implicit EW:ElemWise[F]) = EW

  implicit def anyEffectElemWise[F[_]]:ElemWise[F] = new ElemWise[F] {}
}
