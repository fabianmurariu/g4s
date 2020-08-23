package com.github.fabianmurariu.g4s.sparse.grb

import cats.implicits._

import com.github.fabianmurariu.g4s.sparse.grbv2.Matrix
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import simulacrum.typeclass
import cats.effect.Sync

trait MxM[F[_]] {

  def mxm[A, B, C, X](into: Matrix[F, C])(
      semiring: GrBSemiring[A, B, C],
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
      _ <- Sync[F].delay {
        val res = GRBOPSMAT.mxm(
          c.ref,
          m,
          accum.map(_.pointer).orNull,
          semiring.pointer,
          a.ref,
          b.ref,
          desc.map(_.pointer).orNull
        )
        assert(res == 0)
      }
    } yield into
  }

}

object MxM{

  def apply[F[_]](implicit M:MxM[F]) = M

  implicit def mxmInstance[F[_]]:MxM[F] = new MxM[F]{}
}
