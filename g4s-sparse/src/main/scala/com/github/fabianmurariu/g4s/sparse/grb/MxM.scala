package com.github.fabianmurariu.g4s.sparse.grb

import cats.implicits._

import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import cats.MonadError

trait MxM[F[_]] {

  def mxm[A, B, C, X](
      into: GrBMatrix[F, C]
  )(fa: GrBMatrix[F, A], fb: GrBMatrix[F, B])(
      semiring: GrBSemiring[A, B, C],
      mask: Option[GrBMatrix[F, X]] = None,
      accum: Option[GrBBinaryOp[C, C, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(
      implicit F: MonadError[F, Throwable]
  ): F[GrBMatrix[F, C]] = {

    for {
      c <- into.pointer
      a <- fa.pointer
      b <- fb.pointer
      m <- mask.map(_.pointer.map(_.ref)).getOrElse(F.pure(null))
      _ <- F.fromEither{
        Either.catchNonFatal(GrBError.check(
          GRBOPSMAT.mxm(
            c.ref,
            m,
            accum.map(_.pointer).orNull,
            semiring.pointer,
            a.ref,
            b.ref,
            desc.map(_.pointer).orNull
          )
        ))
      }
    } yield into

  }

}

object MxM {

  def apply[F[_]](implicit M: MxM[F]) = M

  implicit def mxmInstance[F[_]]: MxM[F] = new MxM[F] {}

  def evalOrFail[F[_], A](
      f: => A
  )(implicit F: MonadError[F, Throwable]): F[A] = {
    F.fromEither(Either.catchNonFatal(f))
  }
}
