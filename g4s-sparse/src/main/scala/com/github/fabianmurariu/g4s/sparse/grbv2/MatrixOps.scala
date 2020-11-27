package com.github.fabianmurariu.g4s.sparse.grbv2

import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import cats.effect.Sync
import cats.implicits._
import com.github.fabianmurariu.g4s.sparse.grb.GrBError
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import com.github.fabianmurariu.g4s.sparse.grb.SparseMatrixHandler
import cats.data.EitherT
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.sparse.grb.ElemWise

object MatrixOps extends TransposeOps with ExtractOps with IsAllOps

trait TransposeOps {

  def transpose[F[_], A, B, C, X](out: GrBMatrix[F, A])(in: GrBMatrix[F, A])(
      mask: Option[GrBMatrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, B, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(implicit S: Sync[F], G:GRB, SMH: SparseMatrixHandler[C]): F[GrBMatrix[F, C]] =
    for {
      mpIn <- in.pointer
      mpOut <- out.pointer
      m <- mask.map(_.pointer.map(_.ref)).getOrElse(S.pure(null))
      _ <- S.delay {
        GrBError.check(
          GRBOPSMAT.transpose(
            mpOut.ref,
            m,
            accum.map(_.pointer).orNull,
            mpIn.ref,
            desc.map(_.pointer).orNull
          )
        )
      }
    } yield new DefaultMatrix(S.pure(mpOut))

}

trait ExtractOps {
  // to = from(I, J)

  def extract[F[_], A, B, C, X](
      to: GrBMatrix[F, A]
  )(from: MatrixSelection[F, A])(
      mask: Option[GrBMatrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, B, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(implicit S: Sync[F], G:GRB, SMH: SparseMatrixHandler[C]): F[GrBMatrix[F, C]] =
    for {
      mpIn <- from.mat.pointer
      mpOut <- to.pointer
      grbMask <- mask.map(_.pointer.map(_.ref)).getOrElse(S.pure(null))
      _ <- S.delay {
        GrBError.check(
          GRBOPSMAT.extract(
            mpOut.ref,
            grbMask,
            accum.map(_.pointer).orNull,
            mpIn.ref,
            from.is,
            from.ni,
            from.js,
            from.nj,
            desc.map(_.pointer).orNull
          )
        )
      }
    } yield new DefaultMatrix(S.pure(mpOut))

  // to(I, J) = from
  def assign[F[_], A, B, C, X](
      to: MatrixSelection[F, A]
  )(from: GrBMatrix[F, A])(
      mask: Option[GrBMatrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, B, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(implicit S: Sync[F], G:GRB, SMH: SparseMatrixHandler[C]): F[GrBMatrix[F, C]] =
    for {
      mpIn <- from.pointer
      mpOut <- to.mat.pointer
      grbMask <- mask.map(_.pointer.map(_.ref)).getOrElse(S.pure(null))
      _ <- S.delay {
        GrBError.check(
          GRBOPSMAT.assign(
            mpOut.ref,
            grbMask,
            accum.map(_.pointer).orNull,
            mpIn.ref,
            to.is,
            to.ni,
            to.js,
            to.nj,
            desc.map(_.pointer).orNull
          )
        )
      }
    } yield new DefaultMatrix(S.pure(mpOut))

}

trait IsAllOps {

  def isAll[F[_]:Sync, A](self:GrBMatrix[F, A], other: GrBMatrix[F, A]
  )(op: GrBBinaryOp[A, A, Boolean])(implicit G:GRB): F[Boolean] = {

    def eqEither[T](s1: T, s2: T)(msg: String): EitherT[F, String, Boolean] =
      if (s1 == s2) EitherT.fromEither(Right(true))
      else EitherT.fromEither(Left(msg))

    def eqIntersect(
        expectedNVals: Long,
        r: Long,
        c: Long
    ): F[Either[String, Boolean]] = {
      (for {
        m <- GrBMatrix[F, Boolean](r, c)
        land <- GrBMonoid[F, Boolean](BuiltInBinaryOps.boolean.land, false)
      } yield (m, land)).use {
        case (m, land) =>
          val eF: EitherT[F, String, Boolean] = for {
            inter <- EitherT.right(
              ElemWise[F].intersect(m)(Left(op))(self, other)
            )
            nvals <- EitherT.right(inter.nvals)
            _ <- eqEither(expectedNVals, nvals)(
              s"different nvals after intersect $expectedNVals != $nvals"
            )
            check <- EitherT.right(inter.reduce(true, land))
            out <- eqEither(true, check)("different values")
          } yield out
          eF.value
      }
    }

    val eF = for {
      r1 <- EitherT.right(self.nrows)
      r2 <- EitherT.right(other.nrows)
      _ <- eqEither(r1, r2)(s"different rows $r1 != $r2")
      c1 <- EitherT.right(self.ncols)
      c2 <- EitherT.right(other.ncols)
      _ <- eqEither(c1, c2)(s"different cols $c1 != c2")
      v1 <- EitherT.right(self.nvals)
      v2 <- EitherT.right(other.nvals)
      _ <- eqEither(v1, v2)(s"different nvals $v1 != $v2")
      _ <- EitherT(eqIntersect(v1, r1, c1))
    } yield true

    eF.value.map {
      case Right(_) => true
      case Left(_)  => false
    }
  }
}
