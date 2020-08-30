package com.github.fabianmurariu.g4s.sparse.grbv2

import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import cats.effect.Sync
import cats.implicits._
import com.github.fabianmurariu.g4s.sparse.grb.GrBError
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import cats.Monad
import com.github.fabianmurariu.g4s.sparse.grb.SparseMatrixHandler

object MatrixOps extends TransposeOps with AssignOps

trait TransposeOps {

  def transpose[F[_], A, B, C, X](out: Matrix[F, A])(in: Matrix[F, A])(
      mask: Option[Matrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, B, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(implicit S: Sync[F], SMH: SparseMatrixHandler[C]): F[Matrix[F, C]] =
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
    } yield new Matrix[F, C] {

      override def pointer: F[Pointer] = S.pure(mpOut)

      override def F: Monad[F] = S

      override def H: SparseMatrixHandler[C] = SMH

    }
}

trait AssignOps {
  def assign[F[_], A, B, C, X](to: Matrix[F, A])(
      from: Matrix[F, A],
      is: GrBRange,
      js: GrBRange
  )(
      mask: Option[Matrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, B, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(implicit S: Sync[F], SMH: SparseMatrixHandler[C]): F[Matrix[F, C]] =
    for {
      mpIn <- from.pointer
      mpOut <- to.pointer
      grbMask <- mask.map(_.pointer.map(_.ref)).getOrElse(S.pure(null))
      _ <- S.delay {
        val (ni, grbI) = GrBRange.toGrB(is)
        val (nj, grbJ) = GrBRange.toGrB(js)
        GrBError.check(
          GRBOPSMAT.extract(
            mpOut.ref,
            grbMask,
            accum.map(_.pointer).orNull,
            mpIn.ref,
            grbI,
            ni,
            grbJ,
            nj,
            desc.map(_.pointer).orNull
          )
        )
      }
    } yield new Matrix[F, C] {

      override def pointer: F[Pointer] = S.pure(mpOut)

      override def F: Monad[F] = S

      override def H: SparseMatrixHandler[C] = SMH

    }

}
