package com.github.fabianmurariu.g4s.sparse.grbv2

import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import cats.effect.Sync
import cats.implicits._
import com.github.fabianmurariu.g4s.sparse.grb.GrBError
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import cats.Monad
import com.github.fabianmurariu.g4s.sparse.grb.SparseMatrixHandler

object MatrixOps extends TransposeOps

trait TransposeOps {

  def transpose[F[_], A, B, C, X](out: Matrix[F, A])(
      mask: Option[Matrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, B, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(in: Matrix[F, A])(implicit S: Sync[F], SMH: SparseMatrixHandler[C]): F[Matrix[F, C]] =
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
