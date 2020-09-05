package com.github.fabianmurariu.g4s.sparse.grbv2

import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import cats.effect.Sync
import cats.implicits._
import com.github.fabianmurariu.g4s.sparse.grb.GrBError
import com.github.fabianmurariu.unsafe.GRBOPSMAT
import com.github.fabianmurariu.g4s.sparse.grb.SparseMatrixHandler

object MatrixOps extends TransposeOps with ExtractOps

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
    } yield new DefaultMatrix(S.pure(mpOut), S, SMH)
    
}

trait ExtractOps {
  // to = from(I, J)

  def extract[F[_], A, B, C, X](
      to: Matrix[F, A]
  )(from: MatrixSelection[F, A])(
      mask: Option[Matrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, B, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(implicit S: Sync[F], SMH: SparseMatrixHandler[C]): F[Matrix[F, C]] =
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
    } yield new DefaultMatrix(S.pure(mpOut), S, SMH)


  // to(I, J) = from
  def assign[F[_], A, B, C, X](
      to: MatrixSelection[F, A]
  )(from: Matrix[F, A])(
      mask: Option[Matrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, B, C]] = None,
      desc: Option[GrBDescriptor] = None
  )(implicit S: Sync[F], SMH: SparseMatrixHandler[C]): F[Matrix[F, C]] =
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
    } yield new DefaultMatrix(S.pure(mpOut), S, SMH)
   
}
