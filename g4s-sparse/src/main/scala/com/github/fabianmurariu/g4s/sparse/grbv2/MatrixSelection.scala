package com.github.fabianmurariu.g4s.sparse.grbv2

import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import cats.effect.Sync
import com.github.fabianmurariu.g4s.sparse.grb.SparseMatrixHandler

class MatrixSelection[F[_], A](val mat:Matrix[F, A],
                               private[grbv2] val is:Array[Long],
                               private[grbv2] val ni:Long,
                               private[grbv2] val js:Array[Long],
                               private[grbv2] val nj:Long){ self =>


  def set[X](from: Matrix[F, A],
      mask: Option[Matrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  )(implicit S: Sync[F]): F[Matrix[F, A]] = {

    implicit val H: SparseMatrixHandler[A] = self.mat.H
    MatrixOps.assign(self)(from)(mask, accum, desc)
  }
}
