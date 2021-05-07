package com.github.fabianmurariu.g4s.sparse.grbv2

import cats.implicits._
import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import cats.effect.Sync
import com.github.fabianmurariu.g4s.sparse.grb.SparseMatrixHandler
import java.util.Arrays
import scala.reflect.ClassTag

class MatrixSelection[F[_]: Sync, A: SparseMatrixHandler: ClassTag](
    val mat: GrBMatrix[F, A],
    private[grbv2] val is: Array[Long],
    private[grbv2] val ni: Long,
    private[grbv2] val js: Array[Long],
    private[grbv2] val nj: Long
) { self =>

  def show(): F[String] = Sync[F].delay {
    s"""{is: ${Arrays.toString(is)}, ni:$ni, js: ${Arrays
      .toString(js)}, nj:$nj}"""
  }

  def set[X](
      from: GrBMatrix[F, A],
      mask: Option[GrBMatrix[F, X]] = None,
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  ): F[GrBMatrix[F, A]] = {

    MatrixOps
      .assign(self)(from.pointer)(mask, accum, desc)
      .map(mp => from.liftPointer(mp))
  }
}
