package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.g4s.sparse.grbv2.GrBVector
import cats.implicits._
import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import cats.effect.Sync

trait Diag[F[_]] {

  /**
    * set v onto the main diagonal of the m matrix
    * */
  def diag[A](
      mat: GrBMatrix[F, A]
  )(v: GrBVector[F, A])(implicit F: Sync[F], SMH:SparseMatrixHandler[A]): F[GrBMatrix[F, A]] =
    // TODO: replace with C version and eventually with GrB version
    for {
      tuples <- v.extract
      (is, vs) = tuples
      _ <- mat.pointer.map{ m =>
        SMH.setAll(m.ref)(is, is, vs)
      }
    } yield mat

}

object Diag{

  def apply[F[_]] = new Diag[F] {}
}
