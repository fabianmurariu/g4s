package com.github.fabianmurariu.g4s.sparse.grbv2

import scala.{specialized => sp}
import cats.effect.{Sync, Resource}
import com.github.fabianmurariu.g4s.sparse.grb.{SparseVectorHandler, GRB}
import cats.implicits._

trait GrBVector[F[_], @sp(Boolean, Byte, Short, Int, Long, Float, Double) A] {
  self =>
  def pointer: F[VectorPointer]
  implicit def F: Sync[F]
  def H: SparseVectorHandler[A]

  def extract: F[(Array[Long], Array[A])] = self.pointer.map { p =>
    H.extract(p.ref)
  }
}

object GrBVector {
  def apply[F[_], A](size: Long)(
      implicit M: Sync[F],
      SVH: SparseVectorHandler[A],
      G:GRB
  ): Resource[F, GrBVector[F, A]] = {
    Resource
      .fromAutoCloseable(M.delay(new VectorPointer(SVH.createVector(size))))
      .map { mp =>
        new GrBVector[F, A] {
          val pointer = M.pure(mp)
          override def F = M
          override def H = SVH
        }
      }
  }
}
