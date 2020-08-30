package com.github.fabianmurariu.g4s.sparse.grbv2

import java.nio.Buffer

import scala.{specialized => sp}
import cats.effect.{Sync, Resource}
import com.github.fabianmurariu.g4s.sparse.grb.{grb, SparseVectorHandler}
import cats.implicits._

trait SprVector[F[_], @sp(Boolean, Byte, Short, Int, Long, Float, Double) A] { self =>
  def pointer: F[VectorPointer]
  implicit def F: Sync[F]
  def H: SparseVectorHandler[A]

  def extract:F[(Array[Long], Array[A])] = self.pointer.map {
    p => H.extract(p.ref)
  }
}

object SprVector {
  def apply[F[_], A](size: Long)(
      implicit M: Sync[F],
      SVH: SparseVectorHandler[A]
  ): Resource[F, SprVector[F, A]] = {
    Resource
      .fromAutoCloseable(
        M.delay {
          grb.GRB
          new VectorPointer(SVH.createVector(size))
        }
      )
      .map { mp =>
        new SprVector[F, A] {
          val pointer = M.pure(mp)
          override def F = M
          override def H = SVH
        }
      }
  }
}
