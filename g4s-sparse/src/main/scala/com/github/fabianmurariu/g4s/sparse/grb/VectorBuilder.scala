package com.github.fabianmurariu.g4s.sparse.grb
import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRBCORE
import scala.{specialized => sp}

trait VectorBuilder[A] {
  def build(size:Long): Buffer
}

object VectorBuilder {
  def apply[A](implicit MB: VectorBuilder[A]) = MB

  implicit def matrixBuilder[A: MatrixType]: VectorBuilder[A] =
    new VectorBuilder[A] {
      override def build(size:Long): Buffer = {
        GRBCORE.createVector(MatrixType[A].get, size)
      }
    }
}
