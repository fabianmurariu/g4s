package com.github.fabianmurariu.g4s.sparse.grb
import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRBCORE
import scala.{specialized => sp}

trait MatrixBuilder[A] {
  def build(rows: Long, cols: Long): Buffer
}

object MatrixBuilder {
  def apply[A](implicit MB: MatrixBuilder[A]) = MB

  implicit def matrixBuilder[A: MatrixType]: MatrixBuilder[A] =
    new MatrixBuilder[A] {
      override def build(rows: Long, cols: Long): Buffer = {
        grb.GRB // init the GRB subsystem
        GRBCORE.createMatrix(MatrixType[A].get, rows, cols)
      }
    }
}

