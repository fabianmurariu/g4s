package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.unsafe.GRAPHBLAS
import java.nio.Buffer

trait MatrixType[A] {
  def get: Buffer
}

object MatrixType {
  def apply[A](implicit MT: MatrixType[A]) = MT

  implicit val mtBoolean: MatrixType[Boolean] = new MatrixType[Boolean] {
    def get = GRAPHBLAS.booleanType()
  }
  implicit val mtByte: MatrixType[Byte] = new MatrixType[Byte] {
    def get = GRAPHBLAS.byteType()
  }
  implicit val mtShort: MatrixType[Short] = new MatrixType[Short] {
    def get = GRAPHBLAS.shortType()
  }
  implicit val mtInt: MatrixType[Int] = new MatrixType[Int] {
    def get = GRAPHBLAS.intType()
  }
  implicit val mtLong: MatrixType[Long] = new MatrixType[Long] {
    def get = GRAPHBLAS.longType()
  }
  implicit val mtFloat: MatrixType[Float] = new MatrixType[Float] {
    def get = GRAPHBLAS.floatType()
  }
  implicit val mtDouble: MatrixType[Double] = new MatrixType[Double] {
    def get = GRAPHBLAS.doubleType()
  }
}
