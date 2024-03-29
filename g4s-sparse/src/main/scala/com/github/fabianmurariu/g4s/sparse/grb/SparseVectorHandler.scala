package com.github.fabianmurariu.g4s.sparse.grb

import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.unsafe.GRAPHBLAS

trait SparseVectorHandler[
    @specialized(Boolean, Byte, Short, Int, Long, Float, Double) T
] {
  def createVector(size: Long): Buffer

  def createVectorFromTuples(size: Long)(is: Array[Long], vs: Array[T]): Buffer

  def get(vec: Buffer)(i: Long): Array[T]

  def set(vec: Buffer)(i: Long, v: T): Long

  def setAll(vec: Buffer)(is: Array[Long], ts: Array[T]): Unit = {
    assert(is.length == ts.length)
    var i = 0
    while (i < is.length) {
      set(vec)(is(i), ts(i))
      i += 1
    }
  }

  def extract(vec: Buffer): (Array[Long], Array[T])

  def remove(vec: Buffer)(i: Long): Long =
    GRBCORE.removeElementVector(vec, i)

}

object SparseVectorHandler {
  def apply[T](implicit S: SparseVectorHandler[T]): SparseVectorHandler[T] = S

  implicit val booleanVectorHandler: SparseVectorHandler[Boolean] =
    new SparseVectorHandler[Boolean] {

      override def extract(vec: Buffer): (Array[Long], Array[Boolean]) = {
        val nvals = GRBCORE.nvalsVector(vec)
        val is = new Array[Long](nvals.toInt)
        val vs = new Array[Boolean](nvals.toInt)
        GRAPHBLAS.extractVectorTuplesBoolean(vec, vs, is)
        (is, vs)
      }

      override def createVectorFromTuples(
          size: Long
      )(is: Array[Long], vs: Array[Boolean]): Buffer = {
        val vec = createVector(size)
        GRAPHBLAS.buildVectorFromTuplesBoolean(
          vec,
          is,
          vs,
          is.length.toLong,
          GRAPHBLAS.firstBinaryOpBoolean()
        )
        vec
      }

      def createVector(size: Long): Buffer =
        GRBCORE.createVector(GRAPHBLAS.booleanType(), size)

      def get(vec: Buffer)(i: Long): Array[Boolean] =
        GRAPHBLAS.getVectorElementBoolean(vec, i)

      def set(vec: Buffer)(i: Long, t: Boolean): Long =
        GRAPHBLAS.setVectorElementBoolean(vec, i, t)
    }

  implicit val byteVectorHandler: SparseVectorHandler[Byte] =
    new SparseVectorHandler[Byte] {
      override def extract(vec: Buffer): (Array[Long], Array[Byte]) = {
        val nvals = GRBCORE.nvalsVector(vec)
        val is = new Array[Long](nvals.toInt)
        val vs = new Array[Byte](nvals.toInt)
        GRAPHBLAS.extractVectorTuplesByte(vec, vs, is)
        (is, vs)
      }

      override def createVectorFromTuples(
          size: Long
      )(is: Array[Long], vs: Array[Byte]): Buffer = {
        val vec = createVector(size)
        GRAPHBLAS.buildVectorFromTuplesByte(
          vec,
          is,
          vs,
          is.length.toLong,
          GRAPHBLAS.firstBinaryOpByte()
        )
        vec
      }

      def createVector(size: Long): Buffer =
        GRBCORE.createVector(GRAPHBLAS.byteType(), size)

      def get(vec: Buffer)(i: Long): Array[Byte] =
        GRAPHBLAS.getVectorElementByte(vec, i)

      def set(vec: Buffer)(i: Long, t: Byte): Long =
        GRAPHBLAS.setVectorElementByte(vec, i, t)
    }

  implicit val shortVectorHandler: SparseVectorHandler[Short] =
    new SparseVectorHandler[Short] {
      override def extract(vec: Buffer): (Array[Long], Array[Short]) = {
        val nvals = GRBCORE.nvalsVector(vec)
        val is = new Array[Long](nvals.toInt)
        val vs = new Array[Short](nvals.toInt)
        GRAPHBLAS.extractVectorTuplesShort(vec, vs, is)
        (is, vs)
      }
      override def createVectorFromTuples(
          size: Long
      )(is: Array[Long], vs: Array[Short]): Buffer = {
        val vec = createVector(size)
        GRAPHBLAS.buildVectorFromTuplesShort(
          vec,
          is,
          vs,
          is.length.toLong,
          GRAPHBLAS.firstBinaryOpShort()
        )
        vec
      }
      def createVector(size: Long): Buffer =
        GRBCORE.createVector(GRAPHBLAS.shortType(), size)

      def get(vec: Buffer)(i: Long): Array[Short] =
        GRAPHBLAS.getVectorElementShort(vec, i)

      def set(vec: Buffer)(i: Long, t: Short): Long =
        GRAPHBLAS.setVectorElementShort(vec, i, t)
    }

  implicit val intVectorHandler: SparseVectorHandler[Int] =
    new SparseVectorHandler[Int] {
      override def extract(vec: Buffer): (Array[Long], Array[Int]) = {
        val nvals = GRBCORE.nvalsVector(vec)
        val is = new Array[Long](nvals.toInt)
        val vs = new Array[Int](nvals.toInt)
        GRAPHBLAS.extractVectorTuplesInt(vec, vs, is)
        (is, vs)
      }
      override def createVectorFromTuples(
          size: Long
      )(is: Array[Long], vs: Array[Int]): Buffer = {
        val vec = createVector(size)
        GRAPHBLAS.buildVectorFromTuplesInt(
          vec,
          is,
          vs,
          is.length.toLong,
          GRAPHBLAS.firstBinaryOpInt()
        )
        vec
      }
      def createVector(size: Long): Buffer =
        GRBCORE.createVector(GRAPHBLAS.intType(), size)

      def get(vec: Buffer)(i: Long): Array[Int] =
        GRAPHBLAS.getVectorElementInt(vec, i)

      def set(vec: Buffer)(i: Long, t: Int): Long =
        GRAPHBLAS.setVectorElementInt(vec, i, t)
    }

  implicit val longVectorHandler: SparseVectorHandler[Long] =
    new SparseVectorHandler[Long] {

      override def extract(vec: Buffer): (Array[Long], Array[Long]) = {
        val nvals = GRBCORE.nvalsVector(vec)
        val is = new Array[Long](nvals.toInt)
        val vs = new Array[Long](nvals.toInt)
        GRAPHBLAS.extractVectorTuplesLong(vec, vs, is)
        (is, vs)
      }
      override def createVectorFromTuples(
          size: Long
      )(is: Array[Long], vs: Array[Long]): Buffer = {
        val vec = createVector(size)
        GRAPHBLAS.buildVectorFromTuplesLong(
          vec,
          is,
          vs,
          is.length.toLong,
          GRAPHBLAS.firstBinaryOpLong()
        )
        vec
      }

      def createVector(size: Long): Buffer =
        GRBCORE.createVector(GRAPHBLAS.longType(), size)

      def get(vec: Buffer)(i: Long): Array[Long] =
        GRAPHBLAS.getVectorElementLong(vec, i)

      def set(vec: Buffer)(i: Long, t: Long): Long =
        GRAPHBLAS.setVectorElementLong(vec, i, t)
    }

  implicit val floatVectorHandler: SparseVectorHandler[Float] =
    new SparseVectorHandler[Float] {
      override def extract(vec: Buffer): (Array[Long], Array[Float]) = {
        val nvals = GRBCORE.nvalsVector(vec)
        val is = new Array[Long](nvals.toInt)
        val vs = new Array[Float](nvals.toInt)
        GRAPHBLAS.extractVectorTuplesFloat(vec, vs, is)
        (is, vs)
      }

      override def createVectorFromTuples(
          size: Long
      )(is: Array[Long], vs: Array[Float]): Buffer = {
        val vec = createVector(size)
        GRAPHBLAS.buildVectorFromTuplesFloat(
          vec,
          is,
          vs,
          is.length.toLong,
          GRAPHBLAS.firstBinaryOpFloat()
        )
        vec
      }

      def createVector(size: Long): Buffer =
        GRBCORE.createVector(GRAPHBLAS.floatType(), size)

      def get(vec: Buffer)(i: Long): Array[Float] =
        GRAPHBLAS.getVectorElementFloat(vec, i)

      def set(vec: Buffer)(i: Long, t: Float): Long =
        GRAPHBLAS.setVectorElementFloat(vec, i, t)
    }

  implicit val doubleVectorHandler: SparseVectorHandler[Double] =
    new SparseVectorHandler[Double] {
      override def extract(vec: Buffer): (Array[Long], Array[Double]) = {
        val nvals = GRBCORE.nvalsVector(vec)
        val is = new Array[Long](nvals.toInt)
        val vs = new Array[Double](nvals.toInt)
        GRAPHBLAS.extractVectorTuplesDouble(vec, vs, is)
        (is, vs)
      }

      override def createVectorFromTuples(
          size: Long
      )(is: Array[Long], vs: Array[Double]): Buffer = {
        val vec = createVector(size)
        GRAPHBLAS.buildVectorFromTuplesDouble(
          vec,
          is,
          vs,
          is.length.toLong,
          GRAPHBLAS.firstBinaryOpDouble()
        )
        vec
      }

      def createVector(size: Long): Buffer =
        GRBCORE.createVector(GRAPHBLAS.doubleType(), size)

      def get(vec: Buffer)(i: Long): Array[Double] =
        GRAPHBLAS.getVectorElementDouble(vec, i)

      def set(vec: Buffer)(i: Long, t: Double): Long =
        GRAPHBLAS.setVectorElementDouble(vec, i, t)
    }
}
