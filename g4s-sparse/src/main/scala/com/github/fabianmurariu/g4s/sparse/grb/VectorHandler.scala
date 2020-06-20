package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.unsafe.GRAPHBLAS
import scala.{specialized => sp}
import com.github.fabianmurariu.unsafe.GRBCORE

trait VectorHandler[V[_], @sp(Boolean, Byte, Short, Int, Long, Float, Double) A] {
  @inline
  def set(m: V[A])(i: Long, x: A): Unit
  @inline
  def get(m: V[A])(i: Long): Option[A]

  /**
    *
    * Copies the tuples in the Vector and exports them in 3 arrays
    * (values, I indices, J indices)
    *
    */
  @inline
  def copyData(m: V[A]): (Array[A], Array[Long])
}

object VectorHandler {

  def apply[V[_], A](getFn: (V[A], Long) => Option[A])(
      setFn: (V[A], Long, A) => Unit
  )(copyFb: V[A] => (Array[A], Array[Long])): VectorHandler[V, A] =
    new VectorHandler[V, A] {

      override def copyData(m: V[A]): (Array[A], Array[Long]) =
        copyFb(m)

      override def set(f: V[A])(i: Long, x: A): Unit =
        setFn(f, i, x)

      override def get(f: V[A])(i: Long): Option[A] = getFn(f, i)

    }

  // FIXME: handle errors on calling extractVectorTuples*

  implicit val vectorHandlerBoolean: VectorHandler[GrBVector, Boolean] =
    VectorHandler[GrBVector, Boolean] { (f, i) =>
      GRAPHBLAS.getVectorElementBoolean(f.pointer, i).headOption
    } { (f, i, x) => GRAPHBLAS.setVectorElementBoolean(f.pointer, i, x) } {
      (f) =>
        val n = GRBCORE.nvalsVector(f.pointer)
        val vs = new Array[Boolean](n.toInt)
        val is = new Array[Long](n.toInt)
        GRAPHBLAS.extractVectorTuplesBoolean(f.pointer, vs, is)
        (vs, is)
    }

  implicit val vectorHandlerByte: VectorHandler[GrBVector, Byte] =
    VectorHandler[GrBVector, Byte] { (f, i) =>
      GRAPHBLAS.getVectorElementByte(f.pointer, i).headOption
    } { (f, i, x) => GRAPHBLAS.setVectorElementByte(f.pointer, i, x) } {
      (f) =>
        val n = GRBCORE.nvalsVector(f.pointer)
        val vs = new Array[Byte](n.toInt)
        val is = new Array[Long](n.toInt)
        GRAPHBLAS.extractVectorTuplesByte(f.pointer, vs, is)
        (vs, is)
    }

  implicit val vectorHandlerShort: VectorHandler[GrBVector, Short] =
    VectorHandler[GrBVector, Short] { (f, i) =>
      GRAPHBLAS.getVectorElementShort(f.pointer, i).headOption
    } { (f, i, x) => GRAPHBLAS.setVectorElementShort(f.pointer, i, x) } {
      (f) =>
        val n = GRBCORE.nvalsVector(f.pointer)
        val vs = new Array[Short](n.toInt)
        val is = new Array[Long](n.toInt)
        GRAPHBLAS.extractVectorTuplesShort(f.pointer, vs, is)
        (vs, is)
    }

  implicit val vectorHandlerInt: VectorHandler[GrBVector, Int] =
    VectorHandler[GrBVector, Int] { (f, i) =>
      GRAPHBLAS.getVectorElementInt(f.pointer, i).headOption
    } { (f, i, x) => GRAPHBLAS.setVectorElementInt(f.pointer, i, x) } {
      (f) =>
        val n = GRBCORE.nvalsVector(f.pointer)
        val vs = new Array[Int](n.toInt)
        val is = new Array[Long](n.toInt)
        GRAPHBLAS.extractVectorTuplesInt(f.pointer, vs, is)
        (vs, is)
    }

  implicit val vectorHandlerLong: VectorHandler[GrBVector, Long] =
    VectorHandler[GrBVector, Long] { (f, i) =>
      GRAPHBLAS.getVectorElementLong(f.pointer, i).headOption
    } { (f, i, x) => GRAPHBLAS.setVectorElementLong(f.pointer, i, x) } {
      (f) =>
        val n = GRBCORE.nvalsVector(f.pointer)
        val vs = new Array[Long](n.toInt)
        val is = new Array[Long](n.toInt)
        GRAPHBLAS.extractVectorTuplesLong(f.pointer, vs, is)
        (vs, is)
    }

  implicit val vectorHandlerFloat: VectorHandler[GrBVector, Float] =
    VectorHandler[GrBVector, Float] { (f, i) =>
      GRAPHBLAS.getVectorElementFloat(f.pointer, i).headOption
    } { (f, i, x) => GRAPHBLAS.setVectorElementFloat(f.pointer, i, x) } {
      (f) =>
        val n = GRBCORE.nvalsVector(f.pointer)
        val vs = new Array[Float](n.toInt)
        val is = new Array[Long](n.toInt)
        GRAPHBLAS.extractVectorTuplesFloat(f.pointer, vs, is)
        (vs, is)
    }

  implicit val vectorHandlerDouble: VectorHandler[GrBVector, Double] =
    VectorHandler[GrBVector, Double] { (f, i) =>
      GRAPHBLAS.getVectorElementDouble(f.pointer, i).headOption
    } { (f, i, x) => GRAPHBLAS.setVectorElementDouble(f.pointer, i, x) } {
      (f) =>
        val n = GRBCORE.nvalsVector(f.pointer)
        val vs = new Array[Double](n.toInt)
        val is = new Array[Long](n.toInt)
        GRAPHBLAS.extractVectorTuplesDouble(f.pointer, vs, is)
        (vs, is)
    }

}
