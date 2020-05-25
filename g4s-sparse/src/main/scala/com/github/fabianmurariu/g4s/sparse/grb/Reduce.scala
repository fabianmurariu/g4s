package com.github.fabianmurariu.g4s.sparse.grb
import com.github.fabianmurariu.unsafe.GRBALG
import scala.{specialized => sp}

trait Reduce[F[_], @sp(Boolean, Byte, Short, Int, Long, Float, Double) A] {
  @inline
  def reduceAll(f: F[A])(
      init: A,
      monoid: GrBMonoid[A],
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  ): A

  @inline
  def reduceM(f:F[A])(
      monoid: GrBMonoid[A],
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  )(implicit N:Numeric[A]) : A = {
    reduceAll(f)(N.zero, monoid, accum, desc)
  }
}

object Reduce {
  implicit val reduceMatrixBoolean: Reduce[GrBMatrix, Boolean] =
    new Reduce[GrBMatrix, Boolean] {

      override def reduceAll(f: GrBMatrix[Boolean])(
          init: Boolean,
          monoid: GrBMonoid[Boolean],
          accum: Option[GrBBinaryOp[Boolean, Boolean, Boolean]],
          desc: Option[GrBDescriptor]
      ): Boolean = {
        GRBALG.matrixReduceAllBoolean(init, accum.map(_.pointer).orNull, monoid.pointer, f.pointer, desc.map(_.pointer).orNull)
      }
    }

  implicit val reduceMatrixByte: Reduce[GrBMatrix, Byte] =
    new Reduce[GrBMatrix, Byte] {

      override def reduceAll(f: GrBMatrix[Byte])(
          init: Byte,
          monoid: GrBMonoid[Byte],
          accum: Option[GrBBinaryOp[Byte, Byte, Byte]],
          desc: Option[GrBDescriptor]
      ): Byte = {
        GRBALG.matrixReduceAllByte(init, accum.map(_.pointer).orNull, monoid.pointer, f.pointer, desc.map(_.pointer).orNull)
      }
    }

  implicit val reduceMatrixShort: Reduce[GrBMatrix, Short] =
    new Reduce[GrBMatrix, Short] {

      override def reduceAll(f: GrBMatrix[Short])(
          init: Short,
          monoid: GrBMonoid[Short],
          accum: Option[GrBBinaryOp[Short, Short, Short]],
          desc: Option[GrBDescriptor]
      ): Short = {
        GRBALG.matrixReduceAllShort(init, accum.map(_.pointer).orNull, monoid.pointer, f.pointer, desc.map(_.pointer).orNull)
      }
    }

  implicit val reduceMatrixInt: Reduce[GrBMatrix, Int] =
    new Reduce[GrBMatrix, Int] {

      override def reduceAll(f: GrBMatrix[Int])(
          init: Int,
          monoid: GrBMonoid[Int],
          accum: Option[GrBBinaryOp[Int, Int, Int]],
          desc: Option[GrBDescriptor]
      ): Int = {
        GRBALG.matrixReduceAllInt(init, accum.map(_.pointer).orNull, monoid.pointer, f.pointer, desc.map(_.pointer).orNull)
      }
    }

  implicit val reduceMatrixLong: Reduce[GrBMatrix, Long] =
    new Reduce[GrBMatrix, Long] {

      override def reduceAll(f: GrBMatrix[Long])(
          init: Long,
          monoid: GrBMonoid[Long],
          accum: Option[GrBBinaryOp[Long, Long, Long]],
          desc: Option[GrBDescriptor]
      ): Long = {
        GRBALG.matrixReduceAllLong(init, accum.map(_.pointer).orNull, monoid.pointer, f.pointer, desc.map(_.pointer).orNull)
      }
    }

  implicit val reduceMatrixFloat: Reduce[GrBMatrix, Float] =
    new Reduce[GrBMatrix, Float] {

      override def reduceAll(f: GrBMatrix[Float])(
          init: Float,
          monoid: GrBMonoid[Float],
          accum: Option[GrBBinaryOp[Float, Float, Float]],
          desc: Option[GrBDescriptor]
      ): Float = {
        GRBALG.matrixReduceAllFloat(init, accum.map(_.pointer).orNull, monoid.pointer, f.pointer, desc.map(_.pointer).orNull)
      }
    }

  implicit val reduceDouble: Reduce[GrBMatrix, Double] =
    new Reduce[GrBMatrix, Double] {

      override def reduceAll(f: GrBMatrix[Double])(
          init: Double,
          monoid: GrBMonoid[Double],
          accum: Option[GrBBinaryOp[Double, Double, Double]],
          desc: Option[GrBDescriptor]
      ): Double = {
        GRBALG.matrixReduceAllDouble(init, accum.map(_.pointer).orNull, monoid.pointer, f.pointer, desc.map(_.pointer).orNull)
      }
    }
}
