package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.unsafe.GRBALG
import scala.{specialized => sp}
import java.nio.Buffer

trait Reduce[@sp(Boolean, Byte, Short, Int, Long, Float, Double) A] {
  @inline
  def reduceAll(mat: Buffer)(
      init: A,
      monoid: GrBMonoid[A],
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  ): A

  @inline
  def reduceNumeric(mat: Buffer)(
      monoid: GrBMonoid[A],
      accum: Option[GrBBinaryOp[A, A, A]] = None,
      desc: Option[GrBDescriptor] = None
  )(implicit N: Numeric[A]): A = {
    reduceAll(mat)(N.zero, monoid, accum, desc)
  }
}

object Reduce {
  implicit val reduceMatrixBoolean: Reduce[Boolean] =
    new Reduce[Boolean] {

      override def reduceAll(mat: Buffer)(
          init: Boolean,
          monoid: GrBMonoid[Boolean],
          accum: Option[GrBBinaryOp[Boolean, Boolean, Boolean]],
          desc: Option[GrBDescriptor]
      ): Boolean = {
        GRBALG.matrixReduceAllBoolean(
          init,
          accum.map(_.pointer).orNull,
          monoid.pointer,
          mat,
          desc.map(_.pointer).orNull
        )
      }
    }

  implicit val reduceMatrixByte: Reduce[Byte] =
    new Reduce[Byte] {

      override def reduceAll(mat: Buffer)(
          init: Byte,
          monoid: GrBMonoid[Byte],
          accum: Option[GrBBinaryOp[Byte, Byte, Byte]],
          desc: Option[GrBDescriptor]
      ): Byte = {
        GRBALG.matrixReduceAllByte(
          init,
          accum.map(_.pointer).orNull,
          monoid.pointer,
          mat,
          desc.map(_.pointer).orNull
        )
      }
    }

  implicit val reduceMatrixShort: Reduce[Short] =
    new Reduce[Short] {

      override def reduceAll(mat: Buffer)(
          init: Short,
          monoid: GrBMonoid[Short],
          accum: Option[GrBBinaryOp[Short, Short, Short]],
          desc: Option[GrBDescriptor]
      ): Short = {
        GRBALG.matrixReduceAllShort(
          init,
          accum.map(_.pointer).orNull,
          monoid.pointer,
          mat,
          desc.map(_.pointer).orNull
        )
      }
    }

  implicit val reduceMatrixInt: Reduce[Int] =
    new Reduce[Int] {

      override def reduceAll(mat: Buffer)(
          init: Int,
          monoid: GrBMonoid[Int],
          accum: Option[GrBBinaryOp[Int, Int, Int]],
          desc: Option[GrBDescriptor]
      ): Int = {
        GRBALG.matrixReduceAllInt(
          init,
          accum.map(_.pointer).orNull,
          monoid.pointer,
          mat,
          desc.map(_.pointer).orNull
        )
      }
    }

  implicit val reduceMatrixLong: Reduce[Long] =
    new Reduce[Long] {

      override def reduceAll(mat: Buffer)(
          init: Long,
          monoid: GrBMonoid[Long],
          accum: Option[GrBBinaryOp[Long, Long, Long]],
          desc: Option[GrBDescriptor]
      ): Long = {
        GRBALG.matrixReduceAllLong(
          init,
          accum.map(_.pointer).orNull,
          monoid.pointer,
          mat,
          desc.map(_.pointer).orNull
        )
      }
    }

  implicit val reduceMatrixFloat: Reduce[Float] =
    new Reduce[Float] {

      override def reduceAll(mat: Buffer)(
          init: Float,
          monoid: GrBMonoid[Float],
          accum: Option[GrBBinaryOp[Float, Float, Float]],
          desc: Option[GrBDescriptor]
      ): Float = {
        GRBALG.matrixReduceAllFloat(
          init,
          accum.map(_.pointer).orNull,
          monoid.pointer,
          mat,
          desc.map(_.pointer).orNull
        )
      }
    }

  implicit val reduceDouble: Reduce[Double] =
    new Reduce[Double] {

      override def reduceAll(mat: Buffer)(
          init: Double,
          monoid: GrBMonoid[Double],
          accum: Option[GrBBinaryOp[Double, Double, Double]],
          desc: Option[GrBDescriptor]
      ): Double = {
        GRBALG.matrixReduceAllDouble(
          init,
          accum.map(_.pointer).orNull,
          monoid.pointer,
          mat,
          desc.map(_.pointer).orNull
        )
      }
    }
}
