package com.github.fabianmurariu.g4s.sparse.grb

import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRAPHBLAS

/**
  * Basically a function
  * f(a, b) -> c
  *
  * used to create monoids
  * used for accumulators
 */
sealed trait GrBBinaryOp[A, B, C] {
  def pointer:Buffer
}
// some special ops can be found via implicits
// FIXME: pointer needs to be private
case class EqOp[A](val pointer: Buffer) extends GrBBinaryOp[A, A, Boolean]
case class GrBDefaultBinaryOp[A, B, C](val pointer: Buffer) extends GrBBinaryOp[A, B, C]

/**
  * Default BinaryOps (T, T) -> T
  */
abstract class BuiltInBoolBinaryOps[T] {
    def eq: GrBBinaryOp[T, T, Boolean]
    def ne: GrBBinaryOp[T, T, Boolean]
    def gt: GrBBinaryOp[T, T, Boolean]
    def lt: GrBBinaryOp[T, T, Boolean]
    def ge: GrBBinaryOp[T, T, Boolean]
    def le: GrBBinaryOp[T, T, Boolean]
}

abstract class BuiltInBinaryOps[T] extends BuiltInBoolBinaryOps[T] {

    def first: GrBBinaryOp[T, T, T]
    def second: GrBBinaryOp[T, T, T]
    def any: GrBBinaryOp[T, T, T]
    def pair: GrBBinaryOp[T, T, T]
    def min: GrBBinaryOp[T, T, T]
    def max: GrBBinaryOp[T, T, T]
    def plus: GrBBinaryOp[T, T, T]
    def minus: GrBBinaryOp[T, T, T]
    def rminus: GrBBinaryOp[T, T, T]
    def times: GrBBinaryOp[T, T, T]
    def div: GrBBinaryOp[T, T, T]
    def rdiv: GrBBinaryOp[T, T, T]
    def iseq: GrBBinaryOp[T, T, T]
    def isne: GrBBinaryOp[T, T, T]
    def isgt: GrBBinaryOp[T, T, T]
    def islt: GrBBinaryOp[T, T, T]
    def isge: GrBBinaryOp[T, T, T]
    def isle: GrBBinaryOp[T, T, T]
    def lor: GrBBinaryOp[T, T, T]
    def land: GrBBinaryOp[T, T, T]
    def lxor: GrBBinaryOp[T, T, T]

}



object GrBBinaryOp {

  // eq
  implicit def eqBoolean:EqOp[Boolean] = EqOp(GRAPHBLAS.eqBinaryOpBoolean())
  implicit def eqByte:EqOp[Byte] = EqOp(GRAPHBLAS.eqBinaryOpByte())
  implicit def eqShort:EqOp[Short] = EqOp(GRAPHBLAS.eqBinaryOpShort())
  implicit def eqInt:EqOp[Int] = EqOp(GRAPHBLAS.eqBinaryOpInt())
  implicit def eqLong:EqOp[Long] = EqOp(GRAPHBLAS.eqBinaryOpLong())
  implicit def eqFloat:EqOp[Float] = EqOp(GRAPHBLAS.eqBinaryOpFloat())
  implicit def eqDouble:EqOp[Double] = EqOp(GRAPHBLAS.eqBinaryOpDouble())

  def apply[A, B, C](pointer: Buffer): GrBBinaryOp[A, B, C] =
    GrBDefaultBinaryOp(pointer)
}

object BuiltInBinaryOps {

  implicit val boolean = new BuiltInBinaryOps[Boolean] {
    val first: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.firstBinaryOpBoolean())
    val second: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.secondBinaryOpBoolean())
    val any: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.anyBinaryOpBoolean())
    val pair: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.pairBinaryOpBoolean())
    val min: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.minBinaryOpBoolean())
    val max: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.maxBinaryOpBoolean())
    val plus: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.plusBinaryOpBoolean())
    val minus: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.minusBinaryOpBoolean())
    val rminus: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.rminusBinaryOpBoolean())
    val times: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.timesBinaryOpBoolean())
    val div: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.divBinaryOpBoolean())
    val rdiv: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.rdivBinaryOpBoolean())
    val iseq: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.iseqBinaryOpBoolean())
    val isne: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.isneBinaryOpBoolean())
    val isgt: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.isgtBinaryOpBoolean())
    val islt: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.isltBinaryOpBoolean())
    val isge: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.isgeBinaryOpBoolean())
    val isle: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.isleBinaryOpBoolean())
    val lor: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.lorBinaryOpBoolean())
    val land: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.landBinaryOpBoolean())
    val lxor: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.lxorBinaryOpBoolean())
    val eq: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.eqBinaryOpBoolean())
    val ne: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.neBinaryOpBoolean())
    val gt: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.gtBinaryOpBoolean())
    val lt: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.ltBinaryOpBoolean())
    val ge: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.geBinaryOpBoolean())
    val le: GrBBinaryOp[Boolean, Boolean, Boolean] =
      GrBBinaryOp(GRAPHBLAS.leBinaryOpBoolean())
  }

  implicit val double = new BuiltInBinaryOps[Double] {
    val first: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.firstBinaryOpDouble())
    val second: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.secondBinaryOpDouble())
    val any: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.anyBinaryOpDouble())
    val pair: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.pairBinaryOpDouble())
    val min: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.minBinaryOpDouble())
    val max: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.maxBinaryOpDouble())
    val plus: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.plusBinaryOpDouble())
    val minus: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.minusBinaryOpDouble())
    val rminus: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.rminusBinaryOpDouble())
    val times: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.timesBinaryOpDouble())
    val div: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.divBinaryOpDouble())
    val rdiv: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.rdivBinaryOpDouble())
    val iseq: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.iseqBinaryOpDouble())
    val isne: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.isneBinaryOpDouble())
    val isgt: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.isgtBinaryOpDouble())
    val islt: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.isltBinaryOpDouble())
    val isge: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.isgeBinaryOpDouble())
    val isle: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.isleBinaryOpDouble())
    val lor: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.lorBinaryOpDouble())
    val land: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.landBinaryOpDouble())
    val lxor: GrBBinaryOp[Double, Double, Double] =
      GrBBinaryOp(GRAPHBLAS.lxorBinaryOpDouble())
    val eq: GrBBinaryOp[Double, Double, Boolean] =
      GrBBinaryOp(GRAPHBLAS.eqBinaryOpDouble())
    val ne: GrBBinaryOp[Double, Double, Boolean] =
      GrBBinaryOp(GRAPHBLAS.neBinaryOpDouble())
    val gt: GrBBinaryOp[Double, Double, Boolean] =
      GrBBinaryOp(GRAPHBLAS.gtBinaryOpDouble())
    val lt: GrBBinaryOp[Double, Double, Boolean] =
      GrBBinaryOp(GRAPHBLAS.ltBinaryOpDouble())
    val ge: GrBBinaryOp[Double, Double, Boolean] =
      GrBBinaryOp(GRAPHBLAS.geBinaryOpDouble())
    val le: GrBBinaryOp[Double, Double, Boolean] =
      GrBBinaryOp(GRAPHBLAS.leBinaryOpDouble())
  }

  implicit val short = new BuiltInBinaryOps[Short] {
    val first: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.firstBinaryOpShort())
    val second: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.secondBinaryOpShort())
    val any: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.anyBinaryOpShort())
    val pair: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.pairBinaryOpShort())
    val min: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.minBinaryOpShort())
    val max: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.maxBinaryOpShort())
    val plus: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.plusBinaryOpShort())
    val minus: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.minusBinaryOpShort())
    val rminus: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.rminusBinaryOpShort())
    val times: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.timesBinaryOpShort())
    val div: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.divBinaryOpShort())
    val rdiv: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.rdivBinaryOpShort())
    val iseq: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.iseqBinaryOpShort())
    val isne: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.isneBinaryOpShort())
    val isgt: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.isgtBinaryOpShort())
    val islt: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.isltBinaryOpShort())
    val isge: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.isgeBinaryOpShort())
    val isle: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.isleBinaryOpShort())
    val lor: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.lorBinaryOpShort())
    val land: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.landBinaryOpShort())
    val lxor: GrBBinaryOp[Short, Short, Short] =
      GrBBinaryOp(GRAPHBLAS.lxorBinaryOpShort())
    val eq: GrBBinaryOp[Short, Short, Boolean] =
      GrBBinaryOp(GRAPHBLAS.eqBinaryOpShort())
    val ne: GrBBinaryOp[Short, Short, Boolean] =
      GrBBinaryOp(GRAPHBLAS.neBinaryOpShort())
    val gt: GrBBinaryOp[Short, Short, Boolean] =
      GrBBinaryOp(GRAPHBLAS.gtBinaryOpShort())
    val lt: GrBBinaryOp[Short, Short, Boolean] =
      GrBBinaryOp(GRAPHBLAS.ltBinaryOpShort())
    val ge: GrBBinaryOp[Short, Short, Boolean] =
      GrBBinaryOp(GRAPHBLAS.geBinaryOpShort())
    val le: GrBBinaryOp[Short, Short, Boolean] =
      GrBBinaryOp(GRAPHBLAS.leBinaryOpShort())
  }

  implicit val long = new BuiltInBinaryOps[Long] {
    val first: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.firstBinaryOpLong())
    val second: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.secondBinaryOpLong())
    val any: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.anyBinaryOpLong())
    val pair: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.pairBinaryOpLong())
    val min: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.minBinaryOpLong())
    val max: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.maxBinaryOpLong())
    val plus: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.plusBinaryOpLong())
    val minus: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.minusBinaryOpLong())
    val rminus: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.rminusBinaryOpLong())
    val times: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.timesBinaryOpLong())
    val div: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.divBinaryOpLong())
    val rdiv: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.rdivBinaryOpLong())
    val iseq: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.iseqBinaryOpLong())
    val isne: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.isneBinaryOpLong())
    val isgt: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.isgtBinaryOpLong())
    val islt: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.isltBinaryOpLong())
    val isge: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.isgeBinaryOpLong())
    val isle: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.isleBinaryOpLong())
    val lor: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.lorBinaryOpLong())
    val land: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.landBinaryOpLong())
    val lxor: GrBBinaryOp[Long, Long, Long] =
      GrBBinaryOp(GRAPHBLAS.lxorBinaryOpLong())
    val eq: GrBBinaryOp[Long, Long, Boolean] =
      GrBBinaryOp(GRAPHBLAS.eqBinaryOpLong())
    val ne: GrBBinaryOp[Long, Long, Boolean] =
      GrBBinaryOp(GRAPHBLAS.neBinaryOpLong())
    val gt: GrBBinaryOp[Long, Long, Boolean] =
      GrBBinaryOp(GRAPHBLAS.gtBinaryOpLong())
    val lt: GrBBinaryOp[Long, Long, Boolean] =
      GrBBinaryOp(GRAPHBLAS.ltBinaryOpLong())
    val ge: GrBBinaryOp[Long, Long, Boolean] =
      GrBBinaryOp(GRAPHBLAS.geBinaryOpLong())
    val le: GrBBinaryOp[Long, Long, Boolean] =
      GrBBinaryOp(GRAPHBLAS.leBinaryOpLong())
  }

  implicit val float = new BuiltInBinaryOps[Float] {
    val first: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.firstBinaryOpFloat())
    val second: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.secondBinaryOpFloat())
    val any: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.anyBinaryOpFloat())
    val pair: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.pairBinaryOpFloat())
    val min: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.minBinaryOpFloat())
    val max: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.maxBinaryOpFloat())
    val plus: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.plusBinaryOpFloat())
    val minus: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.minusBinaryOpFloat())
    val rminus: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.rminusBinaryOpFloat())
    val times: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.timesBinaryOpFloat())
    val div: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.divBinaryOpFloat())
    val rdiv: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.rdivBinaryOpFloat())
    val iseq: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.iseqBinaryOpFloat())
    val isne: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.isneBinaryOpFloat())
    val lor: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.lorBinaryOpFloat())
    val isgt: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.isgtBinaryOpFloat())
    val islt: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.isltBinaryOpFloat())
    val isge: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.isgeBinaryOpFloat())
    val isle: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.isleBinaryOpFloat())
    val land: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.landBinaryOpFloat())
    val lxor: GrBBinaryOp[Float, Float, Float] =
      GrBBinaryOp(GRAPHBLAS.lxorBinaryOpFloat())
    val eq: GrBBinaryOp[Float, Float, Boolean] =
      GrBBinaryOp(GRAPHBLAS.eqBinaryOpFloat())
    val ne: GrBBinaryOp[Float, Float, Boolean] =
      GrBBinaryOp(GRAPHBLAS.neBinaryOpFloat())
    val gt: GrBBinaryOp[Float, Float, Boolean] =
      GrBBinaryOp(GRAPHBLAS.gtBinaryOpFloat())
    val lt: GrBBinaryOp[Float, Float, Boolean] =
      GrBBinaryOp(GRAPHBLAS.ltBinaryOpFloat())
    val ge: GrBBinaryOp[Float, Float, Boolean] =
      GrBBinaryOp(GRAPHBLAS.geBinaryOpFloat())
    val le: GrBBinaryOp[Float, Float, Boolean] =
      GrBBinaryOp(GRAPHBLAS.leBinaryOpFloat())
  }

  implicit val int = new BuiltInBinaryOps[Int] {
    val first: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.firstBinaryOpInt())
    val second: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.secondBinaryOpInt())
    val any: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.anyBinaryOpInt())
    val pair: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.pairBinaryOpInt())
    val min: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.minBinaryOpInt())
    val max: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.maxBinaryOpInt())
    val plus: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.plusBinaryOpInt())
    val minus: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.minusBinaryOpInt())
    val rminus: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.rminusBinaryOpInt())
    val times: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.timesBinaryOpInt())
    val div: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.divBinaryOpInt())
    val rdiv: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.rdivBinaryOpInt())
    val iseq: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.iseqBinaryOpInt())
    val isne: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.isneBinaryOpInt())
    val isgt: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.isgtBinaryOpInt())
    val islt: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.isltBinaryOpInt())
    val isge: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.isgeBinaryOpInt())
    val isle: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.isleBinaryOpInt())
    val lor: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.lorBinaryOpInt())
    val land: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.landBinaryOpInt())
    val lxor: GrBBinaryOp[Int, Int, Int] =
      GrBBinaryOp(GRAPHBLAS.lxorBinaryOpInt())
    val eq: GrBBinaryOp[Int, Int, Boolean] =
      GrBBinaryOp(GRAPHBLAS.eqBinaryOpInt())
    val ne: GrBBinaryOp[Int, Int, Boolean] =
      GrBBinaryOp(GRAPHBLAS.neBinaryOpInt())
    val gt: GrBBinaryOp[Int, Int, Boolean] =
      GrBBinaryOp(GRAPHBLAS.gtBinaryOpInt())
    val lt: GrBBinaryOp[Int, Int, Boolean] =
      GrBBinaryOp(GRAPHBLAS.ltBinaryOpInt())
    val ge: GrBBinaryOp[Int, Int, Boolean] =
      GrBBinaryOp(GRAPHBLAS.geBinaryOpInt())
    val le: GrBBinaryOp[Int, Int, Boolean] =
      GrBBinaryOp(GRAPHBLAS.leBinaryOpInt())
  }

  implicit val byte = new BuiltInBinaryOps[Byte] {
    val any: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.anyBinaryOpByte())
    val first: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.firstBinaryOpByte())
    val second: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.secondBinaryOpByte())
    val pair: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.pairBinaryOpByte())
    val min: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.minBinaryOpByte())
    val max: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.maxBinaryOpByte())
    val plus: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.plusBinaryOpByte())
    val minus: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.minusBinaryOpByte())
    val rminus: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.rminusBinaryOpByte())
    val times: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.timesBinaryOpByte())
    val div: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.divBinaryOpByte())
    val rdiv: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.rdivBinaryOpByte())
    val iseq: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.iseqBinaryOpByte())
    val isne: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.isneBinaryOpByte())
    val isgt: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.isgtBinaryOpByte())
    val islt: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.isltBinaryOpByte())
    val isge: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.isgeBinaryOpByte())
    val isle: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.isleBinaryOpByte())
    val lor: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.lorBinaryOpByte())
    val land: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.landBinaryOpByte())
    val lxor: GrBBinaryOp[Byte, Byte, Byte] =
      GrBBinaryOp(GRAPHBLAS.lxorBinaryOpByte())
    val eq: GrBBinaryOp[Byte, Byte, Boolean] =
      GrBBinaryOp(GRAPHBLAS.eqBinaryOpByte())
    val ne: GrBBinaryOp[Byte, Byte, Boolean] =
      GrBBinaryOp(GRAPHBLAS.neBinaryOpByte())
    val gt: GrBBinaryOp[Byte, Byte, Boolean] =
      GrBBinaryOp(GRAPHBLAS.gtBinaryOpByte())
    val lt: GrBBinaryOp[Byte, Byte, Boolean] =
      GrBBinaryOp(GRAPHBLAS.ltBinaryOpByte())
    val ge: GrBBinaryOp[Byte, Byte, Boolean] =
      GrBBinaryOp(GRAPHBLAS.geBinaryOpByte())
    val le: GrBBinaryOp[Byte, Byte, Boolean] =
      GrBBinaryOp(GRAPHBLAS.leBinaryOpByte())
  }

}
