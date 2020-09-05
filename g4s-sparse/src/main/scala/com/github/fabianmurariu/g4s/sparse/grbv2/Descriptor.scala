package com.github.fabianmurariu.g4s.sparse.grbv2

import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.g4s.sparse.grb.GrBDescriptor
import cats.Monad
import cats.effect.Sync
import cats.effect.Resource
import cats.implicits._
import com.github.fabianmurariu.g4s.sparse.grb.{grb, GrBError}

trait Descriptor[F[_]] {
  def pointer: F[GrBDescriptor]
  implicit def F: Monad[F]

  //FIXME: is this an error with GRB? setting a desciptor twice sets it to non?
  def set[K <: DescKey, V <: DescValue](
      implicit DP: DescPair[K, V]
  ): F[Unit] =
    pointer.map { p =>
      GrBError.check(
        GRBCORE.setDescriptorValue(p.pointer, DP.field.code, DP.value.code)
      )
    }

  def get[K <: DescKey, V <: DescValue](
      implicit DP: DescPair[K, V]
  ): F[Option[V]] = pointer.map { p =>
    GRBCORE.getDescriptorValue(p.pointer, DP.field.code) match {
      case -1 => None
      case n  =>
        Some(DP.value).filter(_.code == n)
    }
  }
}

object Descriptor {
  def apply[F[_]](implicit S: Sync[F]): Resource[F, Descriptor[F]] =
    Resource
      .fromAutoCloseable(S.delay {
        grb.GRB
        new GrBDescriptor(GRBCORE.createDescriptor())
      })
      .map { p =>
        new Descriptor[F] {
          override val pointer = F.pure(p)
          override implicit def F: Monad[F] = S
        }
      }

}

sealed abstract class DescKey(val code: Int)
case class Output() extends DescKey(GRBCORE.GrB_OUTP)
case class Mask() extends DescKey(GRBCORE.GrB_MASK)
case class Input0() extends DescKey(GRBCORE.GrB_INP0)
case class Input1() extends DescKey(GRBCORE.GrB_INP1)

sealed abstract class DescValue(val code: Int)

case class Default() extends DescValue(GRBCORE.GxB_DEFAULT)

case class Replace() extends DescValue(GRBCORE.GrB_REPLACE)

case class Complement() extends DescValue(GRBCORE.GrB_COMP)
case class Structure() extends DescValue(GRBCORE.GrB_STRUCTURE)

case class Transpose() extends DescValue(GRBCORE.GrB_TRAN)

case class DescPair[K, V] (field: K, value: V)

object DescPair {
  def defaultForAllKeys[K <: DescKey](k:K): DescPair[K, Default] =
    DescPair(k, Default())

  implicit val defaultForOutput = defaultForAllKeys(Output())
  implicit val defaultForInput0 = defaultForAllKeys(Input0())
  implicit val defaultForInput1 = defaultForAllKeys(Input1())
  implicit val defaultForMask = defaultForAllKeys(Mask())

  implicit val transposeForInput0: DescPair[Input0, Transpose] =
    DescPair(Input0(), Transpose())

  implicit val transposeForInput1: DescPair[Input1, Transpose] =
    DescPair(Input1(), Transpose())

  implicit val replaceForOutput: DescPair[Output, Replace] =
    DescPair(Output(), Replace())

  implicit val structureForMask: DescPair[Mask, Structure] =
    DescPair(Mask(), Structure())

  implicit val complementForMask: DescPair[Mask, Complement] =
    DescPair(Mask(), Complement())

}
