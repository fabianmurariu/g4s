package com.github.fabianmurariu.g4s.sparse.grb
import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRBMONOID
import com.github.fabianmurariu.unsafe.GRBCORE
import cats.effect.Resource
import cats.effect.Sync

final class GrBMonoid[T](private[grb] val pointer: Buffer, zero: T) extends AutoCloseable {

  override def close(): Unit = {
    GRBCORE.freeMonoid(pointer)
  }


}

object GrBMonoid {

  def apply[F[_]:Sync, T: MonoidBuilder](op:GrBBinaryOp[T, T, T], zero: T) =
    Resource.fromAutoCloseable(
      Sync[F].delay{
        grb.GRB
        new GrBMonoid(MonoidBuilder[T].createMonoid(op.pointer, zero), zero)
}
    )
}

trait MonoidBuilder[T] {
  def createMonoid(opPointer:Buffer, zero: T): Buffer
}

object MonoidBuilder {
  def apply[T](implicit M: MonoidBuilder[T]) = M

  implicit val monoidBuilderBoolean:MonoidBuilder[Boolean] = new MonoidBuilder[Boolean] {
    override def createMonoid(opPointer:Buffer, zero: Boolean): Buffer =
      GRBMONOID.createMonoidBoolean(opPointer, zero)
  }

  implicit val monoidBuilderByte:MonoidBuilder[Byte] = new MonoidBuilder[Byte] {
    override def createMonoid(opPointer:Buffer, zero: Byte): Buffer =
      GRBMONOID.createMonoidByte(opPointer, zero)
  }

  implicit val monoidBuilderShort:MonoidBuilder[Short] = new MonoidBuilder[Short] {
    override def createMonoid(opPointer:Buffer, zero: Short): Buffer =
      GRBMONOID.createMonoidShort(opPointer, zero)
  }

  implicit val monoidBuilderInt:MonoidBuilder[Int] = new MonoidBuilder[Int] {
    override def createMonoid(opPointer:Buffer, zero: Int): Buffer =
      GRBMONOID.createMonoidInt(opPointer, zero)
  }

  implicit val monoidBuilderLong:MonoidBuilder[Long] = new MonoidBuilder[Long] {
    override def createMonoid(opPointer:Buffer, zero: Long): Buffer =
      GRBMONOID.createMonoidLong(opPointer, zero)
  }

  implicit val monoidBuilderFloat:MonoidBuilder[Float] = new MonoidBuilder[Float] {
    override def createMonoid(opPointer:Buffer, zero: Float): Buffer =
      GRBMONOID.createMonoidFloat(opPointer, zero)
  }

  implicit val monoidBuilderDouble:MonoidBuilder[Double] = new MonoidBuilder[Double] {
    override def createMonoid(opPointer:Buffer, zero: Double): Buffer =
      GRBMONOID.createMonoidDouble(opPointer, zero)
  }
}
