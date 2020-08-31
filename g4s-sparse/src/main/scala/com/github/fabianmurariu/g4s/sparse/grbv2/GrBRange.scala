package com.github.fabianmurariu.g4s.sparse.grbv2

import com.github.fabianmurariu.unsafe.GRBCORE

sealed trait GrBRange

case class All(n:Long) extends GrBRange
case class Index(is:Array[Long]) extends GrBRange
case class IndexRange(start: Long, end: Long, inc: Long = 1L) extends GrBRange




object GrBRange {

  def apply(r: Range): GrBRange = {
    require(r.start >= 0  && r.end >= 0 && r.step != 0)
    r.isInclusive match {
    case true =>
      IndexRange(r.start, r.end, r.step) // GrBRanges are always exclusive
    case false =>
      IndexRange(r.start, r.end -1 , r.step)
    }
  }


  def indexRangeToGrB(r: IndexRange):(Long, Array[Long]) = {

    r.inc match {
      case 1 =>
        val ni = GRBCORE.GxB_RANGE
        val i:Array[Long] = Array(r.start, r.end)
        (ni, i)
      case n if n > 1 =>
        val ni = GRBCORE.GxB_STRIDE
        val i:Array[Long] = Array(r.start, r.end, n)
        (ni, i)
      case n if n < 0 =>
        val ni = GRBCORE.GxB_BACKWARDS
        val i:Array[Long] = Array(r.start, r.end, Math.abs(n))
        (ni, i)
    }
  }

  implicit def rangIsGrBRange(r:Range):GrBRange = apply(r)
}

trait GrBRangeLike[R] {
  def toGrB(r:R):(Long, Array[Long])
}

object GrBRangeLike {


  def apply[R](implicit G:GrBRangeLike[R]):GrBRangeLike[R] = G

  implicit val grbRange:GrBRangeLike[GrBRange] = new GrBRangeLike[GrBRange] {

    override def toGrB(r: GrBRange): (Long, Array[Long]) = r match {
      case ir:IndexRange => GrBRange.indexRangeToGrB(ir)
      case Index(is) => (is.length, is)
      case All(n) => (n, Array[Long](Long.MinValue))
    }

  }

  implicit val scalaRange:GrBRangeLike[Range] = new GrBRangeLike[Range] {

    override def toGrB(r: Range): (Long, Array[Long]) =
      GrBRangeLike[GrBRange].toGrB(GrBRange(r))

  }

}
