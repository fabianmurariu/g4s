package com.github.fabianmurariu.g4s.sparse.grbv2

import com.github.fabianmurariu.unsafe.GRBCORE

case class GrBRange(start: Long, end: Long, inc: Long = 1L)

object GrBRange {

  def apply(r: Range): GrBRange = {
    require(r.start >= 0  && r.end >= 0 && r.step != 0)
    r.isInclusive match {
    case true =>
      GrBRange(r.start, r.end, r.step) // GrBRanges are always exclusive
    case false =>
      GrBRange(r.start, r.end -1 , r.step)
    }
  }


  def toGrB(r: GrBRange):(Long, Array[Long]) = {

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
