package com.github.fabianmurariu.g4s.sparse.grbv2

import com.github.fabianmurariu.unsafe.GRBCORE

import scala.collection.immutable.NumericRange

class GrBRangeSpec extends munit.FunSuite {
  test("generate simple range with 1 increment") {
    val (ni, i) = GrBRangeLike[NumericRange[Long]].toGrB(0l to 10l)

    assertEquals(ni, GRBCORE.GxB_RANGE)
    assertEquals(i.toVector, Vector(0L, 10L))
  }

  test("generate stride range of step 2") {
    GrBRangeLike.scalaRange
    val (ni, i) = GrBRangeLike[NumericRange[Long]].toGrB(0L until 10L by 2)
    assertEquals(ni, GRBCORE.GxB_STRIDE)
    assertEquals(i.toVector, Vector(0L, 9L, 2L))
  }

  test("generate stride range of step -2") {
    val (ni, i) = GrBRangeLike[NumericRange[Long]].toGrB(10L until 1L by -2L)
    assertEquals(ni, GRBCORE.GxB_BACKWARDS)
    assertEquals(i.toVector, Vector(10L, 0L, 2L))
  }

  test("generage all range for size 10") {
    val (ni, i) = GrBRangeLike[GrBRange].toGrB(All(10))
    assertEquals(ni, 10L)
    assertEquals(i.toVector, Vector(Long.MinValue))
  }

}
