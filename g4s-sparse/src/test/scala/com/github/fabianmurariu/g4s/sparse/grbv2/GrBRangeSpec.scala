package com.github.fabianmurariu.g4s.sparse.grbv2

import com.github.fabianmurariu.unsafe.GRBCORE

class GrBRangeSpec extends munit.FunSuite {
  test("generate simple range with 1 increment") {
    val (ni, i) = GrBRange.toGrB(0 until 10)

    assertEquals(ni, GRBCORE.GxB_RANGE)
    assertEquals(i.toVector, Vector(0L, 10L))
  }

  test("generate stride range of step 2") {
    val (ni, i) = GrBRange.toGrB(0 until 10 by 2)
    assertEquals(ni, GRBCORE.GxB_STRIDE)
    assertEquals(i.toVector, Vector(0L, 10L, 2L))
  }

  test("generate stride range of step -2") {
    val (ni, i) = GrBRange.toGrB(10 until 1 by -2)
    assertEquals(ni, GRBCORE.GxB_BACKWARDS)
    assertEquals(i.toVector, Vector(10L, 1L, 2L))
  }

}
