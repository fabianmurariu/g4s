package com.github.fabianmurariu.g4s.sparse.grb

import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRAPHBLAS
import com.github.fabianmurariu.unsafe.GRBCORE

case class GrBSemiring[A, B, C](private[grb] val pointer: Buffer) extends AutoCloseable {

  override def close(): Unit = {
    GRBCORE.freeSemiring(pointer)
  }


}

object GrBSemiring {
  def apply[A, B, C](
      plus: GrBMonoid[C],
      times: GrBBinaryOp[A, B, C]
  ): GrBSemiring[A, B, C] =
    GrBSemiring(GRBCORE.createSemiring(plus.pointer, times.pointer))
}
