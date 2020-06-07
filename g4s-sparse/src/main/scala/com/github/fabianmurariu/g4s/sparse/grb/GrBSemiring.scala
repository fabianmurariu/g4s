package com.github.fabianmurariu.g4s.sparse.grb

import zio._
import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRAPHBLAS
import com.github.fabianmurariu.unsafe.GRBCORE

final class GrBSemiring[A, B, C](private[grb] val pointer: Buffer)
    extends AutoCloseable {

  override def close(): Unit = {
    GRBCORE.freeSemiring(pointer)
  }

}

object GrBSemiring {
  def apply[A, B, C](
      plus: GrBMonoid[C],
      times: GrBBinaryOp[A, B, C]
  ):TaskManaged[GrBSemiring[A, B, C]] =
    Managed.fromAutoCloseable(
      IO.effect {
        grb.GRB
        new GrBSemiring(GRBCORE.createSemiring(plus.pointer, times.pointer))
      }
    )
}
