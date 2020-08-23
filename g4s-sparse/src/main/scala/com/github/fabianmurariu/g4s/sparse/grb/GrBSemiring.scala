package com.github.fabianmurariu.g4s.sparse.grb

import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRAPHBLAS
import com.github.fabianmurariu.unsafe.GRBCORE
import cats.effect.Sync
import cats.effect.Resource

final class GrBSemiring[A, B, C](private[grb] val pointer: Buffer)
    extends AutoCloseable {

  override def close(): Unit = {
    GRBCORE.freeSemiring(pointer)
  }

}

object GrBSemiring {
  def apply[F[_]:Sync, A, B, C](
      plus: GrBMonoid[C],
      times: GrBBinaryOp[A, B, C]
  ):Resource[F, GrBSemiring[A, B, C]] =
    Resource.fromAutoCloseable(
      Sync[F].delay{
        grb.GRB
        new GrBSemiring[A, B, C](GRBCORE.createSemiring(plus.pointer, times.pointer))
      }
    )
}
