package com.github.fabianmurariu.g4s.sparse.grb

import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRBCORE
import cats.effect.Sync
import cats.effect.Resource

final class GrBSemiring[A, B, C](private[sparse] val pointer: Buffer)(implicit G:GRB)
    extends AutoCloseable {

  override def close(): Unit = {
    GRBCORE.freeSemiring(pointer)
  }

}

object GrBSemiring {
  def apply[F[_]: Sync, A, B, C](
      plus: GrBBinaryOp[C, C, C],
      times: GrBBinaryOp[A, B, C],
      zero: C
  )(implicit M: MonoidBuilder[C], G:GRB): Resource[F, GrBSemiring[A, B, C]] =
    GrBMonoid[F, C](plus, zero).flatMap { plus => GrBSemiring(plus, times) }

  def apply[F[_]: Sync, A, B, C](
      plus: GrBMonoid[C],
      times: GrBBinaryOp[A, B, C]
  )(implicit G:GRB): Resource[F, GrBSemiring[A, B, C]] =
    Resource.fromAutoCloseable(
      Sync[F].delay {
        new GrBSemiring[A, B, C](
          GRBCORE.createSemiring(plus.pointer, times.pointer)
        )
      }
    )
}
