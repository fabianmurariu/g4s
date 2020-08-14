package com.github.fabianmurariu.g4s.sparse.grb

import java.nio.Buffer
import zio._

import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.unsafe.GRAPHBLAS
import com.github.fabianmurariu.g4s.sparse.mutable.Matrix
import com.github.fabianmurariu.g4s.sparse.grb.instances.MatrixLikeInstance
import com.github.fabianmurariu.g4s.sparse.grb.instances.ElemWiseInstance
import com.github.fabianmurariu.g4s.sparse.grb.instances.MxMInstance
import com.github.fabianmurariu.g4s.sparse.grb.instances.MatrixInstance
import simulacrum.typeclass
import cats.effect.Resource
import cats.Applicative
import cats.effect.Sync

final class GrBMatrix[T](private[grb] val pointer: Buffer)
    extends AutoCloseable {

  def close(): Unit = {
    GRBCORE.freeMatrix(this.pointer)
  }

}

object GrBMatrix {
  @inline def apply[A: MatrixBuilder](rows: Long, cols: Long) = {
    Managed.fromAutoCloseable(
      IO.effect {
        // ensure the GRB subsytem is init
        grb.GRB
        new GrBMatrix[A](pointer = MatrixBuilder[A].build(rows, cols))
      }
    )
  }

  @inline def asResource[F[_]: Applicative, A: MatrixBuilder](
      rows: Long,
      cols: Long
  ) = {
    Resource.liftF(Applicative[F].pure {
      grb.GRB;
      new GrBMatrix[A](pointer = MatrixBuilder[A].build(rows, cols))
    })
  }

  @inline def unsafe[A: MatrixBuilder](
      rows: Long,
      cols: Long
  ): Task[GrBMatrix[A]] = {
    IO.effect {
      // ensure the GRB subsytem is init
      grb.GRB
      new GrBMatrix[A](pointer = MatrixBuilder[A].build(rows, cols))
    }
  }

  implicit val matrixInstance = new MatrixInstance

  implicit class MatrixOps[A](val ma: GrBMatrix[A])
      extends AnyVal
      with Matrix.Ops[GrBMatrix, A] {

    def self = ma
    def M = matrixInstance

  }

}
