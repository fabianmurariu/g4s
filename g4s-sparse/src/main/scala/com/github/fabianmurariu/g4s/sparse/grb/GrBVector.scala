package com.github.fabianmurariu.g4s.sparse.grb

import java.nio.Buffer
import zio._

import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.unsafe.GRAPHBLAS
import com.github.fabianmurariu.g4s.sparse.mutable.SparseVector

final class GrBVector[T](private[grb] val pointer: Buffer)
    extends AutoCloseable {

  def close(): Unit = {
    GRBCORE.freeVector(this.pointer)
  }

}

object GrBVector {
  @inline def apply[A: VectorBuilder](size:Long) = {
    Managed.fromAutoCloseable(
      IO.effect {
        // ensure the GRB subsytem is init
        grb.GRB
        new GrBVector[A](pointer = VectorBuilder[A].build(size))
      }
    )
  }

  @inline def unsafe[A: VectorBuilder](size:Long):Task[GrBVector[A]] = {
      IO.effect {
        // ensure the GRB subsytem is init
        grb.GRB
        new GrBVector[A](pointer = VectorBuilder[A].build(size))
      }
}

  implicit val vectorInstance = new SparseVector[GrBVector] {

    override def make[A: VectorBuilder](size: Long): Managed[Throwable,GrBVector[A]] =
      GrBVector[A](size)


  }

}
