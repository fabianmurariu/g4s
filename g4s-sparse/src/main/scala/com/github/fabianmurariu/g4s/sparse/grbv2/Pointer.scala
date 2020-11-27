package com.github.fabianmurariu.g4s.sparse.grbv2

import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.g4s.sparse.grb.GRB

private[grbv2] sealed trait Pointer extends AutoCloseable {
  def ref: Buffer
}

class MatrixPointer(val ref: Buffer)(implicit G:GRB) extends Pointer {

  override def close(): Unit =
    GRBCORE.freeMatrix(ref)

}

class VectorPointer(val ref: Buffer)(implicit G:GRB) extends Pointer {
  override def close(): Unit =
    GRBCORE.freeVector(ref)
}
