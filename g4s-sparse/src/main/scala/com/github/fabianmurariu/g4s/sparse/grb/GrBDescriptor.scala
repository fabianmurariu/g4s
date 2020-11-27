package com.github.fabianmurariu.g4s.sparse.grb

import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRBCORE

case class GrBDescriptor(private[sparse] val pointer:Buffer)(implicit G:GRB) extends AutoCloseable{

  def close():Unit = {
    GRBCORE.freeDescriptor(pointer)
  }
}
