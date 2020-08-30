package com.github.fabianmurariu.g4s.sparse.grb

import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRBCORE

//FIXME: make pointer private
case class GrBDescriptor(val pointer:Buffer) extends AutoCloseable{

  def close():Unit = {
    GRBCORE.freeDescriptor(pointer)
  }
}
