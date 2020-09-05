package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.unsafe.GRBCORE

package object grb {

  lazy val GRB = {
    Runtime.getRuntime().addShutdownHook(new ShutdownGRB)

    GRBCORE.initNonBlocking()
  }

}
class ShutdownGRB extends Thread {
  override def run(): Unit = {
    assert(GRBCORE.grbFinalize() == 0L)
  }
}
