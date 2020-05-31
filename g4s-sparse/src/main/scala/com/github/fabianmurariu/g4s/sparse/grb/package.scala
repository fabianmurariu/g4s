package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.unsafe.GRBCORE
import java.util.concurrent.atomic.AtomicLong

package object grb {

  class ShutdownGRB extends Thread {
    override def run(): Unit = {
      GRBCORE.grbFinalize()
    }
  }

  lazy val GRB = {
    Runtime.getRuntime().addShutdownHook(new ShutdownGRB)

    GRBCORE.initNonBlocking()
  }

}
