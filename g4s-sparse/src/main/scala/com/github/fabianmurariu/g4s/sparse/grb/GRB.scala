package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.unsafe.GRBCORE

sealed trait GRB

case object AsyncGRB extends GRB
case object SyncGRB extends GRB

object GRB {
  object sync {
    implicit lazy val grb: GRB = {
      GRBCORE.initNonBlocking()
      SyncGRB
    }
  }

  object async {
    implicit lazy val grb: GRB = {
      GRBCORE.initBlocking()
      AsyncGRB
    }
  }
}

class ShutdownGRB extends Thread {
  override def run(): Unit = {
    assert(GRBCORE.grbFinalize() == 0L)
  }
}
