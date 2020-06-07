package com.github.fabianmurariu.g4s.sparse.grb

trait GrBError extends RuntimeException {
  def code: Long
}

object GrBError {

  def check(f: => Long): Unit = {
    val grbInfo = f
    grbInfo match {
      case 1 =>
        throw new GrBError {
          def code = 1
        }
      case _ =>
    }
  }

}
