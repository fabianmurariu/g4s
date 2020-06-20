package com.github.fabianmurariu.g4s.sparse.mutable
import zio._
import simulacrum.typeclass
import com.github.fabianmurariu.g4s.sparse.grb.VectorBuilder
import com.github.fabianmurariu.g4s.sparse.grb.VectorHandler

@typeclass trait SparseVector[V[_]] {
  def make[A:VectorBuilder](size:Long):Managed[Throwable, V[A]]

  def set[A](v:V[A])(i:Long, x:A)
         (implicit VH:VectorHandler[V, A]) =
    VH.set(v)(i, x)
}

object SparseVector {
}
