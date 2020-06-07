package com.github.fabianmurariu.g4s.sparse.mutable

import java.lang.AutoCloseable
import java.nio.Buffer
import scala.{specialized => sp}
import simulacrum.typeclass
import com.github.fabianmurariu.g4s.sparse.grb.{
  GrBBinaryOp,
  ElemWise,
  Reduce,
  GrBMonoid,
  GrBMatrix,
  EqOp,
  MatrixHandler
}
import zio._
import com.github.fabianmurariu.g4s.sparse.grb.MatrixBuilder

@typeclass trait MatrixLike[M[_]] {
  def nvals[A](f: M[A]): Long
  def nrows[A](f: M[A]): Long
  def ncols[A](f: M[A]): Long
  def clear[A](f: M[A]): Unit
  def resize[A](f: M[A])(rows: Long, cols: Long): Unit

  def get[A](f: M[A])(i: Long, j: Long)
         (implicit MH: MatrixHandler[M, A]): Option[A] = {
    MH.get(f)(i, j)
  }

  def set[@sp(Boolean, Byte, Short, Int, Long, Float, Double) A](f: M[A])
         (i: Long, j: Long, a: A)
         (implicit MH: MatrixHandler[M, A]): Unit = {
    MH.set(f)(i, j, a)
  }

  def duplicate[A](f: M[A]): Managed[Throwable, M[A]]
  def make[A:MatrixBuilder](rows:Long, cols:Long):Managed[Throwable, M[A]]
  // for the special case when you want to manage your own resources see GraphDB
  def makeUnsafe[A:MatrixBuilder](rows:Long, cols:Long): Task[M[A]]

}
