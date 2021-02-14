package com.github.fabianmurariu.g4s.traverser

import scala.reflect.runtime.universe.{Traverser => _, _}
import fix._
import cats.Eval
import cats.data.IndexedStateT

trait QueryGraphSamples {
  import com.github.fabianmurariu.g4s.traverser.Traverser._
  def singleEdge_Av_X_Bv: IndexedStateT[Eval,QueryGraph,QueryGraph,EdgeRef] =
    for {
      a <- node[A]
      b <- node[B]
      e <- edge[X](a, b)
    } yield e

  def Av_X_Bv_Y_Cv: IndexedStateT[Eval,QueryGraph,QueryGraph,Unit] =
    for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield ()

  def Av_X_Bv_Y_Cv_and_Dv_Z_Bv: IndexedStateT[Eval,QueryGraph,QueryGraph,Unit]=
    for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      d <- node[D]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
      _ <- edge[Z](d, b)
    } yield ()
    
  def Av_X_Bv_Y_Cv_Z_Dv: IndexedStateT[Eval,QueryGraph,QueryGraph,Unit] =
    for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      d <- node[D]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
      _ <- edge[Z](c, d)
    } yield ()

  def Av_X_Bv_and_Av_Y_Cv: IndexedStateT[Eval,QueryGraph,QueryGraph,Unit] =
    for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](a, c)
    } yield ()

  def Av_X_Bv_Y_Cv_and_Ev_X_Cv_and_Cv_Y_Dv: IndexedStateT[Eval,QueryGraph,QueryGraph,Unit] =
    for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      d <- node[D]
      e <- node[E]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
      _ <- edge[X](e, c)
      _ <- edge[Y](c, d)
    } yield ()

  def eval[T](t: Traverser[T]): QueryGraph =
    t.runS(emptyQG).value

  val a: String = implicitly[TypeTag[A]].tpe.toString
  val b: String = implicitly[TypeTag[B]].tpe.toString
  val c: String = implicitly[TypeTag[C]].tpe.toString
  val d: String = implicitly[TypeTag[D]].tpe.toString
  val e: String = implicitly[TypeTag[E]].tpe.toString
  val X: String = implicitly[TypeTag[X]].tpe.toString
  val Y: String = implicitly[TypeTag[Y]].tpe.toString
  val Z: String = implicitly[TypeTag[Z]].tpe.toString
}
