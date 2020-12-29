package com.github.fabianmurariu.g4s.traverser

import scala.reflect.runtime.universe.{Traverser => _, _}
import fix._

trait QueryGraphSamples {
  import com.github.fabianmurariu.g4s.traverser.Traverser._
  def singleEdge_Av_X_Bv =
    for {
      a <- node[A]
      b <- node[B]
      e <- edge[X](a, b)
    } yield e

  def Av_X_Bv_Y_Cv =
    for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield ()

  def Av_X_Bv_Y_Cv_and_Dv_Z_Bv=
    for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      d <- node[D]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
      _ <- edge[Z](d, b)
    } yield ()
    
  def Av_X_Bv_Y_Cv_Z_Dv =
    for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      d <- node[D]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
      _ <- edge[Z](c, d)
    } yield ()

  def Av_X_Bv_and_Av_Y_Cv =
    for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](a, c)
    } yield ()

  def Av_X_Bv_Y_Cv_and_Ev_X_Cv_and_Cv_Y_Dv =
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

  val a = implicitly[TypeTag[A]].tpe.toString
  val b = implicitly[TypeTag[B]].tpe.toString
  val c = implicitly[TypeTag[C]].tpe.toString
  val d = implicitly[TypeTag[D]].tpe.toString
  val e = implicitly[TypeTag[E]].tpe.toString
  val X = implicitly[TypeTag[X]].tpe.toString
  val Y = implicitly[TypeTag[Y]].tpe.toString
  val Z = implicitly[TypeTag[Z]].tpe.toString
}
