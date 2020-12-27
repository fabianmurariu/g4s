package com.github.fabianmurariu.g4s.traverser

import scala.reflect.runtime.universe.{Traverser => _, _}

trait QueryGraphSamples {
  import com.github.fabianmurariu.g4s.traverser.Traverser._
  def singleEdge_Av_X_Bv =
    for {
      a <- node[Av]
      b <- node[Bv]
      e <- edge[X](a, b)
    } yield e

  def Av_X_Bv_Y_Cv =
    for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield ()

  def Av_X_Bv_Y_Cv_Z_Dv =
    for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      d <- node[Dv]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
      _ <- edge[Z](c, d)
    } yield ()

  def Av_X_Bv_and_Av_Y_Cv =
    for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      _ <- edge[X](a, b)
      _ <- edge[Y](a, c)
    } yield ()

  def Av_X_Bv_Y_Cv_and_Ev_X_Cv_and_Cv_Y_Dv =
    for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      d <- node[Dv]
      e <- node[Ev]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
      _ <- edge[X](e, c)
      _ <- edge[Y](c, d)
    } yield ()

  def eval[T](t: Traverser[T]): QueryGraph =
    t.runS(emptyQG).value

  val aTag = implicitly[TypeTag[Av]].tpe.toString
  val bTag = implicitly[TypeTag[Bv]].tpe.toString
  val cTag = implicitly[TypeTag[Cv]].tpe.toString
  val dTag = implicitly[TypeTag[Dv]].tpe.toString
  val eTag = implicitly[TypeTag[Ev]].tpe.toString
  val xTag = implicitly[TypeTag[X]].tpe.toString
  val yTag = implicitly[TypeTag[Y]].tpe.toString
  val zTag = implicitly[TypeTag[Z]].tpe.toString
}
