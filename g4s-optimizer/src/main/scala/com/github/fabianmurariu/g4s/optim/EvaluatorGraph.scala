package com.github.fabianmurariu.g4s.optim

import cats.effect.IO
import com.github.fabianmurariu.g4s.matrix.BlockingMatrix

trait EvaluatorGraph {

  def lookupEdges(
      tpe: Option[String],
      transpose: Boolean
  ): IO[(BlockingMatrix[Boolean], Long)]

  def lookupEdges(
      tpe: String,
      transpose: Boolean
  ): IO[(BlockingMatrix[Boolean], Long)] =
    this.lookupEdges(Some(tpe), transpose)

  def lookupNodes(tpe: Option[String]): IO[(BlockingMatrix[Boolean], Long)]

  def lookupNodes(tpe: String): IO[(BlockingMatrix[Boolean], Long)] =
    this.lookupNodes(Some(tpe))

  def withStats[B](f: StatsStore => IO[B]): IO[B]
}
