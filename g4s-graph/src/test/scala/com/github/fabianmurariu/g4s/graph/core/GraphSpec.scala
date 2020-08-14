package com.github.fabianmurariu.g4s.graph.core

import org.scalacheck._
import cats.Comonad
import cats.Id
import cats.Monad
import cats.Traverse
import zio.Task
import zio.Exit.Failure
import zio.Exit.Success
import cats.Foldable
import cats.effect.IO
import GraphProps._

object AdjacencyMapIdGraphSpec
    extends UndirectedSimpleGraphSpec[AdjacencyMap, IO](
      "Adjacency Map is a Graph"
    )
