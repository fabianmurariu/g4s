package com.github.fabianmurariu.g4s.graph.core

import org.scalatest.flatspec.AnyFlatSpec
import zio._
import zio.interop.catz._
import org.scalatest.matchers.must.Matchers

object GrBSparseMatrixGraphSpec
    extends UndirectedSimpleGraphSpec[GrBSparseMatrixGraph, Task](
      "GrBSparseMatrix is a Graph"
    )
