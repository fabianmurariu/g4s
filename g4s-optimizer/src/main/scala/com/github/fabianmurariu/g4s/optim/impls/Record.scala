package com.github.fabianmurariu.g4s.optim.impls

import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.g4s.matrix.BlockingMatrix
import scala.collection.mutable
import cats.effect.IO

sealed trait Record 

sealed trait EdgesRecord extends Record
// represents an immutable matrix of nodes from the graph
case class Nodes(mat:BlockingMatrix[Boolean]) extends Record
// represents an immutable matrix of edges from the graph
case class Edges(mat:BlockingMatrix[Boolean]) extends EdgesRecord
// represents an intermediate result
case class MatrixRecord(mat:GrBMatrix[IO, Boolean]) extends Record
// represents to builder to get data out of the operators
case class OutputRecord(buffer: mutable.ArrayBuffer[_ <: Any]) extends Record
