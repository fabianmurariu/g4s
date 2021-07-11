package com.github.fabianmurariu.g4s.optim.impls

import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.g4s.matrix.BlockingMatrix

sealed trait Record 

sealed trait EdgesRecord extends Record
// represents an immutable matrix of nodes from the graph
case class Nodes[F[_]](mat:BlockingMatrix[F, Boolean]) extends Record
// represents an immutable matrix of edges from the graph
case class Edges[F[_]](mat:BlockingMatrix[F, Boolean]) extends EdgesRecord
// represents an intermediate result
case class MatrixRecord[F[_]](mat:GrBMatrix[F, Boolean]) extends Record

