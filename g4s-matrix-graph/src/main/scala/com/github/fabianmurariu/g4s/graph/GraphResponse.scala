package com.github.fabianmurariu.g4s.graph

import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix

trait GraphResponse

case class Frontier[F[_]](mat: GrBMatrix[F, Boolean]) extends GraphResponse
