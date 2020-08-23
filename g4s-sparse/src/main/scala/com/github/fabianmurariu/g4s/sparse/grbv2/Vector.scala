package com.github.fabianmurariu.g4s.sparse.grbv2

import java.nio.Buffer

import scala.{specialized => sp}

trait Vector[F[_], @sp(Boolean, Byte, Short, Int, Long, Float, Double) A]

