package com.github.fabianmurariu.g4s.sparse.grbv2

import java.nio.Buffer

sealed trait GrBRef

case object Empty extends GrBRef
case class GrBPointer(ref: Buffer) extends GrBRef
