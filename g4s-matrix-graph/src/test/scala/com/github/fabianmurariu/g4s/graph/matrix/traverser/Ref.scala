package com.github.fabianmurariu.g4s.graph.matrix.traverser

sealed abstract class Ref { self =>
  private def shortName(s: String) = s.split("\\.").last
  override def toString: String = self match {
    case NodeRef(name)           => s"(${shortName(name)})"
    case EdgeRef(name, src, dst) => s"$src -[${shortName(name)}]-> $dst"
  }
}
case class NodeRef(val name: String) extends Ref
case class EdgeRef(val name: String, val src: NodeRef, val dst: NodeRef)
    extends Ref
