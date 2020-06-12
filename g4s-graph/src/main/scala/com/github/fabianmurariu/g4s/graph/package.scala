package com.github.fabianmurariu.g4s.graph

import simulacrum.typeclass

package object graph {

  @typeclass trait Vertex[T] {
    def id(t:T):Long
    def labels(t:T):Set[String]
  }

  @typeclass trait Edge[T] {
    def src(t:T): Long
    def dest(t:T): Long
    def tpe(t:T):String
  }

  object Vertex{
    implicit val defaultVertexInstance:Vertex[DefaultVertex] = new Vertex[DefaultVertex] {

      override def id(t: DefaultVertex): Long = t.id

      override def labels(t: DefaultVertex): Set[String] = t.labels

    }
  }

  object Edge {
    implicit val deftaultEdgeInstance:Edge[DefaultEdge] = new Edge[DefaultEdge] {

      override def src(t: DefaultEdge): Long = t.src

      override def dest(t: DefaultEdge): Long = t.dest

      override def tpe(t: DefaultEdge): String = t.tpe


    }
  }

  case class DefaultVertex(id: Long, labels: Set[String])

  case class DefaultEdge(src: Long, dest: Long, tpe:String)
}
