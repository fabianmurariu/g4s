package com.github.fabianmurariu.g4s.optim

import simulacrum.typeclass
import scala.collection.mutable

@typeclass
trait DirectedGraph[G[_, _]] {
  def insert[V, E](g: G[V, E])(v: V): G[V, E]

  def edge[V, E](g: G[V, E])(src: V, e: E, dst: V): G[V, E]

  def out[V, E](g: G[V, E])(v: V): Iterable[(V, E)]
  def in[V, E](g: G[V, E])(v: V): Iterable[(V, E)]

  def get[V, E](g: G[V, E])(v: V): Option[V]

  def getEdge[V, E](g: G[V, E])(src: V, dst: V):Option[(V, E, V)]

  def neighbours[V, E](g: G[V, E])(v: V): Iterable[(V, E)] =
    out(g)(v) ++ in(g)(v)

  def outDegree[V, E](g: G[V, E])(v:V): Int
  def inDegree[V, E](g: G[V, E])(v:V): Int

  def degree[V, E](g: G[V, E])(v:V): Int =
    outDegree(g)(v) + inDegree(g)(v)

  def vertices[V, E](g: G[V, E]):collection.Set[V]
}

case class VertexContainer[V, E](
    v: V,
    out: mutable.HashMap[V, E],
    in: mutable.HashMap[V, E]
) {
  def insertOutEdge(dst: V, e: E): VertexContainer[V, E] = {
    out += (dst -> e)
    this
  }

  def insertInEdge(src: V, e: E): VertexContainer[V, E] = {
    in += (src -> e)
    this
  }
}

case class MutableGraph[V, E](
    val map: mutable.HashMap[V, VertexContainer[V, E]] = mutable.HashMap.empty[V, VertexContainer[V, E]]
) 

object MutableGraph {

  def empty[V, E] = new MutableGraph[V, E](map = mutable.HashMap.empty)

  implicit def directedGraph: DirectedGraph[MutableGraph] =
    new DirectedGraph[MutableGraph] {

      override def vertices[V, E](g: MutableGraph[V,E]): collection.Set[V] =
        g.map.keySet

      override def getEdge[V, E](g:MutableGraph[V, E])(src: V, dst: V): Option[(V, E, V)] = {
       val direct = for {
         c <- g.map.get(src)
         out <- c.out.get(dst)
         d <- g.map.get(dst)
       } yield (c.v, out, d.v)

        def transpose = for {
          c <- g.map.get(dst)
          out <- c.out.get(src)
          d <- g.map.get(src)
        } yield (c.v, out, d.v)

        direct.orElse(transpose)
      }


      def outDegree[V, E](g:MutableGraph[V, E])(v:V): Int = 
        g.map.get(v).map(_.out.size).getOrElse(-1)
      
      def inDegree[V, E](g: MutableGraph[V, E])(v:V): Int =
        g.map.get(v).map(_.in.size).getOrElse(-1)


      def get[V, E](g: MutableGraph[V, E])(v: V): Option[V] =
        g.map.get(v).map(_.v)

      def emptyContainer[V, E](v: V): VertexContainer[V, E] =
        new VertexContainer[V, E](
          v,
          mutable.HashMap.empty,
          mutable.HashMap.empty
        )

      private def insert0[V, E](g: MutableGraph[V, E])(
          v: V
      ): VertexContainer[V, E] = {
          g.map.updateWith(v){
            case Some(container) => Some(container.copy(v = v))
            case None => Some(emptyContainer(v))
          }.get
      }

      override def insert[V, E](g: MutableGraph[V, E])(
          v: V
      ): MutableGraph[V, E] = {
        insert0(g)(v)
        g
      }

      override def edge[V, E](
          g: MutableGraph[V, E]
      )(src: V, e: E, dst: V): MutableGraph[V, E] = {
        insert0(g)(src).insertOutEdge(dst, e)
        insert0(g)(dst).insertInEdge(src, e)
        g
      }

      override def out[V, E](g: MutableGraph[V, E])(v: V): Iterable[(V, E)] =
        g.map.get(v).toSeq.flatMap(_.out)

      override def in[V, E](g: MutableGraph[V, E])(v: V): Iterable[(V, E)] =
        g.map.get(v).toSeq.flatMap(_.in)

    }
}
