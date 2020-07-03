package com.github.fabianmurariu.g4s.graph.core

import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import scala.collection.mutable
import zio.Runtime.Managed
import zio.Task
import com.github.fabianmurariu.g4s.sparse.grb.MxM
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps

class GrBSparseMatrixGraph[V, E] private (
    private[core] val edges: GrBMatrix[Boolean],
    private[core] val indexE: mutable.Map[Long, mutable.Map[Long, E]],
    private[core] val indexV: mutable.Map[V, Long],
    private[core] val indexRevV: mutable.Map[Long, V],
    private[core] var id: Long
)

object GrBSparseMatrixGraph {

  val resizeSteps = Array(16, 32, 64, 128, 256)

  def empty[V, E] = {
    val rows = 16L
    val cols = 16L
    GrBMatrix[Boolean](rows, cols).map { edges =>
      new GrBSparseMatrixGraph[V, E](
        edges,
        mutable.Map.empty[Long, mutable.Map[Long, E]],
        mutable.Map.empty[V, Long],
        mutable.Map.empty[Long, V],
        0
      )
    }
  }

  implicit def sparseMatrixIsGraph(
      implicit OPS: BuiltInBinaryOps[Boolean]
  ): Graph[GrBSparseMatrixGraph, Task] =
    new Graph[GrBSparseMatrixGraph, Task] {

      override def neighbours[V, E](fg: zio.Task[GrBSparseMatrixGraph[V, E]])(
          v: V
      ): zio.Task[Traversable[(V, E)]] = fg.flatMap { g =>
        //get indices
        g.indexV.get(v) match {
          case None =>
            Task(Traversable.empty)
          case Some(id) =>
            val rows = g.edges.nrows
            val sel = for {
              add <- GrBMonoid[Boolean](OPS.any, true)
              semiring <- GrBSemiring[Boolean, Boolean, Boolean](add, OPS.pair)
              selector <- GrBMatrix[Boolean](1, rows)
            } yield (selector, semiring)

            //TODO: this is terrible fix this
            sel.use {
              case (m, semi) =>
                m.set(0, id, true)
                m.nvals // force
                MxM[GrBMatrix]
                  .mxm(m)(semi)(m, g.edges)
                  .map { grbNeighbours =>
                    val (_, _, js) = grbNeighbours.copyData
                    js.flatMap { neighbourId =>
                      g.indexRevV
                        .get(neighbourId)
                        .flatMap { neighbour =>
                          g.indexE
                            .getOrElse(id, mutable.Map.empty)
                            .get(neighbourId)
                            .map(e => neighbour -> e)
                        }
                    }
                  }
            }
        }
      }

      override def vertices[V, E](
          g: zio.Task[GrBSparseMatrixGraph[V, E]]
      ): zio.Task[Traversable[V]] = ???

      override def edgesTriples[V, E](
          g: zio.Task[GrBSparseMatrixGraph[V, E]]
      ): zio.Task[Traversable[(V, E, V)]] = ???

      override def containsV[V, E](fg: zio.Task[GrBSparseMatrixGraph[V, E]])(
          v: V
      ): zio.Task[Boolean] = fg.map {
        _.indexV.contains(v)
      }

      override def getEdge[V, E](
          g: zio.Task[GrBSparseMatrixGraph[V, E]]
      )(v1: V, v2: V): zio.Task[Option[E]] = ???

      override def insertVertex[V, E](fg: zio.Task[GrBSparseMatrixGraph[V, E]])(
          v: V
      ): zio.Task[GrBSparseMatrixGraph[V, E]] = fg.map { g =>
        g.indexV.get(v) match {
          case None =>
            val id = g.id
            g.indexRevV += (id -> v)
            g.indexV += (v -> id)
            g.id += 1
            val cols = g.edges.ncols
            if (cols <= id + 1) {
              g.edges.resize(
                cols * 2,
                cols * 2
              ) // FIXME: need a better resize policy
            }
            g
          case _ => g
        }
      }

      override def insertEdge[V, E](
          fg: zio.Task[GrBSparseMatrixGraph[V, E]]
      )(src: V, dst: V, e: E): zio.Task[GrBSparseMatrixGraph[V, E]] = fg.map {
        g =>
          val edgeIds = for {
            s <- g.indexV.get(src)
            d <- g.indexV.get(dst)
          } yield (s, d)

          edgeIds.foreach {
            case (s, d) =>
              g.edges.set(s, d, true)
              g.edges.set(d, s, true)
              val src2DstEdges =
                g.indexE.getOrElseUpdate(s, mutable.Map.empty[Long, E])
                src2DstEdges.put(d, e)
              val dst2SrcEdges =
                g.indexE.getOrElseUpdate(d, mutable.Map.empty[Long, E])
                dst2SrcEdges.put(s, e)
          }

          g
      }

      override def removeVertex[V, E](g: zio.Task[GrBSparseMatrixGraph[V, E]])(
          v: V
      ): zio.Task[GrBSparseMatrixGraph[V, E]] = ???

      override def removeEdge[V, E](
          g: zio.Task[GrBSparseMatrixGraph[V, E]]
      )(src: V, dst: V): zio.Task[GrBSparseMatrixGraph[V, E]] = ???

      override def orderG[V, E](
          g: zio.Task[GrBSparseMatrixGraph[V, E]]
      ): zio.Task[Int] = ???

      override def sizeG[V, E](
          g: zio.Task[GrBSparseMatrixGraph[V, E]]
      ): zio.Task[Int] = ???

      override def degree[V, E](g: zio.Task[GrBSparseMatrixGraph[V, E]])(
          v: V
      ): zio.Task[Int] = ???

      override def empty[V, E]: zio.Task[GrBSparseMatrixGraph[V, E]] = ???
    }

}
