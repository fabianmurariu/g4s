package com.github.fabianmurariu.g4s.graph.core

import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import scala.collection.mutable
import zio.Runtime.Managed
import zio.Task
import com.github.fabianmurariu.g4s.sparse.grb.MxM
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import cats.effect.Resource
import zio.interop.catz._

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
    GrBMatrix.asResource[Task, Boolean](rows, cols).map { edges =>
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

      private def selectors(cols: Long) = {
        for {
          add <- GrBMonoid[Boolean](OPS.any, true)
          semiring <- GrBSemiring[Boolean, Boolean, Boolean](add, OPS.pair)
          selector <- GrBMatrix[Boolean](1, cols)
        } yield (selector, semiring)
      }

      override def neighbours[V, E](fg: zio.Task[GrBSparseMatrixGraph[V, E]])(
          v: V
      ): zio.Task[Iterable[(V, E)]] = fg.flatMap { g =>
        //get indices
        g.indexV.get(v) match {
          case None =>
            Task(Iterable.empty)
          case Some(id) =>
            //TODO: this is terrible fix this
            selectors(g.edges.ncols).use {
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
          fg: zio.Task[GrBSparseMatrixGraph[V, E]]
      ): zio.Task[Iterable[V]] = fg.map { g => g.indexV.keys }

      override def edges[V, E](
                fg: zio.Task[GrBSparseMatrixGraph[V, E]]
      )(v:V): Task[Iterable[E]] = {
        ???
      }
      override def edgesTriples[V, E](
          g: zio.Task[GrBSparseMatrixGraph[V, E]]
      ): zio.Task[Iterable[(V, E, V)]] = ???

      override def containsV[V, E](fg: zio.Task[GrBSparseMatrixGraph[V, E]])(
          v: V
      ): zio.Task[Boolean] = fg.map {
        _.indexV.contains(v)
      }

      override def getEdge[V, E](
          fg: zio.Task[GrBSparseMatrixGraph[V, E]]
      )(v1: V, v2: V): zio.Task[Option[E]] = fg.map{ g =>
        for {
          src <- g.indexV.get(v1)
          dst <- g.indexV.get(v2)
          e <- g.indexE.get(src).flatMap(_.get(dst))
        } yield e
      }

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
      ): zio.Task[Long] = ???

      override def degree[V, E](fg: zio.Task[GrBSparseMatrixGraph[V, E]])(
          v: V
      ): zio.Task[Option[Long]] = fg.flatMap { g =>
        selectors(g.edges.ncols).use {
          case (selector, semi) =>
            g.indexV.get(v) match {
              case Some(id) =>
                selector.set(0, id, true)
                MxM[GrBMatrix]
                  .mxm(selector)(semi)(selector, g.edges)
                  .map(m => Some(m.nvals))
              case None => Task(None)
            }
        }
      }

      override def empty[V, E]: Resource[zio.Task, GrBSparseMatrixGraph[V, E]] =
        GrBSparseMatrixGraph.empty[V, E]
    }

}
