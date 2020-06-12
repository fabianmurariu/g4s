package com.github.fabianmurariu.g4s.graph

import cats.free.Free
import cats.arrow.FunctionK
import cats.{Id, ~>}
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import scala.collection.mutable
import com.github.fabianmurariu.g4s.sparse.mutable.MatrixLike
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.MxM
import com.github.fabianmurariu.g4s.sparse.grb.ElemWise
import com.github.fabianmurariu.g4s.sparse.mutable.Matrix
import com.github.fabianmurariu.g4s.sparse.grb.MatrixHandler
import scala.collection.generic.CanBuildFrom
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.sparse.grb.MatrixBuilder
import zio._
import fs2.Stream
import simulacrum.typeclass

/**
  * Naive first pass implementation of a Graph database
  * over sparse matrices that may be of type GraphBLAS
  * FIXME:
  *  this is mutable but all functions return effects
  *  multiple threads are not supported here
  * */
final class GraphDB[M[_], V: graph.Vertex, E: graph.Edge](
    private[graph] val nodes: M[Boolean],
    private[graph] val edgesOut: M[Boolean],
    private[graph] val edgesIn: M[Boolean],
    private[graph] val relTypes: mutable.Map[String, M[Boolean]],
    private[graph] val labelIndex: mutable.Map[String, mutable.Set[
      Long
    ]], // node labels and node ids
    private[graph] val nodeData: mutable.Map[Long, mutable.Set[String]],
    private[graph] val edgeData: mutable.Map[(Long, Long), mutable.Set[String]],
    private[graph] var count: Long
)(
    implicit M: Matrix[M],
    MH: MatrixHandler[M, Boolean],
    MB: MatrixBuilder[Boolean],
    OPS: BuiltInBinaryOps[Boolean]
) { self =>

  def insertVerticesChunk(
      vs: fs2.Chunk[Vector[String]]
  ): Task[fs2.Chunk[Long]] = IO.effect {
    vs.map(GraphDB.insertVertex0(this, _))
  }

  def insertEdge(
      e: (Long, String, Long)
  ): Task[(Long, Long)] = {
    GraphDB.insertEdge0(this, e)
  }

  def insertVertex(labels: Vector[String]): Task[Long] = IO.effect {
    GraphDB.insertVertex0(this, labels)
  }

  /**
    * TODO:
    * assumed relation is we want the matrix including all the labels
    * but we could create a boolean expression saying we want :a labels and not :b labels etc..
    */
  def vectorMatrixForLabels(labels: Set[String]): TaskManaged[M[Boolean]] = {
    val mRes = M.make(16, 16)
    mRes.map { m =>
      labels.foldLeft(m) { (mat, label) =>
        labelIndex.get(label).foreach { nodes =>
          nodes.foldLeft(m) { (mat, vid) =>
            MH.set(m)(vid, vid, true)
            m
          }
        }
        m
      }
    }
  }

  /**
    * TODO:
    * see vectorMatrixForLabels
    */
  def edgeMatrixForTypes(
      types: Set[String],
      out: Boolean
  ): TaskManaged[M[Boolean]] = {
    val outRes = M.make(16, 16)

    outRes.mapM { out =>
      types
        .flatMap(tpe => relTypes.get(tpe))
        .foldLeft(IO.effect(out)) { (intoIO, mat) =>
          for {
            into <- intoIO
            union <- Matrix[M].union(into)(Left(OPS.land))(into, mat)
          } yield union
        }
    }
  }

  /**
    * transform a matrix of vertices to the external world
    * view of Id + Labels
    */
  def labeledVertices(m: M[Boolean]): Vector[(Long, Set[String])] = {
    val (_, _, js) = MH.copyData(m)

    js.map(vId =>
        vId -> self.nodeData.get(vId).map(_.to[Set]).getOrElse(Set.empty)
      )
      .toVector
  }

  /**
    * Naive approach of moving from sparse matrix to external data =
    * FIXME: for more serious business check-out
    * https://groups.google.com/a/lbl.gov/forum/#!topic/graphblas/CU2bTF1DV_8
    * and
    * https://github.com/GraphBLAS/LAGraph/blob/1eb22f947e606fe18e8e0aef2ba9b9047010b718/Source/Algorithm/LAGraph_dense_relabel.c
    */
  def renderEdges(m: M[Boolean]): Vector[(Long, String, Long)] = {
    val (_, is, js) = MH.copyData(m)

    (0 until js.length)
      .foldLeft(Vector.newBuilder[(Long, String, Long)]) { (b, i) =>
        val src = is(i)
        val dest = js(i)
        edgeData.get(src, dest).foreach { tpes =>
          tpes.foreach { tpe => b += ((src, tpe, dest)) }
        }
        b
      }
      .result()
  }

}

object GraphDB {

  def default[V: graph.Vertex, E: graph.Edge]
      : TaskManaged[GraphDB[GrBMatrix, V, E]] =
    for {
      nodes <- GrBMatrix[Boolean](16, 16)
      edgesIn <- GrBMatrix[Boolean](16, 16)
      edgesOut <- GrBMatrix[Boolean](16, 16)
      graph <- Managed.makeEffect(
        new GraphDB[GrBMatrix, V, E](
          nodes,
          edgesIn,
          edgesOut,
          mutable.Map.empty,
          mutable.Map.empty,
          mutable.Map.empty,
          mutable.Map.empty,
          0L
        )
      ) { gdb => gdb.relTypes.valuesIterator.foreach(_.close()) }
    } yield graph

  private def insertVertex0[M[_]: Matrix, V, E](
      g: GraphDB[M, V, E],
      labels: Vector[String]
  )(implicit M: MatrixHandler[M, Boolean]): Long = {
    val vxId = g.count;
    M.set(g.nodes)(vxId, vxId, true)

    labels.foreach { label =>
      val set = g.labelIndex.getOrElseUpdate(label, mutable.Set(vxId))
      set += vxId
    }

    val nodeLabels = g.nodeData.getOrElseUpdate(vxId, mutable.Set())
    labels.foldLeft(nodeLabels) { _ += _ }

    g.count += 1
    vxId
  }

  private def insertEdge0[M[_]: Matrix, V, E](g:GraphDB[M, V, E],
                                              e:(Long, String, Long))
                         (implicit MH:MatrixHandler[M, Boolean]): Task[(Long, Long)] = {
    val (src, tpe, dest) = e
    for {
      _ <- IO.effect {
        MH.set(g.edgesOut)(src, dest, true)
        MH.set(g.edgesIn)(dest, src, true)
      }

      tpeMat <- {
        g.relTypes.get(tpe) match {
          case None => {
            Matrix[M].makeUnsafe[Boolean](16, 16).map { m =>
              g.relTypes.put(tpe, m)
              m
            }
          }
          case Some(m) => IO.effect(m)
        }
      }
      pair <- IO.effect {

        MH.set(tpeMat)(src, dest, true)

        val edgeTypeData =
          g.edgeData.getOrElseUpdate((src, dest), mutable.Set(tpe))
        edgeTypeData += tpe
        (src, dest)

      }
    } yield pair
  }
}
