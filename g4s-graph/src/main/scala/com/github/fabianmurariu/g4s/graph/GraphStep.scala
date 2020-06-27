package com.github.fabianmurariu.g4s.graph

import cats.free.Free
import cats.{Id, ~>}
import zio._
import fs2.Stream
import scala.collection.immutable.{Stream => SStream}
import com.github.fabianmurariu.g4s.sparse.mutable.Matrix
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.MxM
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.sparse.grb.MatrixBuilder
import com.github.fabianmurariu.g4s.sparse.mutable.SparseVector
import com.github.fabianmurariu.g4s.sparse.grb.MatrixHandler
import com.github.fabianmurariu.g4s.sparse.grb.VectorHandler

sealed trait GraphStep[+T]

case class CreateNodes(labels: Vector[Vector[String]]) extends GraphStep[Long]
case class CreateEdges(edges: Vector[(Long, String, Long)])
    extends GraphStep[(Long, Long)]

case class CreateNode(labels: Vector[String]) extends GraphStep[Long]
case class CreateEdge(src: Long, tpe: String, dest: Long)
    extends GraphStep[(Long, Long)]
case class QueryStep(q: Query) extends GraphStep[QueryResult]
case class PathStep(q: Query) extends GraphStep[QueryResult]

object GraphStep {

  type Step[T] = Free[GraphStep, T]
  type Observable[T] = fs2.Stream[Task, T]

  def interpreter[M[_]: Matrix, SV[_]: SparseVector](
      g: GraphDB[M]
  )(implicit MH: MatrixHandler[M, Int], VH: VectorHandler[SV, Int]) =
    new (GraphStep ~> Observable) {
      def apply[A](fa: GraphStep[A]): Observable[A] = fa match {

        case CreateNodes(nodes) =>
          Stream
            .chunk(fs2.Chunk.vector(nodes))
            .chunks
            .evalMap(g.insertVerticesChunk(_))
            .flatMap(c => Stream.chunk(c))

        case CreateEdges(edges) =>
          Stream
            .chunk(fs2.Chunk.vector(edges))
            .evalMap(g.insertEdge(_))

        case CreateNode(labels) =>
          Stream.eval(
            g.insertVertex(labels)
          )

        case CreateEdge(src, tpe, dest) =>
          Stream.eval(
            g.insertEdge(src, tpe, dest)
          )

        case QueryStep(q: EdgeQuery) =>
          Stream.eval(matAsEdges(foldQueryPath(g, q))(g))

        case QueryStep(q: VertexQuery) =>
          Stream.eval(matAsVertices(foldQueryPath(g, q))(g))

        case QueryStep(q @ AndThen(_, _: EdgeQuery)) =>
          Stream.eval(matAsEdges(foldQueryPath(g, q))(g))

        case QueryStep(q @ AndThen(_, _: VertexQuery)) =>
          Stream.eval(matAsVertices(foldQueryPath(g, q))(g))

        case PathStep(q: Query) =>
          Stream.eval(matAsPaths(foldQueryWithPaths(g, q))(g))
      }

    }

  def lift[G[_], A](g: G[A]): TaskManaged[G[A]] =
    Managed.fromEffect(IO.effect(g))

  /**
    * Because we need to track every path from a starting node
    * this will hold a separate matrix per vertex
    * see:
    * https://gist.github.com/fabianmurariu/fafd14bf96bbb13754a9ded33f59e4ff
    * FIXME we sort of ignore query and just walk the graph from all starting vertices
    */
  def foldQueryWithPaths[M[_], SV[_], V, E](g: GraphDB[M], q: Query)(
      implicit OPS: BuiltInBinaryOps[Boolean],
      M: Matrix[M],
      SV: SparseVector[SV],
      MH: MatrixHandler[M, Int],
      VH: VectorHandler[SV, Int]
  ): TaskManaged[M[Boolean]] = {

    /**
      * since we have to interpret every step differently
      * we need to walk the query here as a stream
      */
    def stepLoop(qs: SStream[Query])(
        frontier: M[Int],
        dag: M[Boolean],
        v: SV[Int],
        notInV: Seq[Int]
    ): Task[Unit] = qs match {
      case EdgesTyped(types, direction) #:: tail =>
        val a = g.edgeMatrixForTypes(types, direction == Out)

        // create the diagonal of "not in v" elements
        val notIVsIdx = (for {
          i <- notInV
          if VH.get(v)(i).isEmpty
        } yield i).toArray

        IO.unit
      case s if s.isEmpty => IO.unit
    }

    val qStream = q.toStream

    // determine starting point
    qStream.head match {
      case AllVertices =>
        Stream.emits(0L until g.count).evalMap { vId =>
          val useMe = for {
            frontier <- Matrix[M].make[Int](16, 16)
            dag <- Matrix[M].make[Boolean](16, 16)
            v <- SparseVector[SV].make[Int](16)
          } yield (frontier, dag, v)

          useMe.use {
            case (f, d, v) =>
              M.set(f)(vId, vId, 1)
              SV.set(v)(vId, 1)
              stepLoop(qStream.tail)(f, d, v, 0 until 16)
          }
        }
    }

    ???
  }

  def foldQueryPath[M[_]: Matrix, V, E](g: GraphDB[M], q: Query)(
      implicit OPS: BuiltInBinaryOps[Boolean]
  ): TaskManaged[M[Boolean]] = {

    def loop(
        semiring: GrBSemiring[Boolean, Boolean, Boolean],
        q: Query
    ): TaskManaged[M[Boolean]] =
      q match {
        case AllVertices => lift(g.nodes)
        case AllEdgesOut => lift(g.edgesOut)
        case AllEdgesIn  => lift(g.edgesIn)
        case VertexWithLabels(labels) =>
          g.vectorMatrixForLabels(labels)
        case EdgesTyped(types, Out) =>
          g.edgeMatrixForTypes(types, true)
        case EdgesTyped(types, In) =>
          g.edgeMatrixForTypes(types, false)
        case AndThen(cur, next) =>
          for {
            left <- loop(semiring, cur)
            right <- loop(semiring, next)
            out <- MxM[M].mxmNew(semiring)(left, right)
          } yield out
      }

    for {
      add <- GrBMonoid[Boolean](OPS.any, true)
      semiring <- GrBSemiring[Boolean, Boolean, Boolean](add, OPS.pair)
      out <- loop(semiring, q)
    } yield out
  }

  def matAsVertices[M[_]](
      mat: TaskManaged[M[Boolean]]
  )(g: GraphDB[M]): Task[VerticesRes] =
    mat.use { m => IO.effect(VerticesRes(g.labeledVertices(m))) }

  //TODO: figure this out later
  def matAsPaths[M[_], V, E](
      mat: TaskManaged[M[Boolean]]
  )(g: GraphDB[M]): Task[PathRes] = IO.effect(PathRes(Vector(Path(0))))

  def matAsEdges[M[_], V, E](
      mat: TaskManaged[M[Boolean]]
  )(g: GraphDB[M]): Task[EdgesRes] =
    mat.use { m => IO.effect(EdgesRes(g.renderEdges(m))) }

  def createNode(label: String, labels: String*) = {
    Free.liftF(CreateNode((label +: labels).toVector))
  }

  def createNodes(head: Vector[String], tail: Vector[String]*) = {
    Free.liftF(CreateNodes((head +: tail).toVector))
  }

  def createEdges(head: (Long, String, Long), tail: (Long, String, Long)*) = {
    Free.liftF(CreateEdges((head +: tail).toVector))
  }

  def createEdge(src: Long, tpe: String, dest: Long) = {
    Free.liftF(CreateEdge(src, tpe, dest))
  }

  def query(q: Query) =
    Free.liftF(QueryStep(q))

  def path(q: Query) =
    Free.liftF(PathStep(q))

  def vs = AllVertices

  def vs(label: String, labels: String*) =
    VertexWithLabels((label +: labels).toSet)

}
