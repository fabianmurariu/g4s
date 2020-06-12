package com.github.fabianmurariu.g4s.graph

import cats.free.Free
import cats.{Id, ~>}
import zio._
import fs2.Stream
import com.github.fabianmurariu.g4s.sparse.mutable.Matrix
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.MxM
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps

sealed trait GraphStep[+T]

case class CreateNodes(labels:Vector[Vector[String]]) extends GraphStep[Long]
case class CreateEdges(edges: Vector[(Long, String, Long)]) extends GraphStep[(Long, Long)]

case class CreateNode(labels: Vector[String]) extends GraphStep[Long]
case class CreateEdge(src: Long, tpe: String, dest: Long)
    extends GraphStep[(Long, Long)]
case class QueryStep(q: Query) extends GraphStep[QueryResult]

sealed trait Direction
case object Undirected extends Direction
case object Out extends Direction
case object In extends Direction

sealed trait Query { self =>

  /**
    * (a) -[edgeTypes..]-> (b)
    */
  def out(edgeTypes: String*) = {
    if (edgeTypes.isEmpty) {
      AndThen(self, AllEdgesOut)
    } else {
      AndThen(self, EdgesTyped(edgeTypes.toSet, Out))
    }
  }

  /**
    * (a) <-[edgeTypes..]- (b)
    */
  def in(edgeTypes: String*) = {
    if (edgeTypes.isEmpty) {
      AndThen(self, AllEdgesIn)
    } else {
      AndThen(self, EdgesTyped(edgeTypes.toSet, In))
    }
  }

  /**
    * .. (b: vertexLabel)
    */
  def v(vertexLabels: String*) = {
    if (vertexLabels.isEmpty) {
      AndThen(self, AllVertices)
    } else {
      AndThen(self, VertexWithLabels(vertexLabels.toSet))
    }
  }

  /**
    * shorthand for .out().v(vertexLabel)
    * (a) -> (b:vertexLabel ..)
    */
  def outV(vertexLabel: String, vertexLabels: String*) =
    AndThen(out(), VertexWithLabels((vertexLabel +: vertexLabels).toSet))

  /**
    * shorthand for .in().v(vertexLabel)
    * (a) <- (b:vertexLabel ..)
    */
  def inV(vertexLabel: String, vertexLabels: String*) =
    AndThen(in(), VertexWithLabels((vertexLabel +: vertexLabels).toSet))
}

sealed trait EdgeQuery extends Query
sealed trait VertexQuery extends Query

case object AllVertices extends VertexQuery
case object AllEdges extends EdgeQuery
case object AllEdgesOut extends EdgeQuery
case object AllEdgesIn extends EdgeQuery

case class EdgesTyped(types: Set[String], direction: Direction) extends EdgeQuery
case class VertexWithLabels(labels: Set[String]) extends VertexQuery
case class AndThen(cur: Query, next: Query) extends Query

sealed trait QueryResult
case class VerticesRes(vs: Vector[(Long, Set[String])]) extends QueryResult
case class EdgesRes(es: Vector[(Long, String, Long)]) extends QueryResult

object GraphStep {

  type Step[T] = Free[GraphStep, T]
  type Observable[T] = fs2.Stream[Task, T]

  def interpreter[M[_]: Matrix, V, E](g: GraphDB[M, V, E]) = new (GraphStep ~> Observable) {
    def apply[A](fa: GraphStep[A]): Observable[A] = fa match {

      case CreateNodes(nodes) =>
        Stream.chunk(fs2.Chunk.vector(nodes))
          .chunks
          .evalMap(g.insertVerticesChunk(_))
          .flatMap(c => Stream.chunk(c))

      case CreateEdges(edges) =>
        Stream.chunk(fs2.Chunk.vector(edges))
          .evalMap(g.insertEdge(_))

      case CreateNode(labels) =>
        Stream.eval(
          g.insertVertex(labels)
        )

      case CreateEdge(src, tpe, dest) =>
        Stream.eval(
          g.insertEdge(src, tpe, dest)
        )

      // TODO: we need to know what type is the last step of the path in order to render
      // we should probably use some form of associated types or a Render typeclass
      case QueryStep(q: EdgeQuery) =>
        Stream.eval(matAsEdges(foldQueryPath(g, q))(g))

      case QueryStep(q: VertexQuery) =>
        Stream.eval(matAsVertices(foldQueryPath(g, q))(g))

      case QueryStep(q @ AndThen(_, _: EdgeQuery)) =>
        Stream.eval(matAsEdges(foldQueryPath(g, q))(g))

      case QueryStep(q @ AndThen(_, _: VertexQuery)) =>
        Stream.eval(matAsVertices(foldQueryPath(g, q))(g))
    }

  }

  def lift[G[_], A](g: G[A]): TaskManaged[G[A]] =
    Managed.fromEffect(IO.effect(g))

  def foldQueryPath[M[_]: Matrix, V, E](g: GraphDB[M, V, E], q: Query)(
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

  def matAsVertices[M[_], V, E](
      mat: TaskManaged[M[Boolean]]
  )(g: GraphDB[M, V, E]): Task[VerticesRes] =
    mat.use { m => IO.effect(VerticesRes(g.labeledVertices(m))) }

  def matAsEdges[M[_], V, E](
      mat: TaskManaged[M[Boolean]]
  )(g: GraphDB[M, V, E]): Task[EdgesRes] =
    mat.use { m => IO.effect(EdgesRes(g.renderEdges(m))) }

  def createNode(label: String, labels: String*) = {
    Free.liftF(CreateNode((label +: labels).toVector))
  }

  def createNodes(head: Vector[String], tail:Vector[String]*) = {
    Free.liftF(CreateNodes((head +: tail).toVector))
  }

  def createEdges(head:(Long, String, Long), tail:(Long, String, Long)*) = {
    Free.liftF(CreateEdges((head +: tail).toVector))
  }

  def createEdge(src: Long, tpe: String, dest: Long) = {
    Free.liftF(CreateEdge(src, tpe, dest))
  }

  def query(q: Query) =
    Free.liftF(QueryStep(q))

  def vs = AllVertices

  def vs(label:String, labels:String*) = VertexWithLabels((label +: labels).toSet)

}
