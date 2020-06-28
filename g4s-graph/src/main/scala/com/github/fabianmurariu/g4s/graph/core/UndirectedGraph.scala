package com.github.fabianmurariu.g4s.graph.core
import cats.Functor
import cats.implicits._
import cats.Applicative
import cats.Monad
import cats.Traverse

/**
  *
  * Simple undirected graph
  *
  * In an undirected graph edges point both ways
  * if A -> B then B -> A
  */
trait UndirectedGraph[F[_], G] { self =>

  def neighbours(fg: F[G])(vs: Int): F[Traversable[Int]]

  /**
    * Inserts a node
    * If the node is already present this function does nothing
    *
    * @param fg
    * effectfull graph
    * @param v
    * node to insert
    * @return
    * the graph
    */
  def insertNode(fg: F[G])(id: Int): F[G]

  /**
    * creates and edge if the src or dst does not exit this does nothing
    *
    * @param fg
    * effectfull graph
    * @param src
    * @param dst
    * @return
    * the graph
    */
  def insertEdge(fg: F[G])(src: Int, dst: Int): F[G]

  /**
    * Unlike insert edge this creates the src and dst vertices
    * if they do not exist
    *
    * @param fg
    * effectfull graph
    * @param src
    * @param dst
    * @return
    *  the graph
    */
  def insertTuple(fg: F[G])(src: Int, dst: Int): F[G] = {
    insertEdge(insertNode(insertNode(fg)(src))(dst))(src, dst)
  }

  /**
    * number of vertices
    *
    * @param fg
    * @return
    */
  def orderG(fg: F[G]): F[Int]

  /**
    * number of edges
    *
    * @param fg
    * @return
    */
  def sizeG(fg: F[G]): F[Int]

  /**
    * Number of edges incident to v
    *
    * @param fg
    * @param v
    * @return
    */
  def degree(fg: F[G])(v: Int): F[Int]

  def dfs(fg: F[G])(v: Int)(implicit M: Monad[F]) = {

    M.iterateUntilM((List(v), Map[Int, Option[Int]](v -> None))) {
      case (Nil, history) =>
        M.pure((Nil, history)) // to ward off the warning but won't hit
      case (parent :: tail, history) =>
        self
          .neighbours(fg)(parent)
          .map {
            _.foldLeft(tail, history) {
              case ((t, h), c) if !history.contains(c) => // node not seen
                (c :: t, h + (c -> Some(parent)))
              case (orElse, _) => orElse // when node is already in instory
            }
          }
    } { case (stack, _) => stack.isEmpty }

  }
}

object UndirectedGraph {

  type AdjacencyMap = Map[Int, Set[Int]]
  type DfsTree = Map[Int, Option[Int]] // output from dfs node -> parent

  /**
    * Instance of Undirected graph under any F[_]
    * for AdjacencyMap
    *
    * @return
    */
  implicit def immutableAdjacencyMap[F[_]: Functor]
      : UndirectedGraph[F, AdjacencyMap] =
    new UndirectedGraph[F, AdjacencyMap] {

      private def degree0(g: AdjacencyMap, v: Int): Int = {
        val edges = g.getOrElse(v, Set.empty)
        if (edges(v))
          edges.size + 1
        else
          edges.size
      }

      override def degree(fg: F[AdjacencyMap])(v: Int): F[Int] =
        fg.map { degree0(_, v) }

      override def orderG(fg: F[AdjacencyMap]): F[Int] =
        fg.map(_.size)

      override def sizeG(fg: F[AdjacencyMap]): F[Int] = fg.map { g =>
        val allDegrees = g.keysIterator.map(degree0(g, _)).sum
        allDegrees / 2
      }

      def neighbours(
          fg: F[AdjacencyMap]
      )(vs: Int): F[Traversable[Int]] = {
        fg.map { g => g.getOrElse(vs, Traversable.empty) }
      }

      def insertNode(fg: F[AdjacencyMap])(v: Int): F[AdjacencyMap] = fg.map {
        g =>
          g.get(v) match {
            case None =>
              g + (v -> Set.empty)
            case Some(vOld) =>
              g
          }

      }

      override def insertEdge(
          fg: F[AdjacencyMap]
      )(src: Int, dst: Int): F[AdjacencyMap] = fg.map { g =>
        val op = for {
          srcEdges <- g.get(src)
          dstEdges <- g.get(dst)
        } yield g +
          (src -> (srcEdges + dst)) +
          (dst -> (dstEdges + src))

        op.getOrElse(g)
      }

    }

  def default[F[_]: Applicative]: F[AdjacencyMap] =
    Applicative[F].pure(Map.empty)

  def fromTuples[F[_]: Applicative](
      ts: Traversable[(Int, Int)]
  )(implicit G: UndirectedGraph[F, AdjacencyMap]): F[AdjacencyMap] =
    ts.foldLeft(default[F]) {
      case (g, (src, dst)) => G.insertTuple(g)(src, dst)
    }

  def apply[F[_], G](implicit G: UndirectedGraph[F, G]) = G

  trait Ops[F[_], G] extends Any {
    def self: F[G]

    def insertNode(id: Int)(implicit G: UndirectedGraph[F, G]): F[G] =
      G.insertNode(self)(id)

    def insertEdge(src: Int, dst: Int)(
        implicit G: UndirectedGraph[F, G]
    ): F[G] =
      G.insertEdge(self)(src, dst)
  }

  object ops {
    implicit class OpsImpl[F[_], G](val self: F[G])
        extends AnyVal
        with Ops[F, G]
  }
}
