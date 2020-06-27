package com.github.fabianmurariu.g4s.graph

/**
 * This should encode queries and be used
 * and produce algebra operations and a query plan
 * to be executed against a Graph
 *
 * person works for Microsoft and likes football
 *
 * val work = nodes("Microsoft")
 * val n = nodes("Person")
 * val sport = nodes("Football")
 *
 * val q = query(
 *     n.out("likes").v(sport),
 *     n.in("works_for").v(microsoft)
 * )
 *
 *
 *
 *
 */
@Deprecated
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
   * Experimental check to add
   * multiple queries joined by one node
   * the starting point is this node
   * and the various queries will be joined into one
   */
  // def join(qs: SubPath*) =
  //   AndThen(self, q)

  /**
    * shorthand for .out().v(vertexLabel)
    * (a) -> (b:vertexLabel ..)
    */
  def outV(vertexLabel: String, vertexLabels: String*) =
    self.out().v((vertexLabel +: vertexLabels):_*)

  /**
    * shorthand for .in().v(vertexLabel)
    * (a) <- (b:vertexLabel ..)
    */
  def inV(vertexLabel: String, vertexLabels: String*) =
    self.in().v((vertexLabel +: vertexLabels):_*)

  // return a stream with a Dfs walk
  def toStream:Stream[Query] = self match {
    case AndThen(left, right) => left.toStream ++ right.toStream
    case q:Query => Stream(q)
  }
}

sealed trait Direction
case object Out extends Direction
case object In extends Direction

sealed trait EdgeQuery extends Query
sealed trait VertexQuery extends Query

case object AllVertices extends VertexQuery
case object AllEdges extends EdgeQuery
case object AllEdgesOut extends EdgeQuery
case object AllEdgesIn extends EdgeQuery

case class EdgesTyped(types: Set[String], direction: Direction) extends EdgeQuery
case class VertexWithLabels(labels: Set[String]) extends VertexQuery
case class AndThen(cur: Query, next: Query) extends Query
case class SubPath(qf: VertexQuery => Query) extends Query

sealed trait QueryResult
case class VerticesRes(vs: Vector[(Long, Set[String])]) extends QueryResult
case class EdgesRes(es: Vector[(Long, String, Long)]) extends QueryResult
// turns out a path is just a vector of edges with a starting edge to cover for the disconnected graph
case class Path(start:Long, es:(Long, Long)*)
case class PathRes(paths: Vector[Path]) extends QueryResult


object Query {

  def vs(vertexLabels: String*) =
    if (vertexLabels.isEmpty) {
      AllVertices
    } else {
      VertexWithLabels(vertexLabels.toSet)
    }

}
