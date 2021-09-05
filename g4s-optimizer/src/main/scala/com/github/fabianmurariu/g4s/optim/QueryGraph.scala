package com.github.fabianmurariu.g4s.optim

import org.opencypher.v9_0.ast
import org.opencypher.v9_0.parser.CypherParser
import org.opencypher.v9_0.util.OpenCypherExceptionFactory
import org.opencypher.v9_0.expressions.{RelationshipChain, Pattern}
import org.opencypher.v9_0.expressions.PatternElement
import org.opencypher.v9_0.expressions.NodePattern
import org.opencypher.v9_0.expressions.RelationshipPattern
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.Variable
import DirectedGraph.ops._
import scala.util.control.NonFatal

class QueryGraph(
    val returns: Set[Binding]
) extends MutableGraph[Node, Edge]() {
  def isReturn(n: Node) = n.name match {
    case b: Binding => returns(b)
    case _          => false
  }
}

object QueryGraph {

  def fromCypherText(cypherText: String): Either[Throwable, QueryGraph] =
    defaultParse(cypherText).map(fromAST)

  def fromAST(expr: ast.Statement): QueryGraph = expr match {
    case ast.Query(
        _,
        ast.SingleQuery(
          ast.Match(_, Pattern(paths), _, _) :: ast
            .Return(_, ast.ReturnItems(_, roots, _), _, _, _, _) :: _
        )
        ) =>
      paths
        .map(_.element)
        .foldLeft(QueryGraph(roots.map(item => Binding(item.name)).toSet)) {
          (g, elem) =>
            patternToQueryGraph(g, elem)
            g
        }

  }

  def apply(roots: Set[Binding]) =
    new QueryGraph(roots)

  def patternToQueryGraph(
      g: QueryGraph,
      element: PatternElement
  ): Node = element match {
    case NodePattern(None, labels, _) =>
      val node = Node(new UnNamed)(labels.map(_.name))
      g.insert(node)
      node
    case NodePattern(Some(Variable(name)), labels, _) =>
      val node = Node(Binding(name))(labels.map(_.name))
      val updateNode = g.get(node) match {
        case Some(v) =>
          v.labels = node.labels ++ v.labels
          v
        case None => node
      }
      g.insert(updateNode)
      updateNode
    case RelationshipChain(
        left,
        RelationshipPattern(_, tpes, _, _, direction, _),
        right
        ) =>
      val leftNode = patternToQueryGraph(g, left)
      val rightNode = patternToQueryGraph(g, right)

      direction match {
        case SemanticDirection.OUTGOING =>
          g.edge(leftNode, Edge(Out, tpes.map(_.name)), rightNode)
        case SemanticDirection.INCOMING =>
          In
          g.edge(rightNode, Edge(In, tpes.map(_.name)), leftNode)
      }

      rightNode
  }

  def defaultParse(query: String): Either[Throwable, ast.Statement] =
    try {
      val ast_ = CypherParser.parseOrThrow(
        query,
        OpenCypherExceptionFactory(None),
        None,
        CypherParser.Statements
      )
      Right(ast_)
    } catch {
      case NonFatal(e) =>
        Left(e)
    }

}

case class Node(name: Name)(var labels: Seq[String] = Seq.empty)
case class Edge(direction: Direction, types: Seq[String])
