package com.github.fabianmurariu.g4s.optim

import scala.collection.mutable.ArrayBuffer

abstract class LogicNode(
    cs: ArrayBuffer[LogicNode] = ArrayBuffer.empty
) extends TreeNode[LogicNode](cs) { self =>
  // the binding outputs of the logical operator
  def output: Seq[Name]

  def deRef: LogicNode = self match {
    case LogicMemoRefV2(logic) => logic
    case node                  => node
  }

  // TODO: this needs some thought
  def signature = id.hashCode()

  def id: Set[Any] = this match {
    case LogicMemoRefV2(logic) => logic.id
    case node: GetNodes        => node.label.toSet
    case edges: GetEdges       => edges.tpe.toSet
    case Expand(from, to)   => Set("expand") ++ from.id ++ to.id
    case Filter(frontier, filter) =>
      Set("filter") ++ frontier.id ++ filter.id
    case Join(expr, cont, _) =>
      Set("join") ++ expr.id ++ cont.id
  }
}

sealed abstract class ForkNode(
    cs: ArrayBuffer[LogicNode] = ArrayBuffer.empty
) extends LogicNode(cs) {

  def rewireV2(children: Vector[LogicMemoRefV2]): LogicNode
}

case class GetNodes(label: Option[String], sorted: Option[Name] = None)
    extends LogicNode(ArrayBuffer.empty[LogicNode]) {
  def output: Seq[Name] = sorted.toSeq
}

object GetNodes{
  def apply(label:String, binding:String) =
    new GetNodes(Some(label),Some(Binding(binding)))
}

case class GetEdges(
    tpe: Seq[String],
    transpose: Boolean = false
) extends LogicNode(ArrayBuffer.empty[LogicNode]) {
  def output: Seq[Name] = Seq.empty
}

// (from)<-[:to]-
case class Expand(from: LogicNode, to: LogicNode)
    extends ForkNode(ArrayBuffer(from, to)) {

  override def rewireV2(children: Vector[LogicMemoRefV2]): LogicNode =
    Expand(children(0), children(1))
  def output: Seq[Name] = to.output
}

// terminate expansions by filtering the output nodes
case class Filter(frontier: LogicNode, filter: LogicNode)
    extends ForkNode(ArrayBuffer(frontier, filter)) {
  def output: Seq[Name] = filter.output

  override def rewireV2(children: Vector[LogicMemoRefV2]): LogicNode =
    Filter(children(0), children(1))
}

// join multiple logic nodes
case class Join(
    expr: LogicNode, // this is the expr to hold on to
    cont: LogicNode,
    on: Name
) extends ForkNode(ArrayBuffer(expr, cont)) {
  def output: Seq[Name] = Seq(expr, cont).map(_.output).reduce(_ ++ _)

  override def rewireV2(children: Vector[LogicMemoRefV2]): LogicNode =
    Join(children(0), children(1), on)
}

case class LogicMemoRefV2(logic: LogicNode)
    extends LogicNode(ArrayBuffer(logic)) {

  override def output: Seq[Name] = plan.output

  def plan: LogicNode = children.next()
}

object LogicNode {

  import scala.collection.mutable
  import DirectedGraph.ops._

  def fromQueryGraph(
      qg: QueryGraph
  )(start: Name): Either[Throwable, LogicNode] = {

    def transposed(src: Node, dst: Node): Boolean = {
      val Some((s, _, d)) = qg.getEdge(src, dst)
      !(src == s && dst == d)
    }

    def logicalExpand(
        child: Node,
        edge: Edge,
        root: Node,
        seen: mutable.Set[Node]
    ) = {
      val from = dfsInner(child, seen ++ Set(root))
      val t = transposed(child, root)
      def edges = GetEdges(edge.types, t)

      val to = GetNodes(root.labels, Some(root.name))

      Filter(Expand(from, edges), to)
    }

    def logicalJoinPath(
        child: Node,
        edge: Edge,
        root: Node,
        seen: mutable.Set[Node]
    ) = {
      val from = dfsInner(child, seen ++ Set(root))
      val t = transposed(child, root)
      val to = GetNodes(root.labels, Some(root.name))
      val edges = GetEdges(edge.types, t)

      val right = Filter(Expand(from, edges), to)
      Join(
        expr = from,
        cont = right,
        on = from.output.head
      ) // FIXME: this is questionable
    }

    def dfsInner(
        root: Node,
        seen: mutable.Set[Node]
    ): LogicNode = {
      val children = qg
        .neighbours(root)
        .filterNot { case (other, _) => seen(other) }
        .toVector

      children match {
        case cs if cs.isEmpty => // this root does not have any children
          GetNodes(root.labels, Some(root.name))
        case (child, edge) +: rest if rest.isEmpty && !qg.isReturn(child) =>
          // we only have one edge between the root and the child is not in the return set
          logicalExpand(child, edge, root, seen)
        case (child, edge) +: rest if rest.isEmpty && qg.isReturn(child) =>
          // we only have one edge between the root and the child is in the return set
          logicalJoinPath(child, edge, root, seen)
        case cs =>
          // val to = GetNodes(root.labels, Some(root.name))

          val childrenExpr: Vector[LogicNode] = cs.map {
            case (child, edge) if qg.isReturn(child) =>
              logicalJoinPath(child, edge, root, seen)
            case (child, edge) if !qg.isReturn(child) =>
              logicalExpand(child, edge, root, seen)
          }

          // split into things that can be joined and things that can be
          childrenExpr.tail.foldLeft(childrenExpr.head) {
            case (chain: Filter, next: Filter) =>
              chain.copy(filter = next)
            case (chain: Join, next: Join) =>
              Join(chain, next, chain.on)
            case (chain@ Join(_, cont:Filter, _), next: Filter) =>
              Join(expr = chain, cont = next.copy(filter = cont), on = root.name)
          }

      }
    }

    qg.get(Node(start)()) match {
      case Some(start) =>
        Right(dfsInner(start, mutable.Set.empty[Node]))
      case _ =>
        Left(new IllegalArgumentException(s"Binding: $start not in querygraph"))
    }
  }

}
