package com.github.fabianmurariu.g4s.optim

import scala.collection.mutable.ArrayBuffer

abstract class LogicNode(
    cs: ArrayBuffer[LogicNode] = ArrayBuffer.empty // FIXME: this could change in the future
) extends TreeNode[LogicNode](cs) { self =>
  // the left most binding
  def sorted: Option[Name]
  // the binding outputs of the logical operator
  def output: Seq[Name]

  def deRef: LogicNode = self match {
    case LogicMemoRef(group) => group.logic
    case node => node
  }

  val signature: String = this match {
    case LogicMemoRef(group) => group.logic.signature
    case node:GetNodes => node.toString()
    case node:GetEdges => node.toString()
    case Expand(from, to, transpose) =>
      s"Expand(${from.signature},${to.signature},$transpose)"
    case Filter(frontier, filter) =>
      s"Filter(${frontier.signature},${filter.signature})"
  }


}

sealed abstract class ForkNode(
    cs: ArrayBuffer[LogicNode] = ArrayBuffer.empty
) extends LogicNode(cs) {

  def rewire[F[_]](children:Vector[LogicMemoRef[F]]): LogicNode
}

case class Return(rets: LogicNode*) extends LogicNode(rets.to(ArrayBuffer)) {

  override def sorted: Option[Name] = None

  override def output: Seq[Name] =
    rets.map(_.output).reduce(_ ++ _)

}

case class GetNodes(label: Seq[String], sorted: Option[Name] = None)
    extends LogicNode(ArrayBuffer.empty[LogicNode]) {
  def output: Seq[Name] = sorted.toSeq
}

case class GetEdges(tpe: Seq[String], sorted: Option[Name] = None, transpose:Boolean = false)
    extends LogicNode(ArrayBuffer.empty[LogicNode]) {
  def output: Seq[Name] = Seq.empty
}

// (from)<-[:to]-
case class Expand(from: LogicNode, to: LogicNode, transposed: Boolean)
    extends ForkNode(ArrayBuffer(from, to)) {

  override def rewire[F[_]](children: Vector[LogicMemoRef[F]]): LogicNode = 
      Expand(children(0), children(1), transposed)

  def sorted: Option[Name] = from.sorted
  def output: Seq[Name] = to.output
}

// terminate expansions by filtering the output nodes
case class Filter(frontier: LogicNode, filter: LogicNode)
    extends ForkNode(ArrayBuffer(frontier, filter)) {
  def sorted: Option[Name] = frontier.sorted
  def output: Seq[Name] = filter.output

  override def rewire[F[_]](children: Vector[LogicMemoRef[F]]): LogicNode = 
      Filter(children(0), children(1))
}

// join multiple logic nodes
case class JoinPath(
    left: LogicNode, // this is the expr to hold on to
    right: LogicNode,
    on: Option[Name]
) extends ForkNode(ArrayBuffer(left, right)) {
  def output: Seq[Name] = Seq(left, right).map(_.output).reduce(_ ++ _)
  def sorted: Option[Name] = right.sorted

  override def rewire[F[_]](children: Vector[LogicMemoRef[F]]): LogicNode = 
      JoinPath(children(0), children(1), on)
}

// join into a top node
case class JoinFork(
    to: LogicNode, // root of this tree
    cs: Seq[LogicNode] // children
) extends ForkNode(to +: cs.to(ArrayBuffer)) {

  override def rewire[F[_]](children: Vector[LogicMemoRef[F]]): LogicNode = 
      JoinFork(children(0), children.tail)

  override def sorted: Option[Name] = to.sorted

  override def output: Seq[Name] = children.flatMap(_.output).toSeq

}

case class Diag(node: LogicNode) extends ForkNode(ArrayBuffer(node)) {

  override def sorted: Option[Name] = node.output.headOption

  override def output: Seq[Name] = node.output

  override def rewire[F[_]](children: Vector[LogicMemoRef[F]]): LogicNode = 
      Diag(children(0))
}

case class LogicMemoRef[F[_]](group: Group[F])
    extends LogicNode(ArrayBuffer(group.logic)) {

  override def sorted: Option[Name] = plan.sorted

  override def output: Seq[Name] = plan.output

  def plan: LogicNode = children.next()
}

object LogicNode {

  import scala.collection.mutable

  def fromQueryGraph(
      qg: QueryGraph
  )(start: Binding): Either[Throwable, LogicNode] = {

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
      val from = dfsInner(child, seen + root)
      val t = transposed(child, root)
      def edges = GetEdges(edge.types, None, t)

      val to = GetNodes(root.labels, Some(root.name))

      Filter(Expand(from, edges, t), to)
    }

    def logicalJoinPath(
        child: Node,
        edge: Edge,
        root: Node,
        seen: mutable.Set[Node]
    ) = {
      val from = dfsInner(child, seen + root)
      val t = transposed(child, root)
      val to = GetNodes(root.labels, Some(root.name))
      val edges = GetEdges(edge.types, None, t)

      val right = Filter(Expand(Diag(from), edges, true), to)
      JoinPath(left = from, right = right, on = right.sorted)
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
          val to = GetNodes(root.labels, Some(root.name))

          val childrenExpr = cs.map {
            case (child, edge) if qg.isReturn(child) =>
              logicalJoinPath(child, edge, root, seen)
            case (child, edge) if !qg.isReturn(child) =>
              logicalExpand(child, edge, root, seen)
          }

          JoinFork(
            to = to,
            cs = childrenExpr.toVector
          )
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
