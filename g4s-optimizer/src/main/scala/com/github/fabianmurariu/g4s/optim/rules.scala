package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.optim.impls.GetEdgeMatrix
import com.github.fabianmurariu.g4s.matrix.BlockingMatrix
import cats.effect.IO

sealed abstract class Rule
    extends ((GroupMember, EvaluatorGraph) => IO[List[GroupMember]]) {
  def eval(
      member: GroupMember,
      graph: EvaluatorGraph
  ): IO[List[GroupMember]] = IO.defer {
    if (isDefinedAt(member)) {
      apply(member, graph)
    } else {
      IO(List.empty)
    }
  }

  def isDefinedAt(gm: GroupMember): Boolean
}

trait ImplementationRule extends Rule
trait TransformationRule extends Rule

import com.github.fabianmurariu.g4s.optim.{impls => op}

class Filter2MxM extends ImplementationRule {

  override def apply(
      gm: GroupMember,
      graph: EvaluatorGraph
  ): IO[List[GroupMember]] = IO.delay {
    gm.logic match {
      case Filter(frontier: LogicMemoRef, filter: LogicMemoRef) =>
        val physical: op.Operator =
          op.FilterMul(
            op.RefOperator(frontier),
            op.RefOperator(filter)
          )

        val newGM: GroupMember = EvaluatedGroupMember(gm.logic, physical)
        List(newGM)

    }
  }

  override def isDefinedAt(gm: GroupMember): Boolean =
    gm.logic.isInstanceOf[Filter]

}

class Expand2MxM extends ImplementationRule {

  override def apply(
      gm: GroupMember,
      graph: EvaluatorGraph
  ): IO[List[GroupMember]] = IO.delay {
    gm.logic match {
      case Expand(from: LogicMemoRef, to: LogicMemoRef, _) =>
        val physical: op.Operator =
          op.ExpandMul(op.RefOperator(from), op.RefOperator(to))

        val newGM = EvaluatedGroupMember(gm.logic, physical)
        List(newGM)

    }
  }

  override def isDefinedAt(gm: GroupMember): Boolean =
    gm.logic.isInstanceOf[Expand]

}

class LoadEdges extends ImplementationRule {

  def apply(
      gm: GroupMember,
      graph: EvaluatorGraph
  ): IO[List[GroupMember]] =
    IO.defer {
      gm.logic match {
        case GetEdges((tpe: String) :: _, transpose) =>
          graph.lookupEdges(tpe, transpose).flatMap {
            case (mat, card) =>
              mat.use(_.shape).map { shape =>
                val physical: op.Operator =
                  GetEdgeMatrix(None, Some(tpe), transpose, card)
                List(
                  EvaluatedGroupMember(gm.logic, physical)
                )
              }
          }
      }
    }

  override def isDefinedAt(gm: GroupMember): Boolean =
    gm.logic.isInstanceOf[GetEdges]

}

class LoadNodes extends ImplementationRule {

  def apply(
      gm: GroupMember,
      graph: EvaluatorGraph
  ): IO[List[GroupMember]] = IO.defer {
    gm.logic match {
      case GetNodes((label :: _), sorted) =>
        graph.lookupNodes(label).flatMap {
          case (mat, card) =>
            mat.use(_.shape).map { shape =>
              val physical = op.GetNodeMatrix(
                sorted.getOrElse(new UnNamed),
                Some(label),
                card
              )
              List(
                EvaluatedGroupMember(gm.logic, physical)
              )
            }

        }
    }
  }

  override def isDefinedAt(gm: GroupMember): Boolean =
    gm.logic.isInstanceOf[GetNodes]

}

/**
  *  .. (a)-[]->(b)-[]->(c) return b
  *
  * the tree breaks into 2 branches
  * (a)-[]->(b)
  * (c)<-[]-(b)
  *  *
  * depending on direction these need to be joined on b
  * and/or sorted
  *
  * */
class TreeJoinDiagFilter extends ImplementationRule {

  override def apply(
      gm: GroupMember,
      v2: EvaluatorGraph
  ): IO[List[GroupMember]] =
    gm.logic match {
      case Join(on, Vector(left: LogicMemoRef, right: LogicMemoRef, _*)) =>
        // diag on left FIXME: this is not complete but it's easy to express now
        // probably requires a transformation rule to generate the binary join trees
        // from the initial list
        IO.delay {
          (left.plan, right.plan) match {
            case (
                Filter(front1: LogicMemoRef, _),
                Filter(front2: LogicMemoRef, _)
                ) =>
              List(
                EvaluatedGroupMember(
                  gm.logic,
                  op.FilterMul(
                    op.RefOperator(front1),
                    op.Diag(op.RefOperator(right))
                  )
                ),
                EvaluatedGroupMember(
                  gm.logic,
                  op.FilterMul(
                    op.RefOperator(front2),
                    op.Diag(op.RefOperator(left))
                  )
                )
              )
          }
        }

    }

  override def isDefinedAt(gm: GroupMember): Boolean = gm.logic match {
    case Join(_, Vector(l: LogicMemoRef, r: LogicMemoRef, _*)) =>
      r.plan.isInstanceOf[Filter] && l.plan.isInstanceOf[Filter]
    case _ => false
  }

}

trait EvaluatorGraph {

  def lookupEdges(
      tpe: Option[String],
      transpose: Boolean
  ): IO[(BlockingMatrix[Boolean], Long)]

  def lookupEdges(
      tpe: String,
      transpose: Boolean
  ): IO[(BlockingMatrix[Boolean], Long)] =
    this.lookupEdges(Some(tpe), transpose)

  def lookupNodes(tpe: Option[String]): IO[(BlockingMatrix[Boolean], Long)]

  def lookupNodes(tpe: String): IO[(BlockingMatrix[Boolean], Long)] =
    this.lookupNodes(Some(tpe))

  def statsStore: StatsStore
}
