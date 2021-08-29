package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.optim.impls.GetEdgeMatrix
import com.github.fabianmurariu.g4s.matrix.BlockingMatrix
import cats.effect.IO

sealed abstract class Rule
    extends ((GroupMember, EvaluatorGraph) => IO[List[GroupMember]]) {
  def eval(
      member: GroupMember,
      graph: EvaluatorGraph
  ): IO[List[GroupMember]] = IO.defer{
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

import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb

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
        case GetEdges((tpe: String) :: _, sorted, transpose) =>
          graph.lookupEdges(tpe, transpose).map {
            case (mat, card) =>
              val physical: op.Operator =
                GetEdgeMatrix(sorted, Some(tpe), mat, card)
              List(
                EvaluatedGroupMember(gm.logic, physical)
              )
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
        graph.lookupNodes(label).map {
          case (mat, card) =>
            val physical: op.Operator =
              op.GetNodeMatrix(
                sorted.getOrElse(new UnNamed),
                Some(label),
                mat,
                card
              )
            List(
              EvaluatedGroupMember(gm.logic, physical)
            )
        }
    }
  }

  override def isDefinedAt(gm: GroupMember): Boolean =
    gm.logic.isInstanceOf[GetNodes]

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
}
