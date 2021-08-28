package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import com.github.fabianmurariu.g4s.optim.impls.GetEdgeMatrix
import com.github.fabianmurariu.g4s.matrix.BlockingMatrix
import cats.effect.Sync

sealed abstract class Rule[F[_]: Sync]
    extends ((GroupMember[F], EvaluatorGraph[F]) => F[List[GroupMember[F]]]) {
  def eval(
      member: GroupMember[F],
      graph: EvaluatorGraph[F]
  ): F[List[GroupMember[F]]] = Sync[F].suspend {
    if (isDefinedAt(member)) {
      apply(member, graph)
    } else {
      List.empty.pure[F]
    }
  }

  def isDefinedAt(gm: GroupMember[F]): Boolean
}

trait ImplementationRule[F[_]] extends Rule[F]
trait TransformationRule[F[_]] extends Rule[F]

import com.github.fabianmurariu.g4s.optim.{impls => op}

import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb

class Filter2MxM[F[_]: Sync] extends ImplementationRule[F] {

  override def apply(
      gm: GroupMember[F],
      graph: EvaluatorGraph[F]
  ): F[List[GroupMember[F]]] = Sync[F].delay {
    gm.logic match {
      case Filter(frontier: LogicMemoRef[F], filter: LogicMemoRef[F]) =>
        val physical: op.Operator[F] =
          op.FilterMul[F](
            op.RefOperator[F](frontier),
            op.RefOperator[F](filter)
          )

        val newGM: GroupMember[F] = EvaluatedGroupMember(gm.logic, physical)
        List(newGM)

    }
  }

  override def isDefinedAt(gm: GroupMember[F]): Boolean =
    gm.logic.isInstanceOf[Filter]

}

class Expand2MxM[F[_]: Sync] extends ImplementationRule[F] {

  override def apply(
      gm: GroupMember[F],
      graph: EvaluatorGraph[F]
  ): F[List[GroupMember[F]]] = Sync[F].delay {
    gm.logic match {
      case Expand(from: LogicMemoRef[F], to: LogicMemoRef[F], _) =>
        val physical: op.Operator[F] =
          op.ExpandMul[F](op.RefOperator[F](from), op.RefOperator[F](to))

        val newGM = EvaluatedGroupMember(gm.logic, physical)
        List(newGM)

    }
  }

  override def isDefinedAt(gm: GroupMember[F]): Boolean =
    gm.logic.isInstanceOf[Expand]

}

class LoadEdges[F[_]: Sync] extends ImplementationRule[F] {

  def apply(
      gm: GroupMember[F],
      graph: EvaluatorGraph[F]
  ): F[List[GroupMember[F]]] =
    Sync[F].suspend {
      gm.logic match {
        case GetEdges((tpe: String) :: _, sorted, transpose) =>
          graph.lookupEdges(tpe, transpose).map {
            case (mat, card) =>
              val physical: op.Operator[F] =
                GetEdgeMatrix[F](sorted, Some(tpe), mat, card)
              List(
                EvaluatedGroupMember(gm.logic, physical)
              )
          }
      }
    }

  override def isDefinedAt(gm: GroupMember[F]): Boolean =
    gm.logic.isInstanceOf[GetEdges]

}

class LoadNodes[F[_]: Sync] extends ImplementationRule[F] {

  def apply(
      gm: GroupMember[F],
      graph: EvaluatorGraph[F]
  ): F[List[GroupMember[F]]] = Sync[F].suspend {
    gm.logic match {
      case GetNodes((label :: _), sorted) =>
        graph.lookupNodes(label).map {
          case (mat, card) =>
            val physical: op.Operator[F] =
              op.GetNodeMatrix[F](
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

  override def isDefinedAt(gm: GroupMember[F]): Boolean =
    gm.logic.isInstanceOf[GetNodes]

}

trait EvaluatorGraph[F[_]] {
  implicit def F: Sync[F]

  def lookupEdges(
      tpe: Option[String],
      transpose: Boolean
  ): F[(BlockingMatrix[F, Boolean], Long)]

  def lookupEdges(
      tpe: String,
      transpose: Boolean
  ): F[(BlockingMatrix[F, Boolean], Long)] =
    this.lookupEdges(Some(tpe), transpose)

  def lookupNodes(tpe: Option[String]): F[(BlockingMatrix[F, Boolean], Long)]

  def lookupNodes(tpe: String): F[(BlockingMatrix[F, Boolean], Long)] =
    this.lookupNodes(Some(tpe))
}
