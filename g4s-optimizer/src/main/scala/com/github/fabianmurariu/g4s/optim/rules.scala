package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import com.github.fabianmurariu.g4s.optim.impls.GetEdgeMatrix
import com.github.fabianmurariu.g4s.matrix.BlockingMatrix
import cats.data.StateT
import cats.effect.Sync
import cats.Monad
import simulacrum.typeclass

sealed trait Rule[F[_]]
    extends PartialFunction[GroupMember[F], StateT[F, EvaluatorGraph[F], List[
      GroupMember[F]
    ]]] {
  def eval(member: GroupMember[F])(
      implicit S: Monad[F]
  ): StateT[F, EvaluatorGraph[F], List[GroupMember[F]]] = {
    StateT
      .inspect[F, EvaluatorGraph[F], Boolean](_ => isDefinedAt(member))
      .flatMap {
        case true  => apply(member)
        case false => StateT.empty
      }
  }
}

trait ImplementationRule[F[_]] extends Rule[F]
trait TransformationRule[F[_]] extends Rule[F]

import com.github.fabianmurariu.g4s.optim.{impls => op}

import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb

class Filter2MxM[F[_]: Sync] extends ImplementationRule[F] {

  override def apply(
      gm: GroupMember[F]
  ): StateT[F, EvaluatorGraph[F], List[GroupMember[F]]] = gm.logic match {
    case Filter(frontier: LogicMemoRef[F], filter: LogicMemoRef[F]) =>
      StateT.inspect { _ =>
        val physical: op.Operator[F] =
          op.MatrixMul[F](
            op.RefOperator[F](frontier),
            op.RefOperator[F](filter)
          )

        val newGM = new GroupMember[F](gm.parent, gm.logic, Some(physical))
        List(newGM)
      }

  }

  override def isDefinedAt(gm: GroupMember[F]): Boolean =
    gm.logic.isInstanceOf[Filter]

}

class Expand2MxM[F[_]: Sync] extends ImplementationRule[F] {

  override def apply(
      gm: GroupMember[F]
  ): StateT[F, EvaluatorGraph[F], List[GroupMember[F]]] = gm.logic match {
    case Expand(from: LogicMemoRef[F], to: LogicMemoRef[F], _) =>
      StateT.inspect { _ =>
        val physical: op.Operator[F] =
          op.MatrixMul[F](op.RefOperator[F](from), op.RefOperator[F](to))

        val newGM = new GroupMember[F](gm.parent, gm.logic, Some(physical))
        List(newGM)
      }

  }

  override def isDefinedAt(gm: GroupMember[F]): Boolean =
    gm.logic.isInstanceOf[Expand]

}

class LoadEdges[F[_]: Sync] extends ImplementationRule[F] {

  def apply(
      gm: GroupMember[F]
  ): StateT[F, EvaluatorGraph[F], List[GroupMember[F]]] =
    gm.logic match {
      case GetEdges((tpe: String) :: _, _, transpose) =>
        StateT.inspectF { g: EvaluatorGraph[F] =>
          g.lookupEdge(tpe, transpose).map {
            case (mat, card) =>
              val physical: op.Operator[F] =
                GetEdgeMatrix[F](new UnNamed, Some(tpe), mat, card)
              List(
                new GroupMember(gm.parent, gm.logic, Some(physical))
              )
          }
        }
    }

  override def isDefinedAt(gm: GroupMember[F]): Boolean = gm.logic match {
    case GetEdges(_ :: _, _, _) => true
    case _                      => false
  }

}

class LoadNodes[F[_]: Sync] extends ImplementationRule[F] {

  def apply(
      gm: GroupMember[F]
  ): StateT[F, EvaluatorGraph[F], List[GroupMember[F]]] =
    gm.logic match {
      case GetNodes((label :: _), _) =>
        StateT.inspectF { g: EvaluatorGraph[F] =>
          g.lookupNodes(label).map {
            case (mat, card) =>
              val physical: op.Operator[F] =
                op.GetNodeMatrix[F](new UnNamed, Some(label), mat, card)
              List(
                new GroupMember(gm.parent, gm.logic, Some(physical))
              )
          }
        }
    }

  override def isDefinedAt(gm: GroupMember[F]): Boolean =
    gm.logic.isInstanceOf[GetNodes]

}

trait EvaluatorGraph[F[_]] {
  implicit def F: Sync[F]
  def lookupEdge(
      tpe: String,
      transpose: Boolean
  ): F[(BlockingMatrix[F, Boolean], Long)]
  def lookupNodes(tpe: String): F[(BlockingMatrix[F, Boolean], Long)]
}
