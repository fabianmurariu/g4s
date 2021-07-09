package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import com.github.fabianmurariu.g4s.optim.impls.GetEdgeMatrix
import com.github.fabianmurariu.g4s.graph.BlockingMatrix
import cats.data.StateT
import com.github.fabianmurariu.g4s.graph.ConcurrentDirectedGraph
import cats.effect.Sync

sealed trait Rule[F[_]]
    extends PartialFunction[GroupMember[F], StateT[F, EvaluatorGraph[F], List[
      GroupMember[F]
    ]]]

trait ImplementationRule[F[_]] extends Rule[F]
trait TransformationRule[F[_]] extends Rule[F]

import com.github.fabianmurariu.g4s.optim.{impls => op}

import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb

class Expand2MxM[F[_]: Sync] extends ImplementationRule[F] {

  override def apply(
      gm: GroupMember[F]
  ): StateT[F, EvaluatorGraph[F], List[GroupMember[F]]] = gm.logic match {
    case Expand(from: LogicMemoRef[F], to: LogicMemoRef[F], _) =>
      StateT.inspect { _ =>
        val physical: op.Operator[F] =
          op.Expand[F](op.RefOperator[F](from), op.RefOperator[F](to))

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
    case GetEdges(tpe, _, _) => tpe.size == 1
  }

}

trait EvaluatorGraph[F[_]] {
  implicit def F: Sync[F]
  def lookupEdge(
      tpe: String,
      transpose: Boolean
  ): F[(BlockingMatrix[F, Boolean], Long)]
  def lookupNodes(tpe: String): F[(BlockingMatrix[F, Boolean], Long)]
}

object EvaluatorGraph {
  def apply[F[_], V, E](
      graph: ConcurrentDirectedGraph[F, V, E]
  )(implicit S: Sync[F]): F[EvaluatorGraph[F]] =
    Sync[F].delay {
      new EvaluatorGraph[F] {

        override implicit def F: Sync[F] = S

        override def lookupNodes(
            tpe: String
        ): F[(BlockingMatrix[F, Boolean], Long)] = {
          graph.lookupNodes(tpe)
        }

        override def lookupEdge(
            tpe: String,
            transpose: Boolean
        ): F[(BlockingMatrix[F, Boolean], Long)] = {
          graph.lookupEdges(tpe, transpose)
        }

      }
    }
}
