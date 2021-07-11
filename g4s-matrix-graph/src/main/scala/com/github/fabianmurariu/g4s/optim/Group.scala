package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import cats.data.StateT
import alleycats.std.all._
import cats.effect.Sync

class Group[F[_]: Sync](
    val memo: Memo[F],
    val logic: LogicNode,
    val id: Int,
    var optMember: Option[GroupMember[F]] = None,
    var equivalentExprs: Vector[GroupMember[F]] = Vector.empty
) {

  def appendMember(member: GroupMember[F]): F[Unit] =
    Sync[F].delay {
      equivalentExprs = equivalentExprs :+ member
    }

  def exploreMember(rules: Vector[Rule[F]])(
      member: GroupMember[F]
  ): StateT[F, EvaluatorGraph[F], Vector[GroupMember[F]]] =
    for {
      newMembers <- member.exploreMember(rules)
      _ <- StateT.liftF(newMembers.foldM(()) { (_, newMember) =>
        memo.doEnqueuePlan(newMember.logic).map(_ => ())
      })
    } yield newMembers

  def exploreGroup(
      rules: Vector[Rule[F]]
  ): StateT[F, EvaluatorGraph[F], Unit] = {
    equivalentExprs
      .map(exploreMember(rules))
      .sequence
      .map(_.flatten)
      .flatMapF(newMembers => Sync[F].delay { equivalentExprs = newMembers })
  }

  def optGroupMember =
    Sync[F]
      .defer {
        equivalentExprs
          .asInstanceOf[Iterable[GroupMember[F]]]
          .foldM((Long.MaxValue, Option.empty[GroupMember[F]])) {
            case ((_, None), groupMember) =>
              groupMember.cost.map(cost => cost -> Some(groupMember))
            case ((bestCost, Some(bestGroupMember)), groupMember) =>
              groupMember.cost.map { cost =>
                if (cost < bestCost)
                  cost -> Some(groupMember)
                else
                  bestCost -> Some(bestGroupMember)
              }
          }
      }
      .map(out => out._2.get)
}

object Group {
  def apply[F[_]: Sync](
      memo: Memo[F],
      logic: LogicNode,
      id: Int
  ): F[Group[F]] =
    for {
      group <- Sync[F].delay(new Group(memo, logic, id))
      _ <- group.appendMember(new GroupMember(group, logic))
    } yield group
}
