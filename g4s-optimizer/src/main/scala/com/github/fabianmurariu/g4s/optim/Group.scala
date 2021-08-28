package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import alleycats.std.all._
import cats.effect.Sync
import cats.effect.concurrent.Ref

class Group[F[_]: Sync](
    val memo: Memo[F],
    val logic: LogicNode,
    val optMember: Ref[F, Option[GroupMember[F]]],
    val equivalentExprs: Ref[F, Vector[GroupMember[F]]]
) {

  def appendMember(member: GroupMember[F]): F[Unit] =
    equivalentExprs.update {
      _ :+ member
    }

  def exploreGroup(
      rules: Vector[Rule[F]],
      graph: EvaluatorGraph[F]
  ): F[Unit] = {

    for {
      exprs <- equivalentExprs.get
      newMembers <- exprs
        .map(_.exploreMember(rules, graph))
        .sequence
        .map(_.flatten)
      _ <- newMembers.foldM(()) { 
        case (_, egm:EvaluatedGroupMember[F]) => Sync[F].unit // we're done with you
        case (_, ugm:UnEvaluatedGroupMember[F]) =>  // this is for transformation rules
        Sync[F].delay(println(s"New un-Evaluated Member -> $ugm")) *> memo.doEnqueuePlan(ugm.logic).map(_ => ())
      }
      // _ <- Sync[F].delay{
      //   System.exit(1)
      // }
      _ <- equivalentExprs.set(newMembers)
    } yield ()
  }

  def optGroupMember: F[EvaluatedGroupMember[F]] =
    Sync[F]
      .defer {
        equivalentExprs
        .get.flatMap{_
            .foldM((Long.MaxValue, Option.empty[EvaluatedGroupMember[F]])) {
            case (_, gm: UnEvaluatedGroupMember[F]) =>
              Sync[F].raiseError(
                new IllegalStateException(
                  s"Unable to optimize un-evaluated GroupMember ${gm}"
                )
              )
            case ((_, None), gm: EvaluatedGroupMember[F]) =>
              gm.cost.map(cost => cost -> Some(gm))
            case (
                (bestCost, Some(bestGroupMember)),
                gm: EvaluatedGroupMember[F]
                ) =>
              gm.cost.map { cost =>
                if (cost < bestCost)
                  cost -> Some(gm)
                else
                  bestCost -> Some(bestGroupMember)
              }
          }
        }
      }
      .map(out => out._2.get)
}

object Group {
  def apply[F[_]: Sync](
      memo: Memo[F],
      logic: LogicNode
  ): F[Group[F]] =
    for {
      optMember <- Ref.of(Option.empty[GroupMember[F]])
      members <- Ref.of(Vector.empty[GroupMember[F]])
      group <- Sync[F].delay(new Group(memo, logic, optMember, members))
      _ <- group.appendMember(UnEvaluatedGroupMember(logic))
    } yield group
}
