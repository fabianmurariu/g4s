package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import cats.effect.kernel.Ref
import cats.effect.IO

class Group(
    val memo: Memo,
    val logic: LogicNode,
    val optMember: Ref[IO, Option[GroupMember]],
    val equivalentExprs: Ref[IO, Vector[GroupMember]]
) {

  def appendMember(member: GroupMember): IO[Unit] =
    equivalentExprs.update {
      _ :+ member
    }

  def exploreGroup(
      rules: Vector[Rule],
      graph: EvaluatorGraph
  ): IO[Unit] = {

    for {
      exprs <- equivalentExprs.get
      newMembers <- exprs
        .map(_.exploreMember(rules, graph))
        .sequence
        .map(_.flatten)
      _ <- newMembers.foldM(()) { 
        case (_, _:EvaluatedGroupMember) => IO.unit // we're done with you
        case (_, ugm:UnEvaluatedGroupMember) =>  // this is for transformation rules
        memo.doEnqueuePlan(ugm.logic).map(_ => ())
      }
      _ <- equivalentExprs.set(newMembers)
    } yield ()
  }

  def optGroupMember: IO[EvaluatedGroupMember] =
    IO
      .defer {
        equivalentExprs
        .get.flatMap{_
            .foldM((Long.MaxValue, Option.empty[EvaluatedGroupMember])) {
            case (_, gm: UnEvaluatedGroupMember) =>
              IO.raiseError(
                new IllegalStateException(
                  s"Unable to optimize un-evaluated GroupMember ${gm}"
                )
              )
            case ((_, None), gm: EvaluatedGroupMember) =>
              gm.cost.map(cost => cost -> Some(gm))
            case (
                (bestCost, Some(bestGroupMember)),
                gm: EvaluatedGroupMember
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
  def apply(
      memo: Memo,
      logic: LogicNode
  ): IO[Group] =
    for {
      optMember <- Ref[IO].of(Option.empty[GroupMember])
      members <- Ref[IO].of(Vector.empty[GroupMember])
      group <- IO.delay(new Group(memo, logic, optMember, members))
      _ <- group.appendMember(UnEvaluatedGroupMember(logic))
    } yield group
}
