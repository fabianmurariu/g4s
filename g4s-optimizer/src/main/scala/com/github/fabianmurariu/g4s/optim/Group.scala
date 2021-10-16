package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import cats.effect.kernel.Ref
import cats.effect.IO

case class GroupV2(
    logic: LogicNode,
    equivalentExprs: Vector[GroupMember],
    optMember: Option[EvaluatedGroupMember] = None
)

object GroupV2 {
    def apply(logic:LogicNode):GroupV2 =
        GroupV2(logic, Vector(UnEvaluatedGroupMember(logic)))

  def appendMember(g: GroupV2)(member: GroupMember): GroupV2 =
    g.copy(equivalentExprs = member +: g.equivalentExprs)

  def optim(g: GroupV2): GroupV2 =
    g.optMember match {
      case Some(_) => g
      case None =>
        val best = calculateAndSetOptimGroupMember(g)
        g.copy(optMember = Some(best))
    }

  private def calculateAndSetOptimGroupMember(
      g: GroupV2
  ): EvaluatedGroupMember =
    g.equivalentExprs
      .collect { case egm: EvaluatedGroupMember => egm }
      .minBy(_.cost)
}

class Group(
    val memo: Memo,
    val logic: LogicNode,
    val optMember: Ref[IO, Option[EvaluatedGroupMember]],
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
        case (_, gm: EvaluatedGroupMember) =>
          optMember.update {
            case None                                            => Some(gm)
            case Some(currentBest) if gm.cost < currentBest.cost => Some(gm)
            case same                                            => same
          }

        case (
            _,
            ugm: UnEvaluatedGroupMember
            ) => // this is for transformation rules
          memo.doEnqueuePlan(ugm.logic).map(_ => ())
      }
      _ <- equivalentExprs.set(newMembers)
    } yield ()
  }

  def optGroupMember: IO[EvaluatedGroupMember] =
    optMember.get.flatMap {
      case Some(best) => IO(best)
      case None       => calculateAndSetOptimGroupMember
    }

  private def calculateAndSetOptimGroupMember: IO[EvaluatedGroupMember] =
    IO.defer {
        equivalentExprs.get.map {
          _.map { case egm: EvaluatedGroupMember => egm }.minBy(_.cost)
        }
      }
      .flatMap {
        case best => optMember.updateAndGet(_ => Some(best)).map(_.get)
      }
}

object Group {
  def apply(
      memo: Memo,
      logic: LogicNode
  ): IO[Group] =
    for {
      optMember <- Ref[IO].of(Option.empty[EvaluatedGroupMember])
      members <- Ref[IO].of(Vector.empty[GroupMember])
      group <- IO.delay(new Group(memo, logic, optMember, members))
      _ <- group.appendMember(UnEvaluatedGroupMember(logic))
    } yield group
}
