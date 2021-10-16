package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import com.github.fabianmurariu.g4s.optim.{impls => op}
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import java.util.concurrent.ConcurrentHashMap
import cats.effect.kernel.Ref
import cats.effect.IO
import scala.collection.immutable.Queue

case class MemoV2(
    rootPlans: Map[Binding, LogicNode],
    queue: Queue[GroupV2] = Queue.empty[GroupV2],
    table: Map[String, GroupV2] = Map.empty[String, GroupV2]
)

object MemoV2 {
  import rules2.Rule

  def bestPlan(m: MemoV2): op.Operator =
    m.rootPlans.keySet
      .collect { case name: Binding => physical(m)(name).toSeq }
      .flatten
      .minBy(op => op.cardinality)

  def pop(m: MemoV2): Option[(GroupV2, MemoV2)] =
    m.queue.dequeueOption.map {
      case (group, rest) => group -> m.copy(queue = rest)
    }

  def insertLogic(m: MemoV2)(logic: LogicNode): (MemoV2, GroupV2) = {
    val newGroup = GroupV2(logic)
    insertGroup(m)(newGroup)
  }

  def insertGroup(m: MemoV2)(newGroup: GroupV2) = {
    val newTable = m.table + (newGroup.logic.signature -> newGroup)
    val newQueue = m.queue.enqueue(newGroup)
    MemoV2(m.rootPlans, newQueue, newTable) -> newGroup
  }

  def deRef(m: MemoV2)(ref: LogicMemoRefV2): GroupV2 =
    m.table(ref.signature)

  def doEnqueuePlan(m: MemoV2)(logic: LogicNode): (MemoV2, GroupV2) = {
    // println(s"ENQUEUE ${logic}")
    logic match {
      case ref: LogicMemoRefV2 => m -> deRef(m)(ref)

      case node: ForkNode =>
        val (memo, childrenRef) =
          logic.children.foldLeft((m, Vector.newBuilder[LogicMemoRefV2])) {
            case ((memo, children), child) =>
              val (newMemo, childGroup) = doEnqueuePlan(memo)(child)
              (newMemo, children += LogicMemoRefV2(childGroup.logic))
          }
        val newNode = node.rewireV2(childrenRef.result())
        insertLogic(memo)(newNode)

      case node if node.leaf =>
        insertLogic(m)(node)

    }
  }

  def exploreGroup(
      m: MemoV2
  )(g: GroupV2, rules: Vector[Rule], ss: StatsStore): MemoV2 = {
    val newMembers =
      g.equivalentExprs.flatMap(exprs => exprs.exploreMemberV2(rules, ss))
    // println(s"NEW MEMBERS ${g.logic.signature} -> ${newMembers}")
    val newMemo = newMembers.foldLeft(m) {
      case (memo, newMember: UnEvaluatedGroupMember) =>
        val (newMemo, _) = doEnqueuePlan(memo)(newMember.logic)
        newMemo
      case (memo, _: EvaluatedGroupMember) =>
        memo
    }
    updateMembers(newMemo)(g, newMembers)
  }

  def updateMembers(
      m: MemoV2
  )(g: GroupV2, newMembers: Vector[GroupMember]): MemoV2 = {
    val newTable =
      m.table + (g.logic.signature -> g.copy(equivalentExprs = newMembers))
    m.copy(table = newTable)
  }

  def physical(
      m: MemoV2
  )(name: Binding): Option[op.Operator] = {

    def optimPhysicalPlan(signature: String): Option[op.Operator] = {
      val group = m.table(signature) // yes we can blow up if we don't have the signature
      GroupV2.optim(group).optMember.map(_.plan).flatMap(deRef)
    }

    def deRef(physical: op.Operator): Option[op.Operator] = physical match {
      // case _: op.RefOperator =>
      //   throw new IllegalStateException(
      //     "Local Optimal plan cannot be RefOperator"
      //   )

      case op.ExpandMul(from: op.RefOperator, to: op.RefOperator, sel) =>
        for {
          optimFrom <- optimPhysicalPlan(from.signature)
          optimTo <- optimPhysicalPlan(to.signature)
        } yield op.ExpandMul(optimFrom, optimTo, sel)

      case op.FilterMul(from: op.RefOperator, to: op.RefOperator, sel) =>
        for {
          optimFrom <- optimPhysicalPlan(from.signature)
          optimTo <- optimPhysicalPlan(to.signature)
        } yield op.FilterMul(optimFrom, optimTo, sel)

      case op.FilterMul(
          from: op.RefOperator,
          op.Diag(inner: op.RefOperator),
          sel
          ) =>
        for {
          optimFrom <- optimPhysicalPlan(from.signature)
          optimInner <- optimPhysicalPlan(inner.signature)
        } yield op.FilterMul(optimFrom, op.Diag(optimInner), sel)
      case op => Option(op)
    }

    for {
      rootPlan <- m.rootPlans.get(name)
      linkedPhysical <- optimPhysicalPlan(rootPlan.signature)
      phys <- deRef(linkedPhysical)
    } yield phys
  }

}

class Memo(
    val rootPlans: Map[Binding, LogicNode],
    val stack: Ref[IO, Queue[Group]],
    val table: ConcurrentHashMap[String, Group]
) {
  def doEnqueuePlan(logic: LogicNode): IO[Group] = logic match {
    case ref: LogicMemoRef => IO.delay(ref.group)
    case _ =>
      IO.delay(println(s"ENQUEUE $logic")) *> IO.delay(logic.leaf).flatMap {
        case true =>
          insertGroup(logic)
        case false =>
          logic.children.toVector
            .foldM(Vector.newBuilder[LogicMemoRef]) {
              case (builder, child) =>
                doEnqueuePlan(child).flatMap(group =>
                  IO.delay {
                    builder += LogicMemoRef(group)
                  }
                )
            }
            .map { cs => logic.asInstanceOf[ForkNode].rewire(cs.result()) }
            .flatMap(insertGroup(_))
      }
  }
  def pop: IO[Option[Group]] =
    stack.modify {
      _.dequeueOption match {
        case None               => (Queue.empty, None)
        case Some((head, rest)) => (rest, Some(head))
      }
    }

  def isDone: IO[Boolean] = stack.modify(list => (list, list.isEmpty))

  def insertGroup(logic: LogicNode): IO[Group] = {
    for {
      newG <- Group(this, logic)
      g <- stack.modify { s =>
        // this is cute because table.put is idempotent
        // so this block can run however many times and it will update the stack exactly once
        table.putIfAbsent(logic.signature, newG)
        (s.enqueue(newG), newG)
      }
    } yield g

  }

  /**
    * From this memo derrive the best
    * physical plan the rules have found
    *
    * start from the root plans and
    * */
  def physical(name: Binding): IO[op.Operator] = {

    def optimPhysicalPlan(signature: String): IO[op.Operator] = {

      for {
        grp <- IO.delay(table.get(signature))
        minGroupMember <- grp.optGroupMember
        refPlan <- IO(minGroupMember.plan)
        plan <- refPlan match {
          case _: op.RefOperator =>
            IO.raiseError(
              new IllegalStateException(
                "Local Optimal plan cannot be RefOperator"
              )
            )
          case op.ExpandMul(from: op.RefOperator, to: op.RefOperator, sel) =>
            for {
              optimFrom <- optimPhysicalPlan(from.signature)
              optimTo <- optimPhysicalPlan(to.signature)
            } yield op.ExpandMul(optimFrom, optimTo, sel)

          case op.FilterMul(from: op.RefOperator, to: op.RefOperator, sel) =>
            for {
              optimFrom <- optimPhysicalPlan(from.signature)
              optimTo <- optimPhysicalPlan(to.signature)
            } yield op.FilterMul(optimFrom, optimTo, sel)

          case op.FilterMul(
              from: op.RefOperator,
              op.Diag(inner: op.RefOperator),
              sel
              ) =>
            for {
              optimFrom <- optimPhysicalPlan(from.signature)
              optimInner <- optimPhysicalPlan(inner.signature)
            } yield op.FilterMul(optimFrom, op.Diag(optimInner), sel)

          case op => IO(op)
        }
      } yield plan
    }

    for {
      logic <- IO.delay(rootPlans(name))
      plan <- optimPhysicalPlan(logic.signature)
    } yield plan

  }

}

object Memo {
  def apply(
      rootPlans: Map[Binding, LogicNode]
  ): IO[Memo] = Ref.of[IO, Queue[Group]](Queue.empty[Group]).map { queue =>
    val table = new ConcurrentHashMap[String, Group]
    new Memo(rootPlans, queue, table)
  }
}
