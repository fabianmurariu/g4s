package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import com.github.fabianmurariu.g4s.optim.{impls => op}
import scala.collection.immutable.Queue

case class MemoV2(
    rootPlans: Map[Binding, LogicNode],
    queue: Queue[Int] = Queue.empty[Int],
    table: Map[Int, GroupV2] = Map.empty[Int, GroupV2]
)

object MemoV2 {
  import rules2.Rule

  def bestPlan(m: MemoV2): op.Operator = {

    val best = m.rootPlans.keySet
      .collect { case name: Binding => m.rootPlans.get(name).toSeq }
      .flatten
      .flatMap(logic => m.table.get(logic.signature))
      .collect { case GroupV2(_, _, Some(cgm: CostedGroupMember)) => cgm }
      .minBy(_.cost)
      .plan

    deRefOperator(m)(best).get
  }

  def pop(m: MemoV2): Option[(GroupV2, MemoV2)] =
    m.queue.dequeueOption.map {
      case (signature, rest) => m.table(signature) -> m.copy(queue = rest)
    }

  def insertLogic(m: MemoV2)(logic: LogicNode): (MemoV2, GroupV2) = {
    val newGroup = GroupV2(logic)
    insertGroup(m)(newGroup)
  }

  def updateGroup(m: MemoV2)(g: GroupV2) =
    m.copy(table = m.table + (g.logic.signature -> g))

  def insertGroup(m: MemoV2)(newGroup: GroupV2) = {
    val signature = newGroup.logic.signature
    // val newTable = m.table + (signature -> newGroup)
    val newTable = m.table.updatedWith(signature) {
      case None => Some(newGroup)
      case Some(grp) =>
        Some(newGroup.copy(optMember = grp.optMember))
    }
    val newQueue = m.queue.enqueue(signature)
    MemoV2(m.rootPlans, newQueue, newTable) -> newGroup
  }

  def deRef(m: MemoV2)(ref: LogicMemoRefV2): GroupV2 =
    m.table(ref.signature)

  def doEnqueuePlan(m: MemoV2)(logic: LogicNode): (MemoV2, GroupV2) = {
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

    val processedNewMembers = Vector.newBuilder[GroupMember]

    val (newMemo, members) = newMembers.foldLeft((m, processedNewMembers)) {
      case ((memo, b), UnEvaluatedGroupMember(logic, _)) =>
        val (m, grp) = doEnqueuePlan(memo)(logic)
        (m, b += UnEvaluatedGroupMember(grp.logic))
      case ((m, b), g: PhysicalPlanMember) =>
        (m, b += g)
    }

    val newGroup = g.copy(equivalentExprs = members.result())

    val (memo, _) =
      GroupV2.optimGroup(newGroup, MemoV2.updateGroup(newMemo)(newGroup))
    memo
  }

  def optimPhysicalPlan(m: MemoV2)(signature: Int): Option[op.Operator] = {
    val group = m.table(signature) // yes we can blow up if we don't have the signature
    // GroupV2.optim(group, m).optMember.map(_.plan).flatMap(deRef)
    group.optMember.collect {
      case CostedGroupMember(_, plan, _, _) => deRefOperator(m)(plan)
    }.flatten

  }

  def deRefOperator(m: MemoV2)(physical: op.Operator): Option[op.Operator] =
    physical match {

      case op.ExpandMul(from: op.RefOperator, to: op.RefOperator, sel) =>
        for {
          optimFrom <- optimPhysicalPlan(m)(from.signature)
          optimTo <- optimPhysicalPlan(m)(to.signature)
        } yield op.ExpandMul(optimFrom, optimTo, sel)

      case op.FilterMul(from: op.RefOperator, to: op.RefOperator, sel) =>
        for {
          optimFrom <- optimPhysicalPlan(m)(from.signature)
          optimTo <- optimPhysicalPlan(m)(to.signature)
        } yield op.FilterMul(optimFrom, optimTo, sel)

      case op.FilterMul(
          from: op.RefOperator,
          op.Diag(inner: op.RefOperator),
          sel
          ) =>
        for {
          optimFrom <- optimPhysicalPlan(m)(from.signature)
          optimInner <- optimPhysicalPlan(m)(inner.signature)
        } yield op.FilterMul(optimFrom, op.Diag(optimInner), sel)
      case op.Diag(inner: op.RefOperator) =>
        optimPhysicalPlan(m)(inner.signature).map(op.Diag(_))
      case op => Option(op)
    }

}
