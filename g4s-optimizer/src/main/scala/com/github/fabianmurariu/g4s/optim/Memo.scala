package com.github.fabianmurariu.g4s.optim

import cats.implicits._
import com.github.fabianmurariu.g4s.optim.{impls => op}
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import java.util.concurrent.ConcurrentHashMap
import cats.effect.kernel.Ref
import cats.effect.IO
import scala.collection.immutable.Queue

class Memo(
    val rootPlans: Map[Binding, LogicNode],
    val stack: Ref[IO, Queue[Group]],
    val table: ConcurrentHashMap[String, Group]
) {

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

  /**
    * From this memo derrive the best
    * physical plan the rules have found
    *
    * start from the root plans and
    * */
  def physical(name: Binding)(implicit G: GRB): IO[op.Operator] = {

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
          case op.ExpandMul(from: op.RefOperator, to: op.RefOperator) =>
            for {
              optimFrom <- optimPhysicalPlan(from.logic.signature)
              optimTo <- optimPhysicalPlan(to.logic.signature)
            } yield op.ExpandMul(optimFrom, optimTo)

          case op.FilterMul(from: op.RefOperator, to: op.RefOperator) =>
            for {
              optimFrom <- optimPhysicalPlan(from.logic.signature)
              optimTo <- optimPhysicalPlan(to.logic.signature)
            } yield op.FilterMul(optimFrom, optimTo)

          case op.FilterMul(
              from: op.RefOperator,
              op.Diag(inner: op.RefOperator)
              ) =>
            for {
              optimFrom <- optimPhysicalPlan(from.logic.signature)
              optimInner <- optimPhysicalPlan(inner.logic.signature)
            } yield op.FilterMul(optimFrom, op.Diag(optimInner))

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
