package com.github.fabianmurariu.g4s.optim

import cats.effect.Sync
import cats.implicits._
import com.github.fabianmurariu.g4s.optim.{impls => op}
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import java.util.concurrent.ConcurrentHashMap
import cats.effect.concurrent.Ref

class Memo[F[_]: Sync](
    val rootPlans: Map[Binding, LogicNode],
    val stack: Ref[F, List[Group[F]]],
    val table: ConcurrentHashMap[String, Group[F]]) {

  def pop: F[Option[Group[F]]] =
    stack.modify{
      case head :: tail => (tail, Some(head))
      case Nil => (Nil, None)
    }

  def isDone: F[Boolean] = stack.modify(list => (list, list.isEmpty))

  def insertGroup(logic: LogicNode): F[Group[F]] = {
    for {
      newG <- Group(this, logic)
      g <- stack.modify{s =>
        // this is cute because table.put is idempotent
        // so this block can run however many times and it will update the stack exactly once
        table.putIfAbsent(logic.signature, newG)
        (newG :: s, newG)
      }
    } yield g

  }

  def doEnqueuePlan(logic: LogicNode): F[Group[F]] = logic match {
    case ref: LogicMemoRef[F] => Sync[F].delay(ref.group)
    case _ =>

      Sync[F].delay(println(s"Enqueue $logic")) *> Sync[F].delay(logic.leaf).flatMap {
        case true =>
          insertGroup(logic)
        case false =>
          logic.children.toVector
            .foldM(Vector.newBuilder[LogicMemoRef[F]]) {
              case (builder, child) =>
                doEnqueuePlan(child).flatMap(group =>
                  Sync[F].delay {
                    builder += LogicMemoRef(group)
                  }
                )
            }
            .map { cs => logic.asInstanceOf[ForkNode].rewire[F](cs.result()) }
            .flatMap(insertGroup(_))
      }
  }

  /**
    * From this memo derrive the best
    * physical plan the rules have found
    *
    * start from the root plans and
    * */
  def physical(name: Binding)(implicit G: GRB): F[op.Operator[F]] = {

    def optimPhysicalPlan(signature: String): F[op.Operator[F]] = {

      for {
        grp <- Sync[F].delay(table.get(signature))
        minGroupMember <- grp.optGroupMember
        refPlan <- minGroupMember.plan.pure[F]
        plan <- refPlan match {
          case _: op.RefOperator[F] =>
            Sync[F].raiseError(
              new IllegalStateException(
                "Local Optimal plan cannot be RefOperator"
              )
            )
          case op.ExpandMul(from: op.RefOperator[F], to: op.RefOperator[F]) =>
            for {
              optimFrom <- optimPhysicalPlan(from.logic.signature)
              optimTo <- optimPhysicalPlan(to.logic.signature)
            } yield op.ExpandMul(optimFrom, optimTo)

          case op.FilterMul(from: op.RefOperator[F], to: op.RefOperator[F]) =>
            for {
              optimFrom <- optimPhysicalPlan(from.logic.signature)
              optimTo <- optimPhysicalPlan(to.logic.signature)
            } yield op.FilterMul(optimFrom, optimTo)
          case op => op.pure[F]
        }
      } yield plan
    }

    for {
      logic <- Sync[F].delay(rootPlans(name))
      plan <- optimPhysicalPlan(logic.signature)
    } yield plan

  }

}

object Memo {
  def apply[F[_]: Sync](
      rootPlans: Map[Binding, LogicNode],
  ): F[Memo[F]] = Ref.of(List.empty[Group[F]]).map{ stack =>
    val table = new ConcurrentHashMap[String, Group[F]]
    new Memo(rootPlans, stack, table)
  }
}
