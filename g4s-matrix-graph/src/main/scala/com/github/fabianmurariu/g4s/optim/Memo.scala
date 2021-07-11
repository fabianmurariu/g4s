package com.github.fabianmurariu.g4s.optim

import scala.collection.mutable
import cats.effect.Sync
import cats.implicits._
import com.github.fabianmurariu.g4s.optim.{impls => op}
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import org.scalameta.adt.root

class Memo[F[_]: Sync](
    val rootPlans: Map[Binding, LogicNode],
    val table: mutable.LinkedHashMap[Int, Group[F]] =
      mutable.LinkedHashMap.empty[Int, Group[F]],
    var stack: List[Group[F]] = List.empty
) {

  def pop: F[Group[F]] = Sync[F].delay {
    val head :: tail = stack
    stack = tail
    head
  }

  def isDone: F[Boolean] = Sync[F].delay(stack.isEmpty)

  def insertGroup(logic: LogicNode): F[Group[F]] = {
    for {
      _ <- Sync[F].delay(println(s"insertGroup ${logic.signature} -> $logic"))
      newG <- Group(this, logic, table.size)
      g <- Sync[F].delay {
        table.getOrElseUpdate(logic.signature, {
          stack = newG :: stack
          newG
        })
      }
    } yield g

  }

  def doEnqueuePlan(logic: LogicNode): F[Group[F]] = logic match {
    case ref:LogicMemoRef[F] => Sync[F].delay(ref.group)
    case _ => 
    Sync[F].delay(println(s"Do Enqueue Plan $logic")) >> Sync[F].delay(logic.leaf).flatMap {
      case true =>
        insertGroup(logic)
      case false =>
        logic.children.toVector.zipWithIndex
          .foldM(Vector.newBuilder[LogicMemoRef[F]]) {
            case (builder, (child, i)) =>
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

    def optimPhysicalPlan(signature: Int): F[op.Operator[F]] =
      for {
        grp <- Sync[F].delay(table(signature))
        minGroupMember <- {
          println(s"Optim Physical Plan, $grp, ${grp.optMember}")
          grp.optGroupMember
        }
        plan <- minGroupMember.physical
      } yield plan

    val localOptimPlan = for {
      logic <- Sync[F].delay ( rootPlans(name) )
      plan <- optimPhysicalPlan(logic.signature)
    } yield plan

    localOptimPlan.flatMap {
      case _: op.RefOperator[F] =>
        Sync[F].raiseError(
          new IllegalStateException("Local Optimal plan cannot be RefOperator")
        )
      case op.MatrixMul(from: op.RefOperator[F], to: op.RefOperator[F]) =>
        for {
          optimFrom <- optimPhysicalPlan(from.logic.signature)
          optimTo <- optimPhysicalPlan(to.logic.signature)
        } yield op.MatrixMul(optimFrom, optimTo)
      case op => Sync[F].delay(op)
    }

  }

}

object Memo {
  def apply[F[_]: Sync](
      rootPlans: Map[Binding, LogicNode],
      table: mutable.LinkedHashMap[Int, Group[F]] =
        mutable.LinkedHashMap.empty[Int, Group[F]],
      stack: List[Group[F]] = List.empty
  ): F[Memo[F]] = Sync[F].delay {
    new Memo(rootPlans, table, stack)
  }
}
