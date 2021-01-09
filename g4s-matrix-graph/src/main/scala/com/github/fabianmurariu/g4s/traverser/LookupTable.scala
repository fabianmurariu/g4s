package com.github.fabianmurariu.g4s.traverser

import scala.collection.mutable
import cats.Monad
import cats.implicits._

class LookupTable[F[_], A](
    table: mutable.Map[LKey, LValue[A]] = mutable.Map.empty[LKey, LValue[A]]
)(implicit F:Monad[F]) extends (LKey => F[LValue[A]]) { self =>


  private def get(l:LKey): F[Option[LValue[A]]] =
    F.pure(table.get(l))

  def lookup(k: LKey): F[Option[A]] =
    get(k).map(_.map(_.plan))

  def lookupOrBuildPlan[T <: A](
      k: LKey)(f: => F[T]): F[T] =
    get(k).flatMap {
      case Some(v) =>
        F.pure(v.plan.asInstanceOf[T])
      case None =>
        f.map{
          plan =>
          table.update(k, LValue(0, plan))
          plan
        }
    }

  override def apply(key: LKey): F[LValue[A]] =
    F.pure(table(key))

  def bind(key:LKey): F[Bind[F, A]] = {
    apply(key).map{
      v =>
      table.update(key, v.copy(rc = v.rc + 1))
      Bind(key)(self)
    }
  }

  def clear(key:LKey):F[Unit] = F.pure{
    table -= key
  }

  def iterator = table.clone().iterator
}

object LookupTable {
  def apply[F[_]:Monad, A]:F[LookupTable[F, A]] =
    Monad[F].pure(new LookupTable[F, A])
}

case class LKey(node: NodeRef, mask: Set[EdgeRef])


case class LValue[T](rc: Int, plan: T)

case class Bind[F[_]:Monad, A](key: LKey)(table: LookupTable[F, A]) {
  def lookup: F[A] = table(key).map(_.plan)
}
