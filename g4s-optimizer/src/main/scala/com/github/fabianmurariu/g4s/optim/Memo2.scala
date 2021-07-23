package com.github.fabianmurariu.g4s.optim
import zio._

class Memo2(stack: Stack[Group[Task]]) extends IMemo[Task] {

  def pop: Task[Option[Group[Task]]] = stack.pop
}

class Stack[A](stack: Ref[List[A]]) {

  def pop: Task[Option[A]] = stack.modify {
    case head :: next => (Some(head), next)
    case Nil          => (None, Nil)
  }

  def push(a: A): Task[Unit] = stack.update(a :: _)

  def isEmpty: Task[Boolean] = stack.modify(s => (s.isEmpty, s))
}
