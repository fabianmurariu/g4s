package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.optim.impls.Operator
import cats.effect.IO

class GroupMember(
    val parent: Group,
    val logic: LogicNode,
    var physic: Option[Operator[IO]] = None
) {

  def memo: Memo = parent.memo

  def exploreMember(rules: Vector[Rule]): Unit = {
    for (rule <- rules) {
      if (rule.isDefinedAt(this)) {
        val newMembers = rule(this)
        for (newMember <- newMembers) {
          val logic = newMember.logic
          // enqueue in memo
          memo.doEnqueuePlan(logic)
          // add to the groups expression list
          parent.appendMember(newMember)
        }
      }
    }
  }
}
