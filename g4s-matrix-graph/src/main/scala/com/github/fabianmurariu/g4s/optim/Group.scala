package com.github.fabianmurariu.g4s.optim

import scala.collection.mutable.ArrayBuffer

class Group(
    val memo: Memo,
    val logic: LogicNode,
    val id: Int,
    node: LogicNode
) {
  val equivalentExprs: ArrayBuffer[GroupMember] = ArrayBuffer.empty
  appendMember(new GroupMember(this, node))

  def signature: Int = logic.hashCode()

  def appendMember(member: GroupMember) =
    equivalentExprs += member
}
