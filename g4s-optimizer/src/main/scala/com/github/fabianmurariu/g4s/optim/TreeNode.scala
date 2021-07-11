package com.github.fabianmurariu.g4s.optim

import scala.collection.mutable.ArrayBuffer

class TreeNode[T](cs: ArrayBuffer[T]) { self =>
  def children: Iterator[T] = cs.iterator

  def childrenCol = cs

  def leaf: Boolean = cs.isEmpty

  def update(i: Int, t: T) =
    cs(i) = t
}

object TreeNode {
  def empty[T] = new TreeNode(ArrayBuffer.empty[TreeNode[T]])

  implicit class TreeOps[T <: TreeNode[T]](val t: TreeNode[T]) {

    def right: TreeNode[T] =
      if (t.leaf) t
      else t.childrenCol.last.right
  }
}
