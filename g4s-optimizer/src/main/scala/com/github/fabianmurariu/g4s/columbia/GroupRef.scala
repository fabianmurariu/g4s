package com.github.fabianmurariu.g4s.columbia

import com.github.fabianmurariu.g4s.optim.impls.PhysicalGroupRef
import com.github.fabianmurariu.g4s.optim.logic.LogicGroupRef

trait GroupRef {

  def groupId: Int

}

object GroupRef {
  def logical(id: Int): LogicGroupRef = LogicGroupRef(id)

  def physical(id: Int): PhysicalGroupRef = PhysicalGroupRef(id)
}
