package com.github.fabianmurariu.g4s.optim.rules

import com.github.fabianmurariu.g4s.optim.GroupMember
import com.github.fabianmurariu.g4s.optim.StatsStore

abstract class Rule extends ((GroupMember, StatsStore) => List[GroupMember]) { self =>

  def eval(member: GroupMember, stats: StatsStore): List[GroupMember] =
    if (isDefinedAt(member) && !member.appliedRules(self.getClass()))
      apply(member, stats)
    else
      List.empty

  def isDefinedAt(gm: GroupMember): Boolean
}

trait ImplementationRule extends Rule
trait TransformationRule extends Rule