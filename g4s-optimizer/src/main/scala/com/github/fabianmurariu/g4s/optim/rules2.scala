package com.github.fabianmurariu.g4s.optim

object rules2 {

  sealed abstract class Rule
      extends ((GroupMember, StatsStore) => List[GroupMember]) {

    def eval(member: GroupMember, stats: StatsStore): List[GroupMember] =
      if (isDefinedAt(member))
        apply(member, stats)
      else
        List.empty

    def isDefinedAt(gm: GroupMember): Boolean
  }

  trait ImplementationRule extends Rule
  trait TransformationRule extends Rule

  class FilterExpandComutative extends TransformationRule {

    override def apply(gm: GroupMember, v2: StatsStore): List[GroupMember] =
      gm.logic match {
        case Filter(
            LogicMemoRefV2(
              Expand(left: LogicMemoRefV2, right: LogicMemoRefV2, transpose)
            ),
            filter: LogicMemoRefV2
            ) =>
          List(
            UnEvaluatedGroupMember(
              Expand(left, Filter(right, filter), transpose)
            )
          )
      }

    override def isDefinedAt(gm: GroupMember): Boolean = gm.logic match {
      case Filter(
          LogicMemoRefV2(Expand(_: LogicMemoRefV2, _: LogicMemoRefV2, _)),
          _: LogicMemoRefV2
          ) =>
        true && gm.isInstanceOf[UnEvaluatedGroupMember]
      case _ => false
    }

  }

  import com.github.fabianmurariu.g4s.optim.{impls => op}

  class Filter2MxM extends ImplementationRule {

    override def apply(
        gm: GroupMember,
        stats: StatsStore
    ): List[GroupMember] = {
      gm.logic match {
        // case Filter(
        //     frontier@ LogicMemoRefV2(e: GetEdges),
        //     filter @ LogicMemoRefV2(n: GetNodes)
        //     ) =>
        //   val sel = stats.nodeEdgeInSel(e.tpe.head, n.label.head) FIXME: this doesn't work when it does uncomment
        //   val physical: op.Operator =
        //     op.FilterMul(
        //       op.RefOperator(frontier),
        //       op.RefOperator(filter),
        //       sel
        //     )

        //   val newGM: GroupMember = EvaluatedGroupMember(gm.logic, physical)
        //   List(newGM)
        case Filter(
            frontier: LogicMemoRefV2,
            filter @ LogicMemoRefV2(n: GetNodes)
            ) =>
          val sel = stats.nodeSel(n.label.headOption)
          val physical: op.Operator =
            op.FilterMul(
              op.RefOperator(frontier),
              op.RefOperator(filter),
              sel
            )

          val newGM: GroupMember = EvaluatedGroupMember(gm.logic, physical)
          List(newGM)
        case Filter(frontier: LogicMemoRefV2, filter: LogicMemoRefV2) =>
          val physical: op.Operator =
            op.FilterMul(
              op.RefOperator(frontier),
              op.RefOperator(filter),
              1.0d
            )

          val newGM: GroupMember = EvaluatedGroupMember(gm.logic, physical)
          List(newGM)
      }
    }

    override def isDefinedAt(gm: GroupMember): Boolean =
      gm.logic.isInstanceOf[Filter]

  }

  class Expand2MxM extends ImplementationRule {

    override def apply(
        gm: GroupMember,
        stats: StatsStore
    ): List[GroupMember] = {
      gm.logic match {
        case Expand(
            from: LogicMemoRefV2,
            to @ LogicMemoRefV2(e: GetEdges),
            _
            ) =>
          val sel = stats.edgeSel(e.tpe.headOption)

          val physical: op.Operator =
            op.ExpandMul(op.RefOperator(from), op.RefOperator(to), sel)

          val newGM = EvaluatedGroupMember(gm.logic, physical)
          List(newGM)
        case Expand(from: LogicMemoRefV2, to: LogicMemoRefV2, _) =>
          val physical: op.Operator =
            op.ExpandMul(op.RefOperator(from), op.RefOperator(to))

          val newGM = EvaluatedGroupMember(gm.logic, physical)
          List(newGM)

      }
    }

    override def isDefinedAt(gm: GroupMember): Boolean =
      gm.logic.isInstanceOf[Expand]

  }

  class LoadEdges extends ImplementationRule {

    def apply(
        gm: GroupMember,
        stats: StatsStore
    ): List[GroupMember] = {
      gm.logic match {
        case GetEdges((tpe: String) :: _, transpose) =>
          val edgesCard = stats.edgesTotal(Some(tpe))
          val physical: op.Operator =
            op.GetEdgeMatrix(None, Some(tpe), transpose, edgesCard)
          List(
            EvaluatedGroupMember(gm.logic, physical)
          )

      }
    }

    override def isDefinedAt(gm: GroupMember): Boolean =
      gm.logic.isInstanceOf[GetEdges]

  }

  class LoadNodes extends ImplementationRule {

    def apply(
        gm: GroupMember,
        stats: StatsStore
    ): List[GroupMember] = {
      gm.logic match {
        case GetNodes((label :: _), sorted) =>
          val card = stats.nodesTotal(Some(label))
          val physical = op.GetNodeMatrix(
            sorted.getOrElse(new UnNamed),
            Some(label),
            card
          )
          List(
            EvaluatedGroupMember(gm.logic, physical)
          )

      }
    }

    override def isDefinedAt(gm: GroupMember): Boolean =
      gm.logic.isInstanceOf[GetNodes]

  }

  /**
    *  .. (a)-[]->(b)-[]->(c) return b
    *
    * the tree breaks into 2 branches
    * (a)-[]->(b)
    * (c)<-[]-(b)
    *  *
    * depending on direction these need to be joined on b
    * and/or sorted
    *
    * */
  class TreeJoinDiagFilter extends TransformationRule {

    override def apply(
        gm: GroupMember,
        v2: StatsStore
    ): List[GroupMember] =
      gm.logic match {
        case Join(
            on,
            Vector(left: LogicMemoRefV2, right: LogicMemoRefV2, _*)
            ) =>
          // diag on left FIXME: this is not complete but it's easy to express now
          // probably requires a transformation rule to generate the binary join trees
          // from the initial list
          (left.plan, right.plan) match {
            case (
                Filter(front1: LogicMemoRefV2, _),
                Filter(front2: LogicMemoRefV2, _)
                ) =>

              List(
                EvaluatedGroupMember(
                  gm.logic,
                  op.FilterMul(
                    op.RefOperator(front1),
                    op.Diag(op.RefOperator(right)),
                    1.0d // we can probably do better than this
                  )
                ),
                EvaluatedGroupMember(
                  gm.logic,
                  op.FilterMul(
                    op.RefOperator(front2),
                    op.Diag(op.RefOperator(left)),
                    1.0d // we can probably do better than this
                  )
                )
              )
          }

      }

    override def isDefinedAt(gm: GroupMember): Boolean = gm.logic match {
      case Join(_, Vector(l: LogicMemoRefV2, r: LogicMemoRefV2, _*)) =>
        r.plan.isInstanceOf[Filter] && l.plan.isInstanceOf[Filter]
      case _ => false
    }

  }

}
