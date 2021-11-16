package com.github.fabianmurariu.g4s.optim

import scala.collection.immutable.Queue

trait MemoFixtures {

    val nodesA = GetNodes("A", "a")
    val edgesX = GetEdges(List("X"), false)
    val nodesB = GetNodes("B", "b")
    val nodesC = GetNodes("C", "c")
    val edgesY = GetEdges(List("Y"), true)
    val expandA = Expand(nodesA, edgesX)
    val expandB = Expand(nodesB, edgesY)
    val filter1 = Filter(expandB, nodesC)
    val filter2 = Filter(expandA, filter1)


    val nodesAGroup =
      GroupV2(nodesA, Vector(UnEvaluatedGroupMember(nodesA)), None)
    val nodesBGroup =
      GroupV2(nodesB, Vector(UnEvaluatedGroupMember(nodesB)), None)
    val nodesCGroup =
      GroupV2(nodesC, Vector(UnEvaluatedGroupMember(nodesC)), None)
    val edgesXGroup =
      GroupV2(edgesX, Vector(UnEvaluatedGroupMember(edgesX)), None)
    val edgesYGroup =
      GroupV2(edgesY, Vector(UnEvaluatedGroupMember(edgesY)), None)

    val expandAGroupLogic =
      Expand(LogicMemoRefV2(nodesA), LogicMemoRefV2(edgesX))
    val expandBGroupLogic =
      Expand(LogicMemoRefV2(nodesB), LogicMemoRefV2(edgesY))

    val expandAGroup =
      GroupV2(
        expandAGroupLogic,
        Vector(UnEvaluatedGroupMember(expandAGroupLogic)),
        None
      )

    val expandBGroup =
      GroupV2(
        expandBGroupLogic,
        Vector(UnEvaluatedGroupMember(expandBGroupLogic)),
        None
      )

    val filter1GroupLogic =
      Filter(LogicMemoRefV2(expandBGroupLogic), LogicMemoRefV2(nodesC))
    val filter2GroupLogic =
      Filter(
        LogicMemoRefV2(expandAGroupLogic),
        LogicMemoRefV2(filter1GroupLogic)
      )

    val filter1Group =
      GroupV2(
        filter1GroupLogic,
        Vector(UnEvaluatedGroupMember(filter1GroupLogic)),
        None
      )

    val filter2Group =
      GroupV2(
        filter2GroupLogic,
        Vector(UnEvaluatedGroupMember(filter2GroupLogic)),
        None
      )

    val queue = Queue(
      nodesA.signature,
      edgesX.signature,
      expandA.signature,
      nodesB.signature,
      edgesY.signature,
      expandB.signature,
      nodesC.signature,
      filter1.signature,
      filter2.signature
    )
    val expectedMemo =
      MemoV2(
        Map.empty,
        queue,
        Map(
          nodesA.signature -> nodesAGroup,
          nodesB.signature -> nodesBGroup,
          nodesC.signature -> nodesCGroup,
          edgesY.signature -> edgesYGroup,
          edgesX.signature -> edgesXGroup,
          expandA.signature -> expandAGroup,
          expandB.signature -> expandBGroup,
          filter1.signature -> filter1Group,
          filter2.signature -> filter2Group
        )
      )
}
