package com.github.fabianmurariu.g4s.traverser

class PlanSpec extends munit.FunSuite with QueryGraphSamples {

  test("plan expand (a)-[:X]->(b)-[:Y]-(c)") {

    val qg = eval(Av_X_Bv_Y_Cv)

    val a = NodeLoad(aTag)
    val b = NodeLoad(bTag)
    val c = NodeLoad(cTag)

    val axb = Expand(from = a, to = b, xTag, false)

    val axbEdge = EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag))
    val bxcEdge = EdgeRef(yTag, NodeRef(bTag), NodeRef(cTag))

    val actual = Plan.expand(qg, Set(NodeRef(cTag)))(
      Set(axbEdge) -> axb
    )

    val expected = Seq(
      Set(axbEdge, bxcEdge) -> MasterPlan(
        Map(
          NodeRef(cTag) -> Expand(
            from = a,
            to = Expand(from = b, to = c, yTag, false),
            xTag,
            false
          )
        )
      )
    )

    assertEquals(actual, expected)
  }

  test("plan join (a)-[:X]->(b) , (b)-[:Y]->(c)") {
    val a = NodeLoad("a")
    val b = NodeLoad("b")
    val c = NodeLoad("c")

    val left = Expand(from = a, to = b, "X", false)
    val right = Expand(from = b, to = c, "Y", false)

    val coverLeft = Set(EdgeRef("X", NodeRef("a"), NodeRef("b")))
    val coverRight = Set(EdgeRef("Y", NodeRef("b"), NodeRef("c")))

    val actual = Plan.join(Set(NodeRef("c")))(
      coverLeft -> left,
      coverRight -> right
    )

    val expected = Option(
      (coverLeft ++ coverRight) -> Expand(
        from = a,
        to = Expand(
          from = b,
          to = c,
          name = "Y",
          transpose = false
        ),
        name = "X",
        transpose = false
      )
    )

    assertEquals(actual, expected)
  }

  test("plan single edge out (a) -[:X]-> (b) return b") {
    val qg = eval(singleEdge_Av_X_Bv)

    val ref = NodeRef(bTag)
    val actual = Plan.fromQueryGraph(qg, ref)
    val expected = MasterPlan(
      Map(
        ref -> Expand(
          NodeLoad(aTag),
          NodeLoad(bTag),
          xTag,
          false
        )
      )
    )

    assertEquals(actual, expected)
    // now eval the plan
    val algPlan = Plan.eval(actual).mapValues(_.algStr)

    assertEquals(algPlan, Map(ref -> "Av*X*Bv"))
  }

  test("plan single edge out (a) -[:X]-> (b) return a") {
    val qg = eval(singleEdge_Av_X_Bv)

    val ref = NodeRef(aTag)
    val actual = Plan.fromQueryGraph(qg, ref)
    val expected = MasterPlan(
      Map(
        ref -> Expand(
          NodeLoad(bTag),
          NodeLoad(aTag),
          xTag,
          true
        )
      )
    )

    assertEquals(actual, expected)
    // now eval the plan
    val algPlan = Plan.eval(actual).mapValues(_.algStr)

    assertEquals(algPlan, Map(ref -> "Bv*T(X)*Av"))
  }

  test("plan 2 edge out (a) -[:X]-> (b) -[:Y]-> (c) return c") {
    val qg = eval(Av_X_Bv_Y_Cv)

    val ref = NodeRef(cTag)
    val actual = Plan.fromQueryGraph(qg, ref)
    val expected = MasterPlan(
      Map(
        ref ->
          Expand(
            from = NodeLoad(aTag),
            to = Expand(
              NodeLoad(bTag),
              NodeLoad(cTag),
              yTag,
              transpose = false
            ),
            xTag,
            transpose = false
          )
      )
    )

    assertEquals(actual, expected)
    // now eval the plan
    val algPlan = Plan.eval(actual).mapValues(_.algStr)

    assertEquals(algPlan, Map(ref -> "Av*X*Bv*Y*Cv"))
  }

}
