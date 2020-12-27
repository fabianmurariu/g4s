package com.github.fabianmurariu.g4s.traverser

class PlanSpec extends munit.FunSuite with QueryGraphSamples {

  test("plan for (a)-[:X]->(b) return b") {
    val qg = eval(singleEdge_Av_X_Bv)

    val bRef = NodeRef(bTag)

    val actual = LogicalPlan.compilePlan(qg)(bRef)
    assertEquals(actual.show, out(aTag, xTag, bTag))
  }

  test("plan for (a)-[:X]->(b) return a") {
    val qg = eval(singleEdge_Av_X_Bv)

    val aRef = NodeRef(aTag)

    val actual = LogicalPlan.compilePlan(qg)(aRef)

    assertEquals(actual.show, in(bTag, xTag, aTag))
  }

  test("plan for (a)-[:X]->(b) should have 2 plans for A and B") {

    val aRef = NodeRef(aTag)
    val bRef = NodeRef(bTag)
    val qg = eval(singleEdge_Av_X_Bv)

    val allOut = Set(bRef, aRef)

    val actual = LogicalPlan.compilePlans(qg)(allOut)

    val aPlan = actual(aRef -> None)
    val bPlan = actual(bRef -> None)


    assertEquals(bPlan.show, out(aTag, xTag, bTag))
    assertEquals(aPlan.show, in(bTag, xTag, aTag))
  }

  test("plan for (a)-[:X]->(b)-[:Y]->(c) should have plans for c") {
    val qg = eval(Av_X_Bv_Y_Cv)
    val cRef = NodeRef(cTag)

    val actual = LogicalPlan.compilePlan(qg)(cRef)

    assertEquals(
      actual.show,
      out(out(aTag, xTag, bTag), yTag, cTag)
    )
  }

  test("plan for (a)-[:X]->(b)-[:Y]->(c) should have plans for c, b") {
    val qg = eval(Av_X_Bv_Y_Cv)
    val cRef = NodeRef(cTag)
    val bRef = NodeRef(bTag)

    val bindings = LogicalPlan.emptyBindings

    val actual = LogicalPlan.compilePlans(qg, bindings)(Set(cRef, bRef))

    bindings.mapValues(_.show).foreach(println)

    val cPlan = actual(cRef -> None)
    val bPlan = actual(bRef -> None)

    assertEquals(
      cPlan.show,
      out(out(aTag, xTag, bTag), yTag, cTag)
    )

    assertEquals(
      bPlan.show,
      in(cTag, yTag, out(aTag, xTag, bTag))
    )
  }

  test("plan for (a)-[:X]->(b)-[:Y]->(c)-[:Z]->(d) should have plans for d") {
    val qg = eval(Av_X_Bv_Y_Cv_Z_Dv)
    val dRef = NodeRef(dTag)

    val bindings = LogicalPlan.emptyBindings

    val actual = LogicalPlan.compilePlans(qg, bindings)(Set(dRef))

    bindings.mapValues(_.show).foreach(println)

    val dPlan = actual(dRef -> None)

    assertEquals(
      dPlan.show,
      out(out(out(aTag, xTag, bTag), yTag, cTag), zTag, dTag)
    )
  }

  test("plan for (a)-[:X]->(b)-[:Y]->(c)-[:Z]->(d) should have plans for c") {
    val qg = eval(Av_X_Bv_Y_Cv_Z_Dv)
    val cRef = NodeRef(cTag)

    val bindings = LogicalPlan.emptyBindings

    val actual = LogicalPlan.compilePlans(qg, bindings)(Set(cRef))

    bindings.mapValues(_.show).foreach(println)

    val cPlan = actual(cRef -> None)

    assertEquals(
      cPlan.show,
      in(dTag, zTag, out(out(aTag, xTag, bTag), yTag, cTag))
    )
  }

  def out(src: String, name: String, dst: String): String = {
    s"(${src})-[:$name]->(${dst})"
  }

  def in(src: String, name: String, dst: String): String = {
    s"(${src})<-[:$name]-(${dst})"
  }

}
