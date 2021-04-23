package com.github.fabianmurariu.g4s.traverser

import scala.collection.mutable

class PlanSpec extends munit.FunSuite with QueryGraphSamples {

  test("plan for (a)-[:X]->(b) return b") {
    val qg = eval(singleEdge_Av_X_Bv)

    val bRef = NodeRef(b)

    val actual =
      LogicalPlan.dfsCompilePlan(qg, mutable.Map.empty)(bRef).deref.get
    assertEquals(actual.show, filter(expandOut(a, X), b))
  }

  test("plan for (a)-[:X]->(b) return a".ignore) {
    val qg = eval(singleEdge_Av_X_Bv)

    val aRef = NodeRef(a)

    val actual =
      LogicalPlan.dfsCompilePlan(qg, mutable.Map.empty)(aRef).deref.get

    assertEquals(actual.show, filter(expandIn(b, X), a))
  }

  test("plan for (a)-[:X]->(b) should have 2 plans for a and b".ignore) {

    val aRef = NodeRef(a)
    val bRef = NodeRef(b)
    val qg = eval(singleEdge_Av_X_Bv)

    val allOut = Set(bRef, aRef)

    val actual = LogicalPlan.compilePlans(qg, mutable.Map.empty)(allOut)

    assertEquals(actual(0).count, 2)
    assertEquals(actual(1).count, 2)

    val bPlan = actual(0).deref.get
    val aPlan = actual(1).deref.get

    assertEquals(bPlan.show, filter(expandOut(a, X), b))
    // aPlan is the same as bPlan but a is found on the row axis instead of column
    assertEquals(aPlan.show, filter(expandOut(a, X), b))
  }

  test("plan for (a)-[:X]->(b)-[:Y]->(c) should have plans for c") {
    val qg = eval(Av_X_Bv_Y_Cv)
    val cRef = NodeRef(c)

    val actual =
      LogicalPlan.dfsCompilePlan(qg, mutable.Map.empty)(cRef).deref.get

    assertEquals(
      actual.show,
      filter(expandOut(filter(expandOut(a, X), b), Y), c)
    )
  }

  test("plan for (a)-[:X]->(b)-[:Y]->(c)-[:Z]->(d) should have plans for d") {
    val qg = eval(Av_X_Bv_Y_Cv_Z_Dv)
    val dRef = NodeRef(d)

    val actual = LogicalPlan.compilePlans(qg, mutable.Map.empty)(Set(dRef))

    val dPlan = actual(0).deref.get

    assertEquals(
      dPlan.show,
      filter(
        expandOut(filter(
          expandOut(filter(
            expandOut(a, X), b), Y), c), Z), d))
  }

  test("plan for (a)-[:X]->(b)-[:Y]->(c) should have plans for b") {
    val qg = eval(Av_X_Bv_Y_Cv)
    val bRef = NodeRef(b)

    val actual = LogicalPlan.compilePlans(qg, mutable.Map.empty)(Set(bRef))

    val bPlan = actual(0).deref.get

    assertEquals(
      bPlan.show,
      filter(expandOut(a, X), sel(filter(expandIn(c, Y), b)))
    )
  }

  test(
    "plan for (a)-[:X]->(b)-[:Y]->(c),(d)-[:Z]->(b) should have plans for b"
  ) {
    val qg = eval(Av_X_Bv_Y_Cv_and_Dv_Z_Bv)
    val bRef = NodeRef(b)

    val actual = LogicalPlan.dfsCompilePlan(qg, mutable.Map.empty)(bRef).deref.get

    assertEquals(
      actual.show,
      filter(expandOut(d, Z), sel(filter(expandOut(a, X), sel(filter(expandIn(c, Y), b)))))
    )
  }

  /*
   * In the case where we have a pair of nodes to return
   * this means there is a single matrix required
   * where one node is on rows dimension and the other is on columns
   *
   */
  test("plan for (a)-[:X]->(b)-[:Y]->(c) should have plans for c, b".ignore) {
    val qg = eval(Av_X_Bv_Y_Cv)
    val cRef = NodeRef(c)
    val bRef = NodeRef(b)

    val actual =
      LogicalPlan.compilePlans(qg, mutable.Map.empty)(Seq(cRef, bRef))

    val cPlan = actual(0).deref.get
    val bPlan = actual(1).deref.get

    assertEquals(
      cPlan.show,
      filter(expandOut(sel(filter(expandOut(a, X), b)), Y), c)
    )

    assertEquals(
      bPlan.show,
      filter(expandOut(sel(filter(expandOut(a, X), b)), Y), c)
    )

  }

  test("plan for (a)-[:X]->(b)-[:Y]->(c) should have plans for b, c".ignore) {
    val qg = eval(Av_X_Bv_Y_Cv)
    val cRef = NodeRef(c)
    val bRef = NodeRef(b)

    val actual =
      LogicalPlan.compilePlans(qg, mutable.Map.empty)(Seq(bRef, cRef))

    val cPlan = actual(0).deref.get
    val bPlan = actual(1).deref.get

    val samePlan =
      filter(expandIn(c, Y),sel(filter(expandOut(a, X), b)))

    assertEquals(bPlan.show, samePlan)

    assertEquals(cPlan.show, samePlan)

  }


  test("plan for (a)-[:X]->(b)-[:Y]->(c)-[:Z]->(d) should have plans for c") {
    val qg = eval(Av_X_Bv_Y_Cv_Z_Dv)
    val cRef = NodeRef(c)

    val actual = LogicalPlan.compilePlans(qg, mutable.Map.empty)(Set(cRef))

    val cPlan = actual(0).deref.get

    assertEquals(
      cPlan.show,
      filter( exp = expandOut(filter(expandOut(a, X), b), Y),
        sel(filter(expandIn(d, Z), c)))
    )
  }

  def expandOut(src: String, name: String, rc: Int = 1): String = {
    s"(${ref(src, rc)})-[:$name]->"
  }

  def expandIn(src: String, name: String, rc: Int = 1): String = {
    s"(${ref(src, rc)})<-[:$name]-"
  }

  def filter(exp: String, filter: String, rc: Int = 1): String = {
    s"${ref(exp, rc)}(${filter})"
  }
  def ref(str: String, count: Int = 1): String =
    s"[${count};$str]"

  def sel(expand: String, count: Int = 1): String =
    s"sel[${count};$expand]"
}
