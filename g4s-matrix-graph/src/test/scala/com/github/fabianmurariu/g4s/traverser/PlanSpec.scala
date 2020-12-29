package com.github.fabianmurariu.g4s.traverser

class PlanSpec extends munit.FunSuite with QueryGraphSamples {

  test("plan for (a)-[:X]->(b) return b") {
    val qg = eval(singleEdge_Av_X_Bv)

    val bRef = NodeRef(b)

    val actual = LogicalPlan.compilePlan(qg)(bRef)
    assertEquals(actual.show, out(a, X, b))
  }

  test("plan for (a)-[:X]->(b) return a") {
    val qg = eval(singleEdge_Av_X_Bv)

    val aRef = NodeRef(a)

    val actual = LogicalPlan.compilePlan(qg)(aRef)

    assertEquals(actual.show, in(b, X, a))
  }

  test("plan for (a)-[:X]->(b) should have 2 plans for a and b") {

    val aRef = NodeRef(a)
    val bRef = NodeRef(b)
    val qg = eval(singleEdge_Av_X_Bv)

    val allOut = Set(bRef, aRef)

    val actual = LogicalPlan.compilePlans(qg)(allOut)

    val aPlan = actual(aRef -> Set.empty)
    val bPlan = actual(bRef -> Set.empty)


    assertEquals(bPlan.show, out(a, X, b))
    assertEquals(aPlan.show, in(b, X, a))
  }
  
  test("plan for (a)-[:X]->(b)-[:Y]->(c) should have plans for b and subplans") {
    val qg = eval(Av_X_Bv_Y_Cv)
    val bRef = NodeRef(b)
    val aRef = NodeRef(a)
   
    val axb = qg.out(aRef).head
    val byc = qg.out(bRef).head
    
    val bindings = LogicalPlan.emptyBindings
    
    val actual = LogicalPlan.compilePlan(qg, bindings)(bRef)

    // component parts of b
    assertEquals(
      bindings(bRef -> Set(axb)).show,
      in(c, Y, b)
    )
    
    assertEquals(
      bindings(bRef -> Set(byc)).show,
      out(a, X, b)
    )
    
    assertEquals(
      actual.show,
        in(c, Y, out(a, X, b))
    )
  }

  test("plan for (a)-[:X]->(b)-[:Y]->(c),(d)-[:Z]->(b) should have plans for b and subplans") {
    val qg = eval(Av_X_Bv_Y_Cv_and_Dv_Z_Bv)
    val bRef = NodeRef(b)
    val aRef = NodeRef(a)
    val dRef = NodeRef(d)

    val axb = qg.out(aRef).head
    val byc = qg.out(bRef).head
    val dzb = qg.out(dRef).head

    val bindings = LogicalPlan.emptyBindings

    val actual = LogicalPlan.compilePlan(qg, bindings)(bRef)

    // component parts of b
    assertEquals(
      bindings(bRef -> Set(axb, dzb)).show,
      in(c, Y, b)
    )

    assertEquals(
      bindings(bRef -> Set(byc, dzb)).show,
      out(a, X, b)
    )

    assertEquals(
      bindings(bRef -> Set(axb, byc)).show,
      out(d, Z, b)
    )
    
    assertEquals(
      actual.show,
      in(c, Y, out(a, X, out(d, Z, b)))
    ) 
  }
  
  test("plan for (a)-[:X]->(b)-[:Y]->(c) should have plans for c") {
    val qg = eval(Av_X_Bv_Y_Cv)
    val cRef = NodeRef(c)

    val actual = LogicalPlan.compilePlan(qg)(cRef)

    assertEquals(
      actual.show,
      out(out(a, X, b), Y, c)
    )
  }

  test("plan for (a)-[:X]->(b)-[:Y]->(c) should have plans for c, b") {
    val qg = eval(Av_X_Bv_Y_Cv)
    val cRef = NodeRef(c)
    val bRef = NodeRef(b)

    val bindings = LogicalPlan.emptyBindings

    val actual = LogicalPlan.compilePlans(qg, bindings)(Set(cRef, bRef))
    
    val cPlan = actual(cRef -> Set.empty)
    val bPlan = actual(bRef -> Set.empty)

    assertEquals(
      cPlan.show,
      out(out(a, X, b), Y, c)
    )

    assertEquals(
      bPlan.show,
      in(c, Y, out(a, X, b))
    )
  }

   test("plan for (a)-[:X]->(b)-[:Y]->(c)-[:Z]->(d) should have plans for d") {
     val qg = eval(Av_X_Bv_Y_Cv_Z_Dv)
     val dRef = NodeRef(d)

     val bindings = LogicalPlan.emptyBindings

     val actual = LogicalPlan.compilePlans(qg, bindings)(Set(dRef))

     val dPlan = actual(dRef -> Set.empty)

     assertEquals(
       dPlan.show,
       out(out(out(a, X, b), Y, c), Z, d)
     )
   }

   test("plan for (a)-[:X]->(b)-[:Y]->(c)-[:Z]->(d) should have plans for c") {
     val qg = eval(Av_X_Bv_Y_Cv_Z_Dv)
     val cRef = NodeRef(c)

     val bindings = LogicalPlan.emptyBindings

     val actual = LogicalPlan.compilePlans(qg, bindings)(Set(cRef))

     val cPlan = actual(cRef -> Set.empty)

     assertEquals(
       cPlan.show,
       in(d, Z, out(out(a, X, b), Y, c))
     )
   }

  def out(src: String, name: String, dst: String): String = {
    s"(${src})-[:$name]->(${dst})"
  }

  def in(src: String, name: String, dst: String): String = {
    s"(${src})<-[:$name]-(${dst})"
  }

}
