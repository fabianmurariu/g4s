package com.github.fabianmurariu.g4s.traverser

import scala.collection.immutable.Queue

class TraverserSpec extends munit.FunSuite with QueryGraphSamples {
  import com.github.fabianmurariu.g4s.traverser.Traverser._

  import scala.collection.mutable

  test("select node Bv resolve Av*X*Bv") {
    val qg = eval(singleEdge_Av_X_Bv)
    val matOps = qg.select(NodeRef(bTag))
    assertEquals(matOps.algStr, "Av*X*Bv")
  }

  test("select node Av resolve to Bv*T(X)*Av") {
    val qg = eval(singleEdge_Av_X_Bv)
    val matOps = qg.select(NodeRef(aTag))
    assertEquals(matOps.algStr, "Bv*T(X)*Av")
  }

  test("select node Cv resolve to Av*X*Bv*Y*Cv") {
    val qg = eval(Av_X_Bv_Y_Cv)
    val matOps = qg.select(NodeRef(cTag))
    assertEquals(matOps.algStr, "Av*X*Bv*Y*Cv")
  }

  test("select node Bv resolve from a->b, b->c") {
    val qg = eval(Av_X_Bv_Y_Cv)
    val matOps = qg.select(NodeRef(bTag))
    assertEquals(matOps.algStr, "Cv*T(Y)*Av*X*Bv")
    assertEquals(matOps.toStrExpr(true), "((Cv*T(Y))*((Av*X)*Bv))")
  }

  test("select node Bv resolve from a->b->c->d, e->c") {
    val qg = eval(Av_X_Bv_Y_Cv_and_Ev_X_Cv_and_Cv_Y_Dv)
    val matOps = qg.select(NodeRef(bTag))
    assertEquals(
      matOps.toStrExpr(true),
      "((((Dv*T(Y))*((Ev*X)*Cv))*T(Y))*((Av*X)*Bv))"
    )
  }

  test("select node Cv,Ev resolve from a->b->c->d, e->c") {
    val qg = eval(Av_X_Bv_Y_Cv_and_Ev_X_Cv_and_Cv_Y_Dv)
    val matOpsD = qg.select(NodeRef(dTag))
    assertEquals(
      matOpsD.toStrExpr(true),
      "((((((Av*X)*Bv)*Y)*((Ev*X)*Cv))*Y)*Dv)"
    )
    val matOpsE = qg.select(NodeRef(eTag))
    assertEquals(
      matOpsE.toStrExpr(true),
      "((((Dv*T(Y))*((((Av*X)*Bv)*Y)*Cv))*T(X))*Ev)"
    )
    val matOpsA = qg.select(NodeRef(aTag))
    assertEquals(
      matOpsA.toStrExpr(true),
      "((((((Dv*T(Y))*((Ev*X)*Cv))*T(Y))*Bv)*T(X))*Av)"
    )
    val matOpsC = qg.select(NodeRef(cTag))
    assertEquals(
      matOpsC.toStrExpr(true),
      "((Dv*T(Y))*((((Av*X)*Bv)*Y)*((Ev*X)*Cv)))"
    )
    val matOpsB = qg.select(NodeRef(bTag))
    assertEquals(
      matOpsB.toStrExpr(true),
      "((((Dv*T(Y))*((Ev*X)*Cv))*T(Y))*((Av*X)*Bv))"
    )
  }

  test("single node traverser") {

    val qg = eval(node[Av])

    val expected = mutable.Map(
      NodeRef(aTag) -> QGEdges()
    )

    assertEquals(qg, expected)
  }

  test("one node traverser expand (A)-[:X]->(B)") {
    val qg = eval(singleEdge_Av_X_Bv)
    assertEquals(qg.connectedComponentsUndirected.size, 1)
  }

  test("walk a path for [A] -> [B] -> [C]") {

    val qg = eval(Av_X_Bv_Y_Cv)
    val actual = qg.walkPaths(NodeRef(aTag))
    val expected = List(
      Queue(
        EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag)),
        EdgeRef(yTag, NodeRef(bTag), NodeRef(cTag))
      )
    )

    assertEquals(actual, expected)
    assertEquals(qg.connectedComponentsUndirected.size, 1)
  }

  test("walk a path with splits [A] -> [B], [A] -> [C], start from A") {

    val query = for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      _ <- edge[X](a, b)
      _ <- edge[Y](a, c)
    } yield ()

    val qg = query.runS(emptyQG).value
    val actual = qg.walkPaths(NodeRef(aTag))
    val expected = List(
      Queue(
        EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag))
      ),
      Queue(
        EdgeRef(yTag, NodeRef(aTag), NodeRef(cTag))
      )
    )

    assertEquals(actual, expected)
    assertEquals(qg.connectedComponentsUndirected.size, 1)
  }

  test("walk a path with splits [A] -> [B], [A] -> [C] start from B") {

    val query = for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      _ <- edge[X](a, b)
      _ <- edge[Y](a, c)
    } yield ()

    val qg = query.runS(emptyQG).value
    val actual = qg.walkPaths(NodeRef(bTag))
    val expected = List(
      Queue(
        EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag)),
        EdgeRef(yTag, NodeRef(aTag), NodeRef(cTag))
      )
    )

    assertEquals(qg.connectedComponentsUndirected.size, 1)
    assertEquals(actual, expected)
  }

  test("evaluate 2 disjoint paths [A] -> [B], [C] -> [D]") {

    val query = for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      d <- node[Dv]
      _ <- edge[X](a, b)
      _ <- edge[Y](c, d)
    } yield ()

    val qg = query.runS(emptyQG).value
    val actual1 = qg.walkPaths(NodeRef(aTag))
    val expected1 = List(
      Queue(
        EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag))
      )
    )

    assertEquals(actual1, expected1)

    val actual2 = qg.walkPaths(NodeRef(cTag))
    val expected2 = List(
      Queue(
        EdgeRef(yTag, NodeRef(cTag), NodeRef(dTag))
      )
    )

    assertEquals(actual2, expected2)
    assertEquals(qg.connectedComponentsUndirected.size, 2)
  }

//  test("walk a path in a graph with a cycle [A] -> [B] -> [A]") {
//
//    val query = for {
//      a <- node[A]
//      b <- node[B]
//      _ <- edge[X](a, b)
//      _ <- edge[Y](b, a)
//    } yield ()
//
//    val qg = query.runS(emptyQG).value
//    val actual1 = qg.walkPaths(NodeRef(aTag))
//    val expected1 = List(
//      Queue(
//        EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag)),
//        EdgeRef(yTag, NodeRef(bTag), NodeRef(aTag))
//      ),
//      Queue(
//        EdgeRef(yTag, NodeRef(bTag), NodeRef(aTag))
//      )
//    )
//
//    assertEquals(actual1, expected1)
//  }

  test("find the longest path 4 nodes long") {

    val query = for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      d <- node[Dv]
      e <- node[Ev]
      f <- node[Fv]
      _ <- edge[X](a, b)
      _ <- edge[X](b, c)
      _ <- edge[X](c, d)
      _ <- edge[X](d, e)
      _ <- edge[Y](f, c)
    } yield ()

    val qg = query.runS(emptyQG).value
    val longestPath = qg.longestPath.toSet

    val expectedLongestPath = Set(
      EdgeRef(xTag, NodeRef(bTag), NodeRef(cTag)),
      EdgeRef(xTag, NodeRef(cTag), NodeRef(dTag)),
      EdgeRef(xTag, NodeRef(dTag), NodeRef(eTag)),
      EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag))
    )

    assertEquals(longestPath.size, 4)
    assertEquals(longestPath, expectedLongestPath)
  }

  test("find the longest path of a graph disjoint".ignore) { //FIXME: this should work

    val query = for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      d <- node[Dv]
      e <- node[Ev]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
      _ <- edge[X](d, e)
    } yield ()

    val qg = query.runS(emptyQG).value
    val longestPath = qg.longestPath

    val expectedLongestPath = Queue(
      EdgeRef(yTag, NodeRef(bTag), NodeRef(cTag)),
      EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag))
    )

    assertEquals(longestPath, expectedLongestPath)
  }

  test("find the longest path of a graph") {

    val query = for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      d <- node[Dv]
      e <- node[Ev]
      _ <- edge[X](a, b)
      _ <- edge[X](a, c)
      _ <- edge[Y](c, d)
      _ <- edge[X](d, e)
    } yield ()

    val qg = query.runS(emptyQG).value
    val longestPath = qg.longestPath

    val expectedLongestPath = Queue(
      EdgeRef(xTag, NodeRef(dTag), NodeRef(eTag)),
      EdgeRef(yTag, NodeRef(cTag), NodeRef(dTag)),
      EdgeRef(xTag, NodeRef(aTag), NodeRef(cTag)),
      EdgeRef(xTag, NodeRef(aTag), NodeRef(bTag))
    )

    assertEquals(longestPath, expectedLongestPath)
  }

  test("the simplest query graph with 1 node results in error") {
    node[Av].runS(emptyQG).value.compile
  }

  // val g = ShmukGraph[IO, Vertex, Relation]

  //   g.use { graph =>
  //     for {
  //       a <- graph.insertV(new Av)
  //       b1 <- graph.insertV(new Bv)
  //       b2 <- graph.insertV(new Bv)
  //       c <- graph.insertV(new Cv)
  //       d <- graph.insertV(new Dv)
  //       _ <- graph.edge[X](a, b1)
  //       _ <- graph.edge[Y](a, b2)
  //       _ <- graph.edge[X](b1, c)
  //       _ <- graph.edge[Y](b2, d)
  //       out <- graph.run(matOps).map(_.extract)
  //     } yield out
  //   }

  test("(a) -[:X]-> (b) is Av*X*Bv") {
    val query = for {
      a <- node[Av]
      b <- node[Bv]
      _ <- edge[X](a, b)
    } yield ()

    val Right(matOps) = query.runS(emptyQG).value.compile

    assertEquals(matOps.algStr, "Av*X*Bv")

  }

  test("(a) -[:X] -> (b) -> [:Y] -> (c) is Av*X*Bv*Y*Cv") {

    val query = for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield ()

    val Right(matOps) = query.runS(emptyQG).value.compile

    assertEquals(matOps.algStr, "Av*X*Bv*Y*Cv")
  }

  test("(a) -[:X] -> (b) <- [:Y] - (c) is Av*X*Bv*T(Y)*Cv") {

    val query = for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      _ <- edge[X](a, b)
      _ <- edge[Y](c, b)
    } yield ()

    val Right(matOps) = query.runS(emptyQG).value.compile

    assertEquals(matOps.algStr, "Av*X*Bv*T(Y)*Cv")
  }

  test("(d) -[:X]-> (a) -[:X]-> (b) <-[:Y]- (c) is Cv*Y*Bv*T(X)*Av*T(X)*Dv") {

    val query = for {
      a <- node[Av]
      b <- node[Bv]
      c <- node[Cv]
      d <- node[Dv]
      _ <- edge[X](a, b)
      _ <- edge[Y](c, b)
      _ <- edge[X](d, a)
    } yield ()

    val Right(matOps) = query.runS(emptyQG).value.compile

    assertEquals(matOps.algStr, "Cv*Y*Bv*T(X)*Av*T(X)*Dv")
  }

}
