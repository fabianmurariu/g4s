package com.github.fabianmurariu.g4s.optim

import DirectedGraph.ops._

class LogicNodeSpec extends munit.FunSuite {

  test("signature equality of Filter Expand with Expand Filter") {
    val filter = Filter(
      Expand(
        from = GetNodes("A", "a"),
        to = GetEdges(List("X")),
        transposed = false
      ),
      GetNodes("B", "b")
    )
    val expand = Expand(
      GetNodes("A", "a"),
      Filter(
        GetEdges(List("X")),
        GetNodes("B", "b")
      ),
      transposed = false
    )

    assertEquals(filter.signature, expand.signature)
  }

  test("signature equality nested Filter with Expand 1") {

    val fork1 = Filter(
      Expand(
        from = GetNodes("a", "a"),
        to = GetEdges(List("X")),
        transposed = false
      ),
      Filter(
        Expand(
          from = GetNodes("c", "c"),
          to = GetEdges(List("Y")),
          transposed = false
        ),
        GetNodes("b", "b")
      )
    )

    val fork2 = Filter(
      Expand(
        from = GetNodes("c", "c"),
        to = GetEdges(List("Y")),
        transposed = false
      ),
      Filter(
        Expand(
          from = GetNodes("a", "a"),
          to = GetEdges(List("X")),
          transposed = false
        ),
        GetNodes("b", "b")
      )
    )

    assertEquals(fork1.id(0), fork2.id(0))
    assertEquals(fork1.signature, fork2.signature)
  }

  test("Filter and Expand do not have the same signature") {
    val expand = Expand(
      from = GetNodes("a", "a"),
      to = GetEdges(List("X")),
      transposed = false
    )

    val filter =
      Filter(GetEdges(List("X")), GetNodes("a", "a"))

    assertNotEquals(expand.signature, filter.signature)
  }

  test("signature equality nested Filter with Expand 2") {

    val fork1 = Filter(
      Expand(
        from = GetNodes("a", "a"),
        to = GetEdges(List("X")),
        transposed = false
      ),
      Filter(
        Expand(
          from = GetNodes("c", "c"),
          to = GetEdges(List("Y")),
          transposed = false
        ),
        GetNodes("b", "b")
      )
    )

    val fork2 = Filter(
      Expand(
        from = GetNodes("c", "c"),
        to = GetEdges(List("Y")),
        transposed = false
      ),
      Filter(
        Expand(
          from = GetNodes("a", "a"),
          to = GetEdges(List("X")),
          transposed = false
        ),
        GetNodes("b", "b")
      )
    )

    val fork3 = Expand(
      from = GetNodes("a", "a"),
      to = Filter(
        GetEdges(List("X")),
        Filter(
          Expand(
            from = GetNodes("c", "c"),
            to = GetEdges(List("Y")),
            transposed = false
          ),
          GetNodes("b", "b")
        )
      ),
      transposed = false
    )

    val fork4 = Expand(
      from = GetNodes("a", "a"),
      to = Filter(
        GetEdges(List("X")),
        Expand(
          from = GetNodes("c", "c"),
          to = Filter(
            GetEdges(List("Y")),
            GetNodes("b", "b")
          ),
          transposed = false
        )
      ),
      transposed = false
    )

    assertEquals(fork1.id(0), fork2.id(0))
    assertEquals(fork2.id(0), fork3.id(0))
    assertEquals(fork3.id(0), fork4.id(0))

    assertEquals(fork1.signature, fork2.signature)
    assertEquals(fork2.signature, fork3.signature)
    assertEquals(fork3.signature, fork4.signature)
  }

  test("parse one hop path from cypher and generate query graph") {
    val query = """match (a:A)-[:X]->(c:C) return a"""

    val Right(queryGraph) = QueryGraph.fromCypherText(query)

    val intoC: Set[(Name, String)] = queryGraph
      .in(Node(Binding("c"))())
      .map {
        case (other, edge) => other.name -> edge.types.head
      }
      .toSet

    val expected: Set[(Name, String)] =
      Set(Binding("a") -> "X")

    assertEquals(intoC, expected)

    val outA: Set[(Name, String)] = queryGraph
      .out(Node(Binding("a"))())
      .map {
        case (other, edge) => other.name -> edge.types.head
      }
      .toSet

    val expected2: Set[(Name, String)] =
      Set(Binding("c") -> "X")

    assertEquals(outA, expected2)

  }

  test(
    "parse 2 hop path with central return from cypher and generate query graph"
  ) {
    val query = """match (a:A)-[:X]->(c:C)<-[:Y]-(:B) return c"""

    val Right(queryGraph) = QueryGraph.fromCypherText(query)

    val intoC = queryGraph
      .in(Node(Binding("c"))())
      .map { case (other, edge) => other.name -> edge.types.head }
      .map {
        case (Binding(name), edge) => Some(name) -> edge
        case (_: UnNamed, edge)    => None -> edge
      }
      .toSet

    assertEquals(intoC, Set(None -> "Y", Some("a") -> "X"))

    val outA = queryGraph
      .out(Node(Binding("a"))())
      .map { case (other, edge) => other.name -> edge.types.head }
      .toSet

    val expected: Set[(Name, String)] =
      Set(Binding("c") -> "X")
    assertEquals(outA, expected)

  }

  test(
    "parse 4 edge path with central return from cypher and generate query graph"
  ) {
    val query =
      """match (a:A)-[:X]->(c:C)<-[:Y]-(:B),(d:D)<-[:Z]-(c)-[:W]->(:E) return c"""

    val Right(queryGraph) = QueryGraph.fromCypherText(query)

    val intoC = queryGraph
      .in(Node(Binding("c"))())
      .map { case (other, edge) => other.name -> edge.types.head }
      .map {
        case (Binding(name), edge) => Some(name) -> edge
        case (_: UnNamed, edge)    => None -> edge
      }
      .toSet

    assertEquals(intoC, Set(None -> "Y", Some("a") -> "X"))

    val outC = queryGraph
      .out(Node(Binding("c"))())
      .map { case (other, edge) => other.name -> edge.types.head }
      .map {
        case (Binding(name), edge) => Some(name) -> edge
        case (_: UnNamed, edge)    => None -> edge
      }
      .toSet

    assertEquals(
      outC,
      Set(
        None -> "W",
        Some("d") -> "Z"
      )
    )

  }

  test(
    "parse a path expansion with one return (a:A)-[:X]->(c:C)-[:Y]->(d:D) return d"
  ) {

    val query =
      """match (a:A)-[:X]->(c:C)-[:Y]->(d:D) return d"""

    val Right(queryGraph) = QueryGraph.fromCypherText(query)

    val Right(actual) = LogicNode.fromQueryGraph(queryGraph)(Binding("d"))

    val expected =
      Filter(
        Expand(
          Filter(
            Expand(
              GetNodes("A", "a"),
              GetEdges(Seq("X")),
              false
            ),
            GetNodes("C", "c")
          ),
          GetEdges(Seq("Y")),
          false
        ),
        GetNodes("D", "d")
      )

    assertEquals(actual, expected)
    assertEquals(actual.output.contains(Binding("d")), true)
  }

  test(
    "parse a path expression with a return in the middle (a:A)-[:X]->(c:C)-[:Y]->(d:D) return c"
  ) {

    val query =
      """match (a:A)-[:X]->(c:C)-[:Y]->(d:D) return c"""

    val Right(queryGraph) = QueryGraph.fromCypherText(query)

    val Right(actual) = LogicNode.fromQueryGraph(queryGraph)(Binding("c"))
    val expected = Filter(
      Expand(
        GetNodes("D", "d"),
        GetEdges(Seq("Y"), transpose = true),
        true
      ),
      Filter(
        Expand(
          GetNodes("A", "a"),
          GetEdges(Seq("X")),
          false
        ),
        GetNodes("C", "c")
      )
    )

    // in case we use join again
    // val expected = Join(
    //   GetNodes(Seq("C"), Some(Binding("c"))),
    //   Vector(
    //     Filter(
    //       Expand(
    //         GetNodes(Seq("D"), Some(Binding("d"))),
    //         GetEdges(Seq("Y"), transpose = true),
    //         true
    //       ),
    //       GetNodes(Seq("C"), Some(Binding("c")))
    //     ),
    //     Filter(
    //       Expand(
    //         GetNodes(Seq("A"), Some(Binding("a"))),
    //         GetEdges(Seq("X")),
    //         false
    //       ),
    //       GetNodes(Seq("C"), Some(Binding("c")))
    //     )
    //   )
    // )

    assertEquals(actual, expected)
    assertEquals(actual.output.contains(Binding("c")), true)

  }

  test(
    "parse a path expression with 2 returns (a:A)-[:X]->(c:C)-[:Y]->(d:D) return c,d"
  ) {

    val query =
      """match (a:A)-[:X]->(c:C)-[:Y]->(d:D) return c,d"""

    val Right(queryGraph) = QueryGraph.fromCypherText(query)

    val Right(actual) = LogicNode.fromQueryGraph(queryGraph)(Binding("c"))

    val expected = Join(
      on = Binding("c"),
      expr = Join(
        expr = GetNodes("D", "d"),
        cont = Filter(
          Expand(
            GetNodes("D", "d"),
            GetEdges(Seq("Y"), transpose = true),
            true
          ),
          GetNodes("C", "c")
        ),
        on = Binding("d")
      ),
      cont = Filter(
        frontier = Expand(
          GetNodes("A", "a"),
          GetEdges(Seq("X")),
          false
        ),
        filter = Filter(
          Expand(
            GetNodes("D", "d"),
            GetEdges(Seq("Y"), transpose = true),
            true
          ),
          GetNodes("C", "c")
        )
      )
    )

    assertEquals(actual, expected)
    assertEquals(actual.output.contains(Binding("c")), true)
    assertEquals(actual.output.contains(Binding("d")), true)
  }
}
