package com.github.fabianmurariu.g4s.optim

import DirectedGraph.ops._

class LogicNodeSpec extends munit.FunSuite {

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
              GetNodes(Seq("A"), Some(Binding("a"))),
              GetEdges(Seq("X")),
              false
            ),
            GetNodes(Seq("C"), Some(Binding("c")))
          ),
          GetEdges(Seq("Y")),
          false
        ),
        GetNodes(Seq("D"), Some(Binding("d")))
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

    val expected = Join(
      GetNodes(Seq("C"), Some(Binding("c"))),
      Vector(
        Filter(
          Expand(
            GetNodes(Seq("D"), Some(Binding("d"))),
            GetEdges(Seq("Y"), transpose = true),
            true
          ),
          GetNodes(Seq("C"), Some(Binding("c")))
        ),
        Filter(
          Expand(
            GetNodes(Seq("A"), Some(Binding("a"))),
            GetEdges(Seq("X")),
            false
          ),
          GetNodes(Seq("C"), Some(Binding("c")))
        )
      )
    )

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

    println(actual)
    val expected = Join(
      GetNodes(Seq("C"), Some(Binding("c"))),
      Vector(
        JoinPath(
          GetNodes(Seq("D"), Some(Binding("d"))),
          Filter(
            Expand(
              Diag(GetNodes(Seq("D"), Some(Binding("d")))),
              GetEdges(Seq("Y"), transpose = true),
              true
            ),
            GetNodes(Seq("C"), Some(Binding("c")))
          ),
          on = Binding("d")
        ),
        Filter(
          Expand(
            GetNodes(Seq("A"), Some(Binding("a"))),
            GetEdges(Seq("X")),
            false
          ),
          GetNodes(Seq("C"), Some(Binding("c")))
        )
      )
    )

    assertEquals(actual, expected)
    assertEquals(actual.output.contains(Binding("c")), true)
    assertEquals(actual.output.contains(Binding("d")), true)
  }
}
