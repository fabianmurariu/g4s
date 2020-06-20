package com.github.fabianmurariu.g4s.graph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import com.github.fabianmurariu.g4s.graph.QPath.DFSEdgeType
import com.github.fabianmurariu.g4s.graph.QPath.Back
import com.github.fabianmurariu.g4s.graph.QPath.Forward

class QuerySpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  import QPath._

  "QueryPath" should "represent the simplest one hop edge ()-[:edge]->() predicate" in {

    val start = vs()
    val query = start.out("edge").vs()
    query shouldBe Triplet(start, EdgesQ(Out, Set("edge")), AnyV)

  }

  it should "represent predicates on both nodes and edges (x:X)-[:a]->(y:Y)" in {
    val start = vs("x")
    val query = start.out("a").vs("y")
    query shouldBe Triplet(start, EdgesQ(Out, Set("a")), NodesQ(Set("y")))
  }

  it should "support 2 hops out, in (x)-[:a]->(y)<-[:b]-(z)" in {
    val start = vs()
    val query = start.out("a").vs().in("b").vs()

    query shouldBe Connect(
      Vector(
        Triplet(start, EdgesQ(Out, Set("a")), AnyV),
        Triplet(AnyV, EdgesQ(In, Set("b")), AnyV)
      )
    )
  }

  it should "support 2 hops out, out (x)-[:a]->(y)-[:b]->(z)" in {
    val start = vs()
    val query = start.out("a").vs().out("b").vs()

    query shouldBe Connect(
      Vector(
        Triplet(start, EdgesQ(Out, Set("a")), AnyV),
        Triplet(AnyV, EdgesQ(Out, Set("b")), AnyV)
      )
    )
  }

  it should "support split paths" in {
    val start = vs("x")

    val query = start.split(
      path { _.out("a").vs("y").out("c").vs("z") },
      path { _.out("b").vs("w") }
    )

    query shouldBe Join(
      start,
      Connect(
        Vector(
          Triplet(start, EdgesQ(Out, Set("a")), NodesQ(Set("y"))),
          Triplet(NodesQ(Set("y")), EdgesQ(Out, Set("c")), NodesQ(Set("z")))
        )
      ),
      Triplet(start, EdgesQ(Out, Set("b")), NodesQ(Set("w")))
    )
  }

  "DFS walk" should "be a simple pair for one edge query (x)-[:a]->(y)" in {
    val start = vs("x")
    val query = start.out("a").vs("y")

    dfsWalk(query) shouldBe Vector(
      (Triplet(start, EdgesQ(Out, Set("a")), NodesQ(Set("y"))), Forward)
    )
  }

  it should "return 2 forward paths for a 2 hop query (x)-[:a]->(y)-[:b]->z" in {
    val start = vs("x")
    val query = start.out("a").vs("y").out("b").vs("z")

    dfsWalk(query) shouldBe Vector(
      (Triplet(start, EdgesQ(Out, Set("a")), NodesQ(Set("y"))), Forward),
      (Triplet(NodesQ(Set("y")), EdgesQ(Out, Set("b")), NodesQ(Set("z"))), Forward),
      (Triplet(start, EdgesQ(Out, Set("a")), NodesQ(Set("y"))), Back)
    )
  }

  it should "return 3 forward paths for a 2hop, 1hop fork " in {

    val start = vs("x")
    val query = start.split(
      path { _.out("a").vs("y").out("c").vs("z") },
      path { _.out("b").vs("w") }
    )

    val actualWalk = dfsWalk(query)

    actualWalk shouldBe Vector(
      (Triplet(start, EdgesQ(Out, Set("a")), NodesQ(Set("y"))), Forward),
      (Triplet(NodesQ(Set("y")), EdgesQ(Out, Set("c")), NodesQ(Set("z"))), Forward),
      (Triplet(start, EdgesQ(Out, Set("a")), NodesQ(Set("y"))), Back),
      (Triplet(start, EdgesQ(Out, Set("b")), NodesQ(Set("w"))), Forward),
      )

    val algExpr = MagiQ.queryTranslation(actualWalk.toVector)
    println(algExpr)
  }
}

sealed trait QPath { self =>

  /**
    * Return the left most node of a path
    */
  def head: QPath = self match {
    case v: VRef      => v.pred
    case n: NodeQPath => n
    case c: Connect   => c.triplets.head.head
    case j: Join      => j.root
    case t: Triplet   => t.src
  }

}

sealed trait NodeQPath extends QPath { self =>
  // expand to the edge
  def outE = edges(Out)
  def inE = edges(In)

  def out(tpes: String*) =
    edges(Out, tpes: _*)

  def in(tpes: String*) =
    edges(In, tpes: _*)

  def edges(d: Direction, tpes: String*) = {
    if (tpes.isEmpty) Triplet(self, new AnyE(d), AnyV)
    else Triplet(self, EdgesQ(d, tpes.toSet), AnyV)
  }

  /**
    * splits this query node path and adds multiple paths to it forking
    */
  def split(qps: QPathFn*): Join = {
    val childrenPaths = qps.map { qpfn =>
      val subPath = qpfn.f(self)
      if (subPath.head == self) // we're connected
        subPath
      else { // disjoint subpaths figure out later
        assert(
          subPath.head == self,
          s"Paths do not connect ${subPath.head} != $self"
        )
        subPath
      }
    }
    Join(self, childrenPaths: _*)
  }
}

class VRef(val pred: NodeQPath = AnyV) extends NodeQPath
case object AnyV extends NodeQPath
case class NodesQ(labels: Set[String]) extends NodeQPath

sealed trait EdgeQPath
case class AnyE(d: Direction) extends EdgeQPath
case class EdgesQ(d: Direction, tpes: Set[String]) extends EdgeQPath

sealed trait ContainerQPath extends QPath
// (src)-[:edge]->(dst)
case class Triplet(src: NodeQPath, edge: EdgeQPath, dst: NodeQPath)
    extends ContainerQPath { self =>

  def vs(labels: String*) = {
    val dst = if (labels.isEmpty) AnyV else NodesQ(labels.toSet)
    self.copy(dst = dst)
  }

  // the triplet is complete, add the next one
  def out = edges(Out)
  def in = edges(In)

  def out(tpe1: String, tpes: String*) = edges(Out, (tpe1 +: tpes): _*)
  def in(tpe1: String, tpes: String*) = edges(In, (tpe1 +: tpes): _*)

  def edges(d: Direction, tpes: String*) = {
    val next = dst.edges(d, tpes: _*)
    Connect(Vector(self, next))
  }
}
// (x)-[:a]->(y)-[:b]->(z),(y)->[:c]->(w)
case class Join(root: QPath, children: ContainerQPath*) extends ContainerQPath
case class QPathFn(f: NodeQPath => ContainerQPath)

case class Connect(triplets: Seq[Triplet]) extends ContainerQPath { self =>
  def out = edges(Out)
  def in = edges(In)

  def out(tpe1: String, tpes: String*) = edges(Out, (tpe1 +: tpes): _*)
  def in(tpe1: String, tpes: String*) = edges(In, (tpe1 +: tpes): _*)

  def edges(d: Direction, tpes: String*) = {
    val last = triplets.last
    last.edges(d, tpes: _*) match {
      case Connect(Vector(_, next)) => Connect(triplets :+ next)
    }
  }

  def vs(labels: String*) = {
    val last = triplets.last
    Connect(triplets.updated(triplets.length - 1, last.vs(labels: _*)))
  }

}

object QPath {
  def vs(labels: String*) =
    if (labels.isEmpty) new VRef else new VRef(NodesQ(labels.toSet))

  def path(f: NodeQPath => ContainerQPath) = QPathFn(f)

  /**
   * Produce the dfsWalk and output pairs
   * according to MAGiQ
   * every triplet that is not
   * FIXME: this is incomplete and cannot support forks at any point in the AST
   */
  def dfsWalk(q:ContainerQPath, direction:DFSEdgeType = Forward):Stream[(Triplet, DFSEdgeType)] = {
    q match {
      case t:Triplet => Stream((t, Forward))
      case Connect(head +: tail) if tail.isEmpty => Stream((head, Forward))
      case c@Connect(triplets :+ last) =>
        c.triplets.map(t => (t, Forward)).toStream ++ triplets.map(t => (t, Back))
      case Join(root, children @ _*) =>
        children.map(dfsWalk(_)).reduce(_ ++ _)
        //FIXME ignore root for now as it's the first src for all children

    }
  }

  sealed trait DFSEdgeType
  case object Forward extends DFSEdgeType
  case object Back extends DFSEdgeType
}


object MagiQ {

  case class MatKey(v1:NodeQPath, v2:NodeQPath)


  case class Pred(tpes:Set[String])

  sealed trait AlgExpr
  case object Identity extends AlgExpr
  case class MulMatPred(m:AlgExpr, p:Pred) extends AlgExpr
  case class Select(m:AlgExpr, p:Pred, a:AlgExpr) extends AlgExpr
  case class Select2(m:AlgExpr, s:AlgExpr) extends AlgExpr
  case class Transpose(m:AlgExpr) extends AlgExpr
  case class Any(m:AlgExpr) extends AlgExpr
  case class Diag(m:AlgExpr) extends AlgExpr

  case object GraphMat extends AlgExpr


  def queryTranslation(dfsWalk:Vector[(Triplet, DFSEdgeType)]) = {
    val ctx = scala.collection.mutable.HashMap[MatKey, AlgExpr]()

    dfsWalk.zipWithIndex.foreach{
      case ((triplet, dfsEdgeTpe), i) =>
        val Triplet(v1, edge, v2) = triplet

        // val (prevTriplet, direction) = if (i == 0) None else Some(dfsWalk(i-1))

        if (i == 0) {
          val (pred, a) = edge match {
            case EdgesQ(Out, tpes) => (Pred(tpes), GraphMat)
            case AnyE(Out) => (Pred(Set()), GraphMat)
            case EdgesQ(In, tpes) => (Pred(tpes), Transpose(GraphMat))
            case AnyE(In) => (Pred(Set()), Transpose(GraphMat))
	        }
          println(s"put $v1-$v2")
          ctx.put(MatKey(v1, v2), Select(Identity, pred, a))
        } else {

          val (prevTriplet, prevDfsEdgeTpe) = dfsWalk(i - 1)
          val Triplet(w1, pedge, w2) = prevTriplet
          //   prevDfsEdgeTpe match {
          //   case Forward => prevTriplet
          //   case Back => Triplet(prevTriplet.dst, prevTriplet.edge, prevTriplet.src)
          // }

          // the condition on the Out/In of the GraphMat is dubious and needs to be tested
          val (pred, a) = edge match {
            case EdgesQ(Out, tpes) => (Pred(tpes), GraphMat)
            case AnyE(Out) => (Pred(Set()), GraphMat)
            case EdgesQ(In, tpes) => (Pred(tpes), Transpose(GraphMat))
            case AnyE(In) => (Pred(Set()), Transpose(GraphMat))
	        }

          if (dfsEdgeTpe == Forward) {
            val mIn1 = if (v1 == w1) {
              Diag(Any(ctx(MatKey(w1, w2))))
            } else {
              Diag(Any(Transpose(ctx(MatKey(w1, w2)))))
            }

            val mout = Select(mIn1, pred, a)
            println(s"put $v1-$v2")
            ctx.put(MatKey(v1, v2), mout)
          } else {
            val mIn1 = ctx(MatKey(v1, v2))
            val mIn2 = Diag(Any(ctx(MatKey(w1, w2))))
            println(s"put $v1-$v2")
            ctx.put(MatKey(v1, v2), Select2(mIn1, mIn2))
          }

        }

    }
    ctx
  }
}
