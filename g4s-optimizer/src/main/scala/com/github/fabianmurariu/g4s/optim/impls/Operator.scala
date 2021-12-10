package com.github.fabianmurariu.g4s.optim.impls

import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grbv2.MxM
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import com.github.fabianmurariu.g4s.optim.Name
import cats.effect.IO
import com.github.fabianmurariu.g4s.optim.Binding
import com.github.fabianmurariu.g4s.optim.UnNamed
import cats.effect.kernel.MonadCancel
import cats.effect.unsafe.IORuntime
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.optim.EvaluatorGraph
import com.github.fabianmurariu.g4s.optim.MemoV2
import com.github.fabianmurariu.g4s.optim.EvaluatedGroupMember
import com.github.fabianmurariu.g4s.optim.CostedGroupMember
import com.github.fabianmurariu.g4s.optim.logic.LogicMemoRefV2

/**
  * base class for push operators
  * inspired by "How to Architect a Query Compiler, Revised" Ruby T Tahboub, et al.
  */
sealed abstract class Operator(val children: Vector[Operator] = Vector.empty) {
  self =>

  def eval(eg: EvaluatorGraph)(cb: Record => IO[Unit])(
      implicit G: GRB
  ): IO[Unit]

  /**
    * The binding on which the operator output is sorted
    *
    * @return
    */
  def sorted: Option[Name]

  /**
    * What bindings are covered by this operator
    * [A]
    *
    * or [A, B]
    *
    * @return
    */
  def output: Seq[Name]

  def relativeCost(m: MemoV2): (Double, Long) =
    Operator.relativeCost(m, self)

  private def showInner(m: MemoV2): List[String] = {
    val (cost, cardinality) = relativeCost(m)
    val sortedText =
      self.sorted.collect { case Binding(name) => name }.getOrElse("")
    val coverText =
      self.output
        .map {
          case Binding(name) => name
          case _: UnNamed    => "*"
        }
        .mkString("[", ",", "]")
    val sizeText = s"${cardinality}"
    val costText = s"${cost}"
    val text =
      List(
        "size" -> sizeText,
        "sorted" -> sortedText,
        "output" -> coverText,
        "cost" -> costText
      )
    text
      .filter { case (_, text) => text.nonEmpty }
      .map { case (key, text) => s"$key=$text" }
  }

  def show(m: MemoV2, offset: String = ""): String = self match {
    case _: GetNodeMatrix =>
      val textBlocks = showInner(m).mkString(", ")
      s"Nodes[$textBlocks]"
    case em: GetEdgeMatrix =>
      val textBlocks = showInner(m).mkString(", ")
      val name = em.name.mkString("[", ",", "]")
      s"Edges[$textBlocks, label=$name, transpose=${em.transpose}]"
    case ExpandMul(left, right, sel) =>
      val padding = offset + "  "
      val textBlocks = showInner(m).mkString(", ")
      s"""ExpandMul[sel=$sel, $textBlocks]:\n${padding}l=${left
        .show(m, padding)}\n${padding}r=${right.show(m, padding)}"""
    case FilterMul(left, right, sel) =>
      val padding = offset + "  "
      val textBlocks = showInner(m).mkString(", ")
      s"""FilterMul[sel=$sel, $textBlocks]:\n${padding}l=${left
        .show(m, padding)}\n${padding}r=${right.show(m, padding)}"""
    case Diag(op) =>
      val padding = offset + "  "
      val textBlocks = showInner(m).mkString(", ")
      s"""Diag[$textBlocks]:\n${padding}on=${op.show(m, padding)}"""
    case RefOperator(_) =>
      val textBlocks = showInner(m).mkString(", ")
      s"Ref[${textBlocks}]"
  }
}

sealed trait MatrixOperator extends Operator

sealed abstract class ForkOperator(cs: Vector[Operator]) extends Operator(cs) {
  def rewrite(children: Vector[Operator]): ForkOperator
}

object Operator {

  val semiRing: GrBSemiring[Boolean, Boolean, Boolean] =
    Expand.staticAnyPairSemiring

  def relativeCost(m: MemoV2, op: Operator): (Double, Long) = op match {
    case RefOperator(logic) =>
      m.table(logic.signature).optMember match {
        case Some(CostedGroupMember(_, _, cost, card, _)) =>
          (cost, card)
        case Some(EvaluatedGroupMember(_, plan, _)) =>
          relativeCost(m, plan)
        case _ =>
          throw new IllegalStateException(
            s"Unable to calculate cost or cardinality in un-optimized member ${logic.signature}"
          )
      }
    case GetEdgeMatrix(_, _, _, card) => (0d, card)
    case GetNodeMatrix(_, _, card)    => (0d, card)
    case ExpandMul(frontier, edges, sel) =>
      val (frontierCost, frontierCard) = relativeCost(m, frontier)
      val (edgesCost, edgesCard) = relativeCost(m, edges)
      val cardinality = Math.max((frontierCard * edgesCard) * sel, 1.0d).toLong
      val cost = (1.2 * cardinality) + frontierCost + edgesCost
      (cost, cardinality)
    case FilterMul(frontier, filter, sel) =>
      val (frontierCost, frontierCard) = relativeCost(m, frontier)
      val (filterCost, filterCard) = relativeCost(m, filter)
      val cardinality = Math.max((frontierCard * filterCard) * sel, 1.0d).toLong
      val cost = (1.2 * cardinality) + frontierCost + filterCost
      (cost, cardinality)
    case Diag(op) =>
      val (opCost, opCard) = relativeCost(m, op)
      val card = Math.sqrt(opCard.toDouble).toLong
      val cost = (1.2 * opCard) + opCost
      (cost, card)

  }

  def commonMxM(eg: EvaluatorGraph)(left: Operator, right: Operator)(
      cb: Record => IO[Unit]
  )(implicit G: GRB): IO[Unit] = {
    left.eval(eg) {
      case left: MatrixRecord @unchecked =>
        right.eval(eg) {
          case right: MatrixRecord @unchecked =>
            MonadCancel[IO].bracket(IO.delay(right.mat)) { edges =>
              MxM[IO]
                .mxm(left.mat)(left.mat, edges)(semiRing)
                .flatMap(rec => cb(MatrixRecord(rec)))
            }(_.release)
          case e: Edges =>
            e.mat
              .use[GrBMatrix[IO, Boolean]] { edges =>
                MxM[IO].mxm(left.mat)(left.mat, edges)(semiRing)
              }
              .flatMap(mat => cb(MatrixRecord(mat)))
          case n: Nodes => // filter case (maybe we should break MatrixMul into 2 operators one for Expand and one for filter)
            n.mat
              .use { nodes => MxM[IO].mxm(left.mat)(left.mat, nodes)(semiRing) }
              .flatMap(mat => cb(MatrixRecord(mat)))
        }
      case left: Nodes =>
        right.eval(eg) {
          case right: MatrixRecord =>
            left.mat.use { nodes =>
              MxM[IO]
                .mxm(right.mat)(nodes, right.mat)(semiRing)
                .flatMap(rec => cb(MatrixRecord(rec)))
            }
          case e: Edges =>
            e.mat
              .use[GrBMatrix[IO, Boolean]] { edges =>
                left.mat.use { nodes =>
                  for {
                    shape <- edges.shape
                    (rows, cols) = shape
                    out <- GrBMatrix.unsafe[IO, Boolean](rows, cols)
                    output <- MxM[IO].mxm(out)(nodes, edges)(semiRing)
                  } yield output
                }
              }
              .flatMap(rec => cb(MatrixRecord(rec)))
        }
    }

  }
}

/**
  * A placeholder operator
  * pointing to the memo
  * entry of the actual phisical
  * operator
  * */
case class RefOperator(logic: LogicMemoRefV2) extends Operator {

  def eval(eg: EvaluatorGraph)(cb: Record => IO[Unit])(
      implicit G: GRB
  ): IO[Unit] = IO.unit

  def sorted: Option[Name] = None

  def output: Seq[Name] = logic match {
    case LogicMemoRefV2(value) => value.output
  }

  def cardinality: Long = 1L

  def signature: Int = logic match {
    case LogicMemoRefV2(value) => value.signature
  }

}

object RefOperator {
  def apply(logic: LogicMemoRefV2): RefOperator = new RefOperator(logic)
}

case class GetNodeMatrix(
    binding: Name,
    name: Option[String],
    cardinality: Long
) extends MatrixOperator {

  def eval(
      eg: EvaluatorGraph
  )(cb: Record => IO[Unit])(implicit G: GRB): IO[Unit] =
    eg.lookupNodes(name).flatMap {
      case (nodes, _) =>
        cb(Nodes(nodes))
    }

  def sorted: Option[Name] = Some(binding)

  def output: Seq[Name] = Seq(binding)

}

case class GetEdgeMatrix(
    binding: Option[Name],
    name: Option[String],
    transpose: Boolean,
    cardinality: Long
) extends MatrixOperator {

  def eval(
      eg: EvaluatorGraph
  )(cb: Record => IO[Unit])(implicit G: GRB): IO[Unit] =
    eg.lookupEdges(name, transpose).flatMap {
      case (edges, _) =>
        cb(Edges(edges))
    }
  def sorted: Option[Name] = None

  def output: Seq[Name] = binding.toSeq

}

case class ExpandMul(
    frontier: Operator,
    edges: Operator,
    edgesSel: Double = 1.0d
) extends ForkOperator(Vector(frontier, edges)) {

  lazy val semiRing: GrBSemiring[Boolean, Boolean, Boolean] =
    Expand.staticAnyPairSemiring //FIXME get rid of this

  def eval(
      eg: EvaluatorGraph
  )(cb: Record => IO[Unit])(implicit G: GRB): IO[Unit] =
    Operator.commonMxM(eg)(frontier, edges)(cb)

  def sorted: Option[Name] = frontier.sorted

  def output: Seq[Name] =
    (frontier.output.headOption ++ edges.output.lastOption).toSeq

  override def rewrite(children: Vector[Operator]): ForkOperator =
    ExpandMul(
      children(0),
      children(1),
      edgesSel
    ) // this is questionable, does the selectivity remain the same?, what is the edgesSel doing here?

}

case class FilterMul(
    frontier: Operator,
    filter: Operator,
    sel: Double
) extends ForkOperator(Vector(frontier, filter)) {

  val semiRing: GrBSemiring[Boolean, Boolean, Boolean] =
    Expand.staticAnyPairSemiring

  def eval(
      eg: EvaluatorGraph
  )(cb: Record => IO[Unit])(implicit G: GRB): IO[Unit] = {
    Operator.commonMxM(eg)(frontier, filter)(cb)
  }

  def sorted: Option[Name] = frontier.sorted

  def output: Seq[Name] =
    (frontier.output.headOption ++ filter.output.lastOption).toSeq

  override def rewrite(children: Vector[Operator]): ForkOperator =
    FilterMul(
      children(0),
      children(1),
      sel
    ) // this is questionable, does the selectivity remain the same?, what is the sel doing here?
}

case class Diag(op: Operator) extends ForkOperator(Vector(op)) {

  def eval(
      eg: EvaluatorGraph
  )(cb: Record => IO[Unit])(implicit G: GRB): IO[Unit] =
    op.eval(eg) {
      case MatrixRecord(mat) =>
        for {
          shape <- mat.shape
          (rows, cols) = shape
          _ <- mat.show().map(println)
          _ <- mat.reduceColumns(BuiltInBinaryOps.boolean.lor, None).use {
            vec => mat.assignToDiag(vec)
          }
          _ <- mat.show().map(println)
          _ <- cb(MatrixRecord(mat))
        } yield ()
    }

  def sorted: Option[Name] = op.sorted

  def output: Seq[Name] = op.output.lastOption.toSeq

  override def rewrite(children: Vector[Operator]): ForkOperator =
    Diag(children(0))
}

import scala.collection.mutable

//FIXME: trivial renderer that will push every item onto a buffer
case class MatrixTuples(op: Operator) extends Operator(Vector(op)) {

  def eval(
      eg: EvaluatorGraph
  )(cb: Record => IO[Unit])(implicit G: GRB): IO[Unit] = {
    op.eval(eg) {
      case rec: MatrixRecord =>
        for {
          data <- rec.mat.extract
          (is, js, _) = data
          buf: mutable.ArrayBuffer[(Long, Long)] = mutable.ArrayBuffer.from(
            is.zip(js)
          )
          _ <- cb(OutputRecord(buf))
        } yield ()
    }
  }

  def sorted: Option[Name] = None

  def output: Seq[Name] = op.output

}

object Expand {

  import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb

  /**
    * Acquire a single ANY_PAIR semiring and use it
    * it should have no state thus no reason
    * to cause undefined behaviour when
    * shared between threads
    * */
  lazy val staticAnyPairSemiring: GrBSemiring[Boolean, Boolean, Boolean] = {
    val (semi, shutdown) =
      GrBSemiring.anyPair[IO].allocated.unsafeRunSync()(IORuntime.global)
    Runtime
      .getRuntime()
      .addShutdownHook(new Thread() {
        override def run: Unit = {
          shutdown.unsafeRunSync()(IORuntime.global)
        }
      })
    semi
  }

}
