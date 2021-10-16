package com.github.fabianmurariu.g4s.optim.impls

import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grbv2.MxM
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import com.github.fabianmurariu.g4s.optim.Name
import com.github.fabianmurariu.g4s.optim.LogicMemoRef
import cats.effect.IO
import com.github.fabianmurariu.g4s.optim.Binding
import com.github.fabianmurariu.g4s.optim.UnNamed
import cats.effect.kernel.MonadCancel
import cats.effect.unsafe.IORuntime
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.optim.EvaluatorGraph
import com.github.fabianmurariu.g4s.optim.LogicMemoRefV2

/**
  * base class for push operators
  * inspired by "How to Architect a Query Compiler, Revised" Ruby T Tahboub, et al.
  */
trait Operator { self =>

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

  /**
    * the estimated cardinality of the output
    * how much stuff do we expect to produce
    */
  def cardinality: Long

  /**
    *  the estimated cost of executing the operator
    * */
  def cost: Double = 1.2 * cardinality

  private def showInner: List[String] = {
    val sortedText =
      self.sorted.collect { case Binding(name) => name }.getOrElse("")
    val coverText =
      self.output
        .map {
          case Binding(name) => name
          case _: UnNamed    => "*"
        }
        .mkString("[", ",", "]")
    val sizeText = s"${self.cardinality}"
    val costText = s"${self.cost}"
    val text =
      List("size" -> sizeText, "sorted" -> sortedText, "output" -> coverText, "cost" -> costText)
    text
      .filter { case (_, text) => text.nonEmpty }
      .map { case (key, text) => s"$key=$text" }
  }

  def show(offset: String = ""): String = self match {
    case _: GetNodeMatrix =>
      val textBlocks = showInner.mkString(", ")
      s"Nodes[$textBlocks]"
    case em: GetEdgeMatrix =>
      val textBlocks = showInner.mkString(", ")
      val name = em.name.mkString("[", ",", "]")
      s"Edges[$textBlocks, label=$name, transpose=${em.transpose}]"
    case ExpandMul(left, right, sel) =>
      val padding = offset + "  "
      val textBlocks = showInner.mkString(", ")
      s"""ExpandMul[sel=$sel, $textBlocks]:\n${padding}l=${left
        .show(padding)}\n${padding}r=${right.show(padding)}"""
    case FilterMul(left, right, sel) =>
      val padding = offset + "  "
      val textBlocks = showInner.mkString(", ")
      s"""FilterMul[sel=$sel, $textBlocks]:\n${padding}l=${left
        .show(padding)}\n${padding}r=${right.show(padding)}"""
    case Diag(op) =>
      val padding = offset + "  "
      val textBlocks = showInner.mkString(", ")
      s"""Diag[$textBlocks]:\n${padding}on=${op.show(padding)}"""
    case RefOperator(_) =>
      val textBlocks = showInner.mkString(", ")
      s"Ref[${textBlocks}]"
  }
}

sealed trait MatrixOperator extends Operator

object Operator {

  val semiRing: GrBSemiring[Boolean, Boolean, Boolean] =
    Expand.staticAnyPairSemiring

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
case class RefOperator(logic: Either[LogicMemoRefV2, LogicMemoRef]) extends Operator {

  def eval(eg: EvaluatorGraph)(cb: Record => IO[Unit])(
      implicit G: GRB
  ): IO[Unit] = IO.unit

  def sorted: Option[Name] = None

  def output: Seq[Name] = logic match {
      case Left(value) => value.output
      case Right(value) => value.output
  }

  def cardinality: Long = 1L

  def signature:String = logic match {
      case Left(value) => value.signature
      case Right(value) => value.signature
  }
}

object RefOperator{
    def apply(logic:LogicMemoRef): RefOperator = RefOperator(Right(logic))
    def apply(logic:LogicMemoRefV2):RefOperator = RefOperator(Left(logic))
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

  override def cost: Double = 0d
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

  override def cost: Double = 0d
}

case class ExpandMul(
    frontier: Operator,
    edges: Operator,
    edgesSel:Double = 1.0d
) extends Operator {

  val semiRing: GrBSemiring[Boolean, Boolean, Boolean] =
    Expand.staticAnyPairSemiring //FIXME get rid of this

  def eval(
      eg: EvaluatorGraph
  )(cb: Record => IO[Unit])(implicit G: GRB): IO[Unit] =
    Operator.commonMxM(eg)(frontier, edges)(cb)


  def sorted: Option[Name] = frontier.sorted

  def output: Seq[Name] = frontier.output ++ edges.output.lastOption

  def cardinality: Long = (Math.max((frontier.cardinality * edges.cardinality) * edgesSel, 1.0d)).toLong

}

case class FilterMul(
    frontier: Operator,
    filter: Operator,
    sel: Double
) extends Operator {

  val semiRing: GrBSemiring[Boolean, Boolean, Boolean] =
    Expand.staticAnyPairSemiring

  def eval(
      eg: EvaluatorGraph
  )(cb: Record => IO[Unit])(implicit G: GRB): IO[Unit] = {
    Operator.commonMxM(eg)(frontier, filter)(cb)
  }

  def sorted: Option[Name] = frontier.sorted

  def output: Seq[Name] = frontier.output ++ filter.output.lastOption
  // the filter should never output more than the input
  def cardinality: Long = (Math.max((frontier.cardinality * filter.cardinality) * sel, 1.0d)).toLong

}

case class Diag(op: Operator) extends Operator {

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

  // max cardinality is the number of items on the diagonal
  // we estimate as 1/10
  def cardinality: Long = op.cardinality / 10


}

//FIXME: trivial renderer that will push every item onto a buffer
case class MatrixTuples(op: Operator) extends Operator {

  import scala.collection.mutable

  def eval(
      eg: EvaluatorGraph
  )(cb: Record => IO[Unit])(implicit G: GRB): IO[Unit] = {
    op.eval(eg) {
      case rec: MatrixRecord =>
        for {
          data <- rec.mat.extract
          (is, js, _) = data
          buf = mutable.ArrayBuffer.from(is.zip(js))
          _ <- cb(OutputRecord(buf))
        } yield ()
    }
  }

  def sorted: Option[Name] = ???

  def output: Seq[Name] = ???

  def cardinality: Long = ???

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
