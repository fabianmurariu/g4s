package com.github.fabianmurariu.g4s.optim.impls

import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grbv2.MxM
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.matrix.BlockingMatrix
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import com.github.fabianmurariu.g4s.optim.Name
import com.github.fabianmurariu.g4s.optim.LogicMemoRef
import cats.effect.IO
import com.github.fabianmurariu.g4s.optim.Binding
import com.github.fabianmurariu.g4s.optim.UnNamed
import cats.effect.kernel.MonadCancel
import cats.effect.unsafe.IORuntime

/**
  * base class for push operators
  * inspired by "How to Architect a Query Compiler, Revised" Ruby T Tahboub, et al.
  */
trait Operator { self =>

  def eval(cb: Record => IO[Unit]): IO[Unit]

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
    val text =
      List("size" -> sizeText, "sorted" -> sortedText, "output" -> coverText)
    text
      .filter { case (_, text) => text.nonEmpty }
      .map { case (key, text) => s"$key=$text" }
  }

  def show(offset: String = ""): String = self match {
    case _: GetNodeMatrix =>
      val textBlocks = showInner.mkString(", ")
      s"Nodes[$textBlocks]"
    case _: GetEdgeMatrix =>
      val textBlocks = showInner.mkString(", ")
      s"Edges[$textBlocks]"
    case ExpandMul(left, right) =>
      val padding = offset + "  "
      val textBlocks = showInner.mkString(", ")
      s"""ExpandMul[$textBlocks]:\n${padding}l=${left
        .show(padding)}\n${padding}r=${right.show(padding)}"""
    case FilterMul(left, right) =>
      val padding = offset + "  "
      val textBlocks = showInner.mkString(", ")
      s"""FilterMul[$textBlocks]:\n${padding}l=${left
        .show(padding)}\n${padding}r=${right.show(padding)}"""
    case RefOperator(_) =>
      val textBlocks = showInner.mkString(", ")
      s"Ref[${textBlocks}]"
  }
}

object Operator {

  val semiRing: GrBSemiring[Boolean, Boolean, Boolean] =
    Expand.staticAnyPairSemiring

  def commonMxM(left: Operator, right: Operator)(
      cb: Record => IO[Unit]
  )(implicit G:GRB): IO[Unit] = {
    left.eval {
      case left: MatrixRecord @ unchecked =>
        right.eval {
          case right: MatrixRecord @ unchecked  =>
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
        right.eval {
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
case class RefOperator(logic: LogicMemoRef)
    extends Operator {

  override def eval(cb: Record => IO[Unit]): IO[Unit] = IO.unit

  override def sorted: Option[Name] = logic.sorted

  override def output: Seq[Name] = logic.output

  override def cardinality: Long = -1

}

sealed trait EdgeMatrix extends Operator

case class GetNodeMatrix(
    binding: Name,
    name: Option[String],
    nodes: BlockingMatrix[Boolean],
    cardinality: Long // very likely nodes.nvals
) extends Operator {

  override def eval(cb: Record => IO[Unit]): IO[Unit] =
    cb(Nodes(nodes))

  override def sorted: Option[Name] = Some(binding)

  override def output: Seq[Name] = Seq(binding)

}

case class GetEdgeMatrix(
    binding: Option[Name],
    name: Option[String],
    edges: BlockingMatrix[Boolean],
    cardinality: Long // very likely edges.nvals
) extends EdgeMatrix {

  override def eval(cb: Record => IO[Unit]): IO[Unit] =
    cb(Edges(edges))

  override def sorted: Option[Name] = None

  override def output: Seq[Name] = binding.toSeq

}

case class ExpandMul(
    frontier: Operator,
    edges: Operator
)(implicit G: GRB)
    extends Operator {

  val semiRing: GrBSemiring[Boolean, Boolean, Boolean] =
    Expand.staticAnyPairSemiring

  override def eval(cb: Record => IO[Unit]): IO[Unit] = {
    Operator.commonMxM(frontier, edges)(cb)
  }

  override def sorted: Option[Name] = frontier.sorted

  override def output: Seq[Name] = frontier.output ++ edges.output.lastOption

  override def cardinality: Long = frontier.cardinality * edges.cardinality

}

case class FilterMul(
    frontier: Operator,
    filter: Operator
)(implicit G: GRB)
    extends Operator {

  val semiRing: GrBSemiring[Boolean, Boolean, Boolean] =
    Expand.staticAnyPairSemiring

  override def eval(cb: Record => IO[Unit]): IO[Unit] = {
    Operator.commonMxM(frontier, filter)(cb)
  }

  override def sorted: Option[Name] = frontier.sorted

  override def output: Seq[Name] = frontier.output ++ filter.output.lastOption
  // the filter should never output more than the input
  override def cardinality: Long = frontier.cardinality

}

//FIXME: trivial renderer that will push every item onto a buffer
case class Render(op: Operator) extends Operator {

  import scala.collection.mutable

  override def eval(cb: Record => IO[Unit]): IO[Unit] = {
    op.eval {
      case rec: MatrixRecord =>
        for {
          data <- rec.mat.extract
          (is, js, _) = data
          buf = mutable.ArrayBuffer.from(is.zip(js))
          _ <- cb(OutputRecord(buf))
        } yield ()
    }
  }

  override def sorted: Option[Name] = ???

  override def output: Seq[Name] = ???

  override def cardinality: Long = ???

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
    val (semi, shutdown) = GrBSemiring.anyPair[IO].allocated.unsafeRunSync()(IORuntime.global)
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
