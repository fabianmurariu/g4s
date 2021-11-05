package com.github.fabianmurariu.g4s.graph

import com.github.fabianmurariu.g4s.optim.QueryGraph
import cats.effect.IO
import com.github.fabianmurariu.g4s.optim.EvaluatorGraph
import com.github.fabianmurariu.g4s.optim.{impls => op}
import com.github.fabianmurariu.g4s.optim.MemoV2
import com.github.fabianmurariu.g4s.optim.Optimizer

/**
 * Pipeline for
 * parsing
 * optimising
 * evaluating
 *
 * graph queries
 * */
object GraphDB{

  def parse(cypher:String):IO[QueryGraph] =
    IO.defer(IO.fromEither(QueryGraph.fromCypherText(cypher)))

  def optim(graph:EvaluatorGraph)(qg: QueryGraph): IO[(op.Operator, MemoV2)] =
    for {
    stats <- graph.withStats{ stats => IO(stats.copy) }
    memo <- IO.delay( Optimizer.optimizeV2(qg, stats))
    } yield MemoV2.bestPlan(memo) -> memo


  // def eval(graph:EvaluatorGraph)(plan: op.Operator):fs2.Stream[Row] =
  //   graph.eval(plan)

}
