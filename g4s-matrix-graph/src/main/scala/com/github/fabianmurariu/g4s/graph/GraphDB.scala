package com.github.fabianmurariu.g4s.graph

import com.github.fabianmurariu.g4s.optim.QueryGraph
import cats.effect.IO
import com.github.fabianmurariu.g4s.optim.EvaluatorGraph
import com.github.fabianmurariu.g4s.optim.{impls => op}
import com.github.fabianmurariu.g4s.optim.Optimizer
import com.github.fabianmurariu.g4s.sparse.grb.GRB

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

  def optim(graph:EvaluatorGraph)(qg: QueryGraph)(implicit G:GRB): IO[op.Operator] =
   for {
     memo <- Optimizer.default.optimize(qg, graph)
     plan <- memo.physical(qg.returns.head) // FIXME: need to test with all of the root plans
   } yield plan

  // def eval(graph:EvaluatorGraph)(plan: op.Operator):fs2.Stream[Row] =
  //   graph.eval(plan)

}
