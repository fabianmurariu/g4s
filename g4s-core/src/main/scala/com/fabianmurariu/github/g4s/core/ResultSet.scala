package com.fabianmurariu.github.g4s.core

trait ResultSet[IO[_], A] {

    def next:IO[Iterable[A]]

}
