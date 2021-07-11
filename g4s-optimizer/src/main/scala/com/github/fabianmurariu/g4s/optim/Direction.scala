package com.github.fabianmurariu.g4s.optim

sealed trait Direction
case object Out extends Direction
case object In extends Direction
case object Both extends Direction
