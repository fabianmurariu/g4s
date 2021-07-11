package com.github.fabianmurariu.g4s.optim

sealed trait Name
class UnNamed extends Name
case class Binding(name: String) extends Name
