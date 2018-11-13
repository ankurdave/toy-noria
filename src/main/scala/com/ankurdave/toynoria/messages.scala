package com.ankurdave.toynoria

/**
 * Noria dataflow message.
 */
sealed trait Msg[A]

case class Insert[A](x: A) extends Msg[A]

case class Update[A](x: A) extends Msg[A]

case class Delete[A](x: A) extends Msg[A]

case class Evict[A](x: A) extends Msg[A]
