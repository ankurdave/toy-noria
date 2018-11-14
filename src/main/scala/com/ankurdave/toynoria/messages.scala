package com.ankurdave.toynoria

/** Noria dataflow message. */
sealed trait Msg[A <: Record]

/** Insert the specified record. */
case class Insert[A <: Record](x: A) extends Msg[A]

/** Update any records with the same id as the specified record to the new value. */
case class Update[A <: Record](x: A) extends Msg[A]

/** Delete any records with the same id as the specified record. */
case class Delete[A <: Record](x: A) extends Msg[A]

/** Evict any records with the same id as the specified record. */
case class Evict[A <: Record](x: A) extends Msg[A]
